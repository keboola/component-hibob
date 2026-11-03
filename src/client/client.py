import base64
import logging

import requests.exceptions
from keboola.http_client import HttpClient
from ratelimit import limits, sleep_and_retry

# Maximum number of employee ids accepted by a single HiBob bulk request.
# https://apidocs.hibob.com/reference/get_bulk-people-employment
MAX_EMPLOYEE_IDS_PER_REQUEST = 200

# Maximum page size accepted by the HiBob bulk endpoints.
MAX_PAGE_SIZE = 200

# HiBob rate limits the bulk history endpoints to 100 calls per minute, twice the budget
# of the per-employee history endpoints.
BULK_CALLS_PER_MINUTE = 100

# HiBob rate limits the per-employee history endpoints to 50 calls per minute. The client used
# to send 100 and rely on the 429 retry. This path is now only the fallback.
PER_EMPLOYEE_CALLS_PER_MINUTE = 50

# Maps the component endpoint names onto the HiBob bulk paths and the per-employee paths.
# The per-employee path is the fallback used when the bulk endpoint is not available.
HISTORY_ENDPOINTS = {
    "employment_history": {"bulk": "bulk/people/employment", "single": "employment"},
    "employee_lifecycle": {"bulk": "bulk/people/lifecycle", "single": "lifecycle"},
    "employee_work_history": {"bulk": "bulk/people/work", "single": "work"},
}


class HiBobException(Exception):
    pass


class HiBobBulkUnavailableException(HiBobException):
    """
    Raised when a HiBob bulk history endpoint cannot be used with these credentials.

    The documented permissions for the bulk endpoints are the same as for the per-employee
    endpoints, so this is not expected. It is raised so the component can fall back to the
    per-employee endpoints instead of failing the whole run.
    """


class HiBobClient(HttpClient):
    base_url = "https://api.hibob.com/v1/"

    def __init__(self, service_user_id, service_user_token):
        base64_credentials = self.make_base64_credentials(service_user_id, service_user_token)

        default_header = {
            "Authorization": f"Basic {base64_credentials}",
            "accept": "application/json"
        }
        super().__init__(base_url=self.base_url, default_http_header=default_header,
                         status_forcelist=(429, 500, 502, 503, 504), max_retries=10)
        # Set once a bulk endpoint reports that it is unavailable, so the remaining endpoints
        # go straight to the per-employee fallback instead of repeating a failing call.
        self.bulk_unavailable = False

    @staticmethod
    def make_base64_credentials(service_user_id, service_user_token) -> str:
        credentials = f"{service_user_id}:{service_user_token}"
        return base64.b64encode(credentials.encode()).decode()

    def test_connection(self):
        """Returns True if people endpoint is reachable with entered credentials."""
        payload = {"fields": ["About"]}

        try:
            r = self.post_raw("people/search", json=payload)
            r.raise_for_status()
            return True
        except requests.HTTPError:
            return False

    def get_employees(self, human_readable: bool = False, custom_fields: list = None):
        logging.info("Retrieving employees.")

        params = {
            "showInactive": True
        }

        if custom_fields:
            params["fields"] = custom_fields

        if human_readable:
            params["humanReadable"] = "REPLACE"
            logging.info("Component will fetch only human readable values for employees table.")

        try:
            logging.info(params)
            r = self.post("people/search", json=params)
        except requests.exceptions.HTTPError as e:
            raise HiBobException(f"Cannot fetch employees, reason: {e}")

        employees = r.get("employees")

        if employees:
            for employee in employees:
                yield employee

    def get_history_in_bulk(self, endpoint_name: str, employee_ids: list):
        """
        Yield (employee_id, record) pairs for a history endpoint using the HiBob bulk API.

        The employee ids are sent explicitly in batches of at most 200, so inactive employees
        stay included exactly as they are with the per-employee endpoints. Each batch is read
        page by page using the cursor returned in response_metadata.

        Args:
            endpoint_name: One of the keys of HISTORY_ENDPOINTS.
            employee_ids: Employee ids to fetch history for.

        Raises:
            HiBobBulkUnavailableException: The bulk endpoint rejected the credentials.
            HiBobException: Any other failure while reading the endpoint.
        """
        if self.bulk_unavailable:
            raise HiBobBulkUnavailableException(
                "The HiBob bulk endpoints are not available with these credentials."
            )

        bulk_path = HISTORY_ENDPOINTS[endpoint_name]["bulk"]
        restricted_columns_reported = False
        results_seen = 0
        refused = 0

        for batch_number, batch in enumerate(self._batched(employee_ids, MAX_EMPLOYEE_IDS_PER_REQUEST), start=1):
            logging.info(f"Retrieving {endpoint_name} for employee batch {batch_number} ({len(batch)} employees).")
            cursor = None
            seen_cursors = set()
            requested = {str(employee_id) for employee_id in batch}

            while True:
                params = {
                    "employeeIds": ",".join(str(employee_id) for employee_id in batch),
                    "limit": MAX_PAGE_SIZE
                }
                if cursor:
                    params["cursor"] = cursor

                response = self._get_bulk(bulk_path, params)

                refused += self._log_bulk_errors(endpoint_name, response.get("errors"))

                if cursor and cursor in seen_cursors:
                    raise HiBobException(
                        f"HiBob returned the same page cursor twice for {endpoint_name}. Reading "
                        f"the endpoint cannot continue without duplicating or losing records."
                    )
                if cursor:
                    seen_cursors.add(cursor)

                for result in response.get("results") or []:
                    results_seen += 1
                    employee_id = result.get("employeeId")
                    if not employee_id:
                        logging.warning(f"Skipping a {endpoint_name} result without an employeeId.")
                        continue
                    if employee_id not in requested:
                        raise HiBobException(
                            f"HiBob returned {endpoint_name} for employee id {employee_id}, which "
                            f"was not requested. The employee_id column would stop matching the "
                            f"employees table."
                        )
                    values = result.get("values")
                    if not restricted_columns_reported and self._has_restricted_columns(values):
                        restricted_columns_reported = True
                        logging.warning(
                            f"HiBob removed some columns from the {endpoint_name} response because "
                            f"the service user has only partial column permissions. The table will "
                            f"be missing those columns."
                        )
                    for record in self._iterate_values(values):
                        yield employee_id, record

                cursor = (response.get("response_metadata") or {}).get("next_cursor")
                if not cursor:
                    break

        # Some employees missing is a partial table, which the warnings above make visible.
        # No result objects at all is a different case: it empties the whole table, and a full
        # load then deletes every existing row. That is not partial data, so the run stops.
        if employee_ids and not results_seen:
            reason = (
                f"HiBob reported {refused} error(s) and returned no employee at all." if refused
                else "HiBob returned no result objects, so the response was not in the expected form."
            )
            raise HiBobException(
                f"The HiBob bulk endpoint returned nothing at all for {endpoint_name}, for "
                f"{len(employee_ids)} requested employees. {reason} The run stopped, because "
                f"writing an empty table would delete every existing row from Storage."
            )

        if refused:
            logging.warning(
                f"{refused} employee(s) are missing from {endpoint_name} because HiBob did not "
                f"return them. The table is incomplete."
            )

    def get_history_per_employee(self, endpoint_name: str, employee_id: str):
        """
        Yield (employee_id, record) pairs for a single employee.

        This is the original per-employee path, kept as the fallback for credentials that
        cannot use the bulk endpoints.
        """
        single_path = HISTORY_ENDPOINTS[endpoint_name]["single"]
        response = self._get(f"people/{employee_id}/{single_path}")

        for record in self._iterate_values(response.get("values")):
            yield employee_id, record

    @staticmethod
    def _batched(items: list, batch_size: int):
        for start in range(0, len(items), batch_size):
            yield items[start:start + batch_size]

    @staticmethod
    def _iterate_values(values):
        """
        Yield the history entries held in a `values` list.

        The two HiBob paths nest this field differently, and both shapes are handled here.

        - Bulk: `results[].values[]` is a list of
          `{"values": [entry, ...], "restricted_columns": ...}` wrappers. The specification
          requires this nesting, so the unwrapping below is the documented happy path for the
          bulk endpoints, not defensive code. Removing it would yield wrapper objects instead
          of history entries and every history table would come out empty.
        - Per-employee: `values` is already a flat list of entries.

        A wrapper is recognised by a list in `values` together with no `id`. Every history entry
        carries `id`, because it is half of the [id, employee_id] primary key, and a wrapper never
        does. Matching on the absence of `id` rather than on an exact key set keeps this working
        if HiBob adds a field to the wrapper, and still never mistakes an entry for a wrapper.
        """
        for value in values or []:
            if isinstance(value, dict) and isinstance(value.get("values"), list) and "id" not in value:
                for record in value["values"] or []:
                    yield record
            else:
                yield value

    @staticmethod
    def _has_restricted_columns(values) -> bool:
        """Return True if HiBob reported that field-level permissions filtered the columns."""
        for value in values or []:
            if isinstance(value, dict) and value.get("restricted_columns"):
                return True
        return False

    @staticmethod
    def _log_bulk_errors(endpoint_name: str, errors) -> int:
        """
        Report the employees HiBob refused and return how many it refused.

        HiBob lists these employees in `errors` and still answers 200. In HiBob the employee
        scope and the table permissions are set separately, so a service user can legitimately
        read an employee in people/search and still not be allowed to read that employee's
        history. The run therefore continues with the employees it did get, and the refused ones
        are reported in the job log.

        The documented shape of this field is ambiguous, so it is reported as received.
        """
        if not errors:
            return 0

        logging.warning(
            f"HiBob did not return {len(errors)} employee(s) for {endpoint_name}. Those employees "
            f"are missing from the table. Give the service user permission for them if they "
            f"should be included. HiBob reported: {errors}"
        )
        return len(errors)

    @sleep_and_retry
    @limits(calls=BULK_CALLS_PER_MINUTE, period=60)
    def _get_bulk(self, endpoint, params) -> dict:
        try:
            return self.get(endpoint, params=params)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            if status_code in (403, 404):
                self.bulk_unavailable = True
                raise HiBobBulkUnavailableException(
                    f"The bulk endpoint {endpoint} is not available with these credentials "
                    f"(HTTP {status_code})."
                ) from e
            if status_code == 401:
                # A rejected token will be rejected by the per-employee endpoints too.
                raise HiBobException(
                    "HiBob rejected the service user credentials (HTTP 401). Check the Service "
                    "User ID and the Service User Token."
                ) from e
            raise HiBobException(f"Failed to fetch endpoint {endpoint}, {e}") from e
        except requests.exceptions.RetryError as e:
            raise HiBobException(f"Failed to fetch endpoint {endpoint}, {e}") from e

    @sleep_and_retry
    @limits(calls=PER_EMPLOYEE_CALLS_PER_MINUTE, period=60)
    def _get(self, endpoint) -> dict:
        try:
            return self.get(endpoint)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            raise HiBobException(f"Failed to fetch endpoint {endpoint} (HTTP {status_code}), {e}") from e
        except requests.exceptions.RetryError as e:
            raise HiBobException(f"Failed to fetch endpoint {endpoint}, {e}") from e
