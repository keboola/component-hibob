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
        bulk_path = HISTORY_ENDPOINTS[endpoint_name]["bulk"]

        for batch_number, batch in enumerate(self._batched(employee_ids, MAX_EMPLOYEE_IDS_PER_REQUEST), start=1):
            logging.info(f"Retrieving {endpoint_name} for employee batch {batch_number} ({len(batch)} employees).")
            cursor = None
            seen_cursors = set()

            while True:
                params = {
                    "employeeIds": ",".join(str(employee_id) for employee_id in batch),
                    "limit": MAX_PAGE_SIZE
                }
                if cursor:
                    params["cursor"] = cursor

                response = self._get_bulk(bulk_path, params)

                self._log_bulk_errors(endpoint_name, response.get("errors"))

                for result in response.get("results") or []:
                    employee_id = result.get("employeeId")
                    if not employee_id:
                        logging.warning(f"Skipping a {endpoint_name} result without an employeeId.")
                        continue
                    for record in self._iterate_values(result.get("values")):
                        yield employee_id, record

                cursor = (response.get("response_metadata") or {}).get("next_cursor")
                if not cursor:
                    break

                # A cursor that repeats would page forever and keep the job running until it
                # times out, so the batch is stopped instead.
                if cursor in seen_cursors:
                    logging.warning(
                        f"HiBob returned a repeating page cursor for {endpoint_name}. "
                        f"Stopping batch {batch_number} to avoid an endless loop."
                    )
                    break
                seen_cursors.add(cursor)

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

        The per-employee endpoints return `values` as a flat list of entries. The bulk
        specification describes the same field ambiguously, so a nested
        `{"values": [...], "restricted_columns": ...}` wrapper is unwrapped here as well.
        A wrapper is only recognised when the object holds nothing but those two keys, so a
        real history entry is never mistaken for one.
        """
        wrapper_keys = {"values", "restricted_columns"}

        for value in values or []:
            if isinstance(value, dict) and "values" in value and set(value).issubset(wrapper_keys):
                for record in value.get("values") or []:
                    yield record
            else:
                yield value

    @staticmethod
    def _log_bulk_errors(endpoint_name: str, errors) -> None:
        """
        Log the per-employee errors reported by a bulk response.

        HiBob reports employees it cannot return under `errors` instead of failing the request,
        for example when the service user has no permission for them. The documented shape of
        this field is ambiguous, so it is logged as received rather than parsed.
        """
        if errors:
            logging.warning(f"HiBob reported errors for {endpoint_name}: {errors}")

    @sleep_and_retry
    @limits(calls=BULK_CALLS_PER_MINUTE, period=60)
    def _get_bulk(self, endpoint, params) -> dict:
        try:
            return self.get(endpoint, params=params)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            if status_code in (401, 403, 404):
                raise HiBobBulkUnavailableException(
                    f"The bulk endpoint {endpoint} is not available with these credentials "
                    f"(HTTP {status_code})."
                ) from e
            raise HiBobException(f"Failed to fetch endpoint {endpoint}, {e}") from e
        except requests.exceptions.RetryError as e:
            raise HiBobException(f"Failed to fetch endpoint {endpoint}, {e}") from e

    @sleep_and_retry
    @limits(calls=100, period=60)
    def _get(self, endpoint) -> dict:
        try:
            r = self.get(endpoint)
            return r
        except requests.exceptions.RetryError as e:
            raise HiBobException(f"Failed to fetch endpoint {endpoint}, {e}") from e
