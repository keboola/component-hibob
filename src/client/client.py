import base64
import logging

import requests.exceptions
from keboola.http_client import HttpClient


class HiBobException(Exception):
    pass


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

    def get_employees(self, human_readable: bool = False):
        logging.info("Retrieving employees.")

        params = {
            "showInactive": True
        }

        if human_readable:
            params["humanReadable"] = "REPLACE"
            logging.info("Component will fetch only human readable values for employees table.")

        try:
            r = self.post("people/search", json=params)
        except requests.exceptions.HTTPError as e:
            raise HiBobException(f"Cannot fetch employees, reason: {e}")

        employees = r.get("employees")

        if employees:
            for employee in employees:
                yield employee

    def get_employment_history(self, employee_id: str) -> dict:
        endpoint = f"people/{employee_id}/employment"
        r = self._get(endpoint).get("values")
        r["employee_id"] = employee_id
        return r

    def get_employee_lifecycle(self, employee_id: str) -> dict:
        endpoint = f"people/{employee_id}/lifecycle"
        r = self._get(endpoint).get("values")
        print(r)
        exit()
        r["employee_id"] = employee_id
        return r

    def get_employee_work_history(self, employee_id: str) -> dict:
        endpoint = f"people/{employee_id}/work"
        r = self._get(endpoint).get("values")
        r["employee_id"] = employee_id
        return r

    def _get(self, endpoint) -> dict:
        try:
            r = self.get(endpoint)
            return r
        except requests.exceptions.RetryError as e:
            raise HiBobException(f"Failed to fetch endpoint {endpoint}, {e}") from e
