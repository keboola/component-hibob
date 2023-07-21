import base64
import logging

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
                         status_forcelist=(429, 500, 502, 504))

    @staticmethod
    def make_base64_credentials(service_user_id, service_user_token) -> str:
        credentials = f"{service_user_id}:{service_user_token}"
        return base64.b64encode(credentials.encode()).decode()

    def get_employees(self):
        """This will be deprecated in Q4 2023"""
        logging.info("Retrieving employees.")
        r = self.get("people")

        employees = r.get("employees")

        if employees:
            for employee in employees:
                yield employee

    def get_employment_history(self, employee_id: str) -> dict:
        r = self.get(f"people/{employee_id}/employment")

        return r.get("values")

    def get_employee_lifecycle(self, employee_id: str) -> dict:
        r = self.get(f"people/{employee_id}/lifecycle")
        return r.get("values")

    def get_employee_work_history(self, employee_id: str) -> dict:
        r = self.get(f"people/{employee_id}/work")
        return r.get("values")
