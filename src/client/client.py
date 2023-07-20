from keboola.http_client import HttpClient

import base64


class HiBobException(Exception):
    pass


class HiBobClient(HttpClient):
    base_url = "https://api.hibob.com/v1/"

    def __init__(self, service_user_id, service_user_token):
        self.service_user_id = service_user_id
        self.service_user_token = service_user_token

        credentials = f"{service_user_id}:{service_user_token}"
        self.base64_credentials = base64.b64encode(credentials.encode()).decode()

        default_header = {
            "Authorization": f"Basic {self.base64_credentials}",
            "accept": "application/json"
        }
        super().__init__(base_url=self.base_url, default_http_header=default_header,
                         status_forcelist=(429, 500, 502, 504))

    def get_all_employees(self):
        """This will be deprecated in Q4 2023"""
        r = self.get("people")

        employees = r.get("employees")

        if employees:
            for employee in employees:
                yield employee

    def get_employment_history(self, employee_id: str):
        r = self.get(f"people/{employee_id}/employment")

        employment_history = r.get("values")

        return employment_history


