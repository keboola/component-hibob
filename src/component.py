"""
Template Component main class.

"""
from keboola.csvwriter import ElasticDictWriter
import logging


from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration, Account
from client.client import HiBobClient

# configuration variables
KEY_API_TOKEN = '#api_token'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_TOKEN]


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self._configuration: Configuration

    def run(self):
        """
        Main execution code
        """

        self._init_configuration()

        service_user_id = self._configuration.account.service_user_id
        service_user_token = self._configuration.account.pswd_service_user_token

        client = HiBobClient(service_user_id, service_user_token)

        employee_ids = self.get_employees(client)

        table = self.create_out_table_definition('employment_history.csv', incremental=True, primary_key=[])
        with ElasticDictWriter(table.full_path, fieldnames=[]) as wr:
            wr.writeheader()
            for employee_id in employee_ids:
                employment_history = client.get_employment_history(employee_id)
                wr.writerow(self.flatten_dictionary(employment_history))

    def get_employees(self, client) -> list:

        table = self.create_out_table_definition('employees.csv', incremental=True, primary_key=['id'])
        employee_ids = []
        with ElasticDictWriter(table.full_path, fieldnames=[]) as wr:
            wr.writeheader()
            for employee in client.get_all_employees():
                wr.writerow(self.flatten_dictionary(employee))

                if employee.get("id"):
                    employee_ids.append(employee.get("id"))

        self.write_manifest(table)
        return employee_ids

    def get_employment_history(self, employee_id):
        pass

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    @staticmethod
    def flatten_dictionary(nested_dict, sep='_'):
        def _flatten(d, parent_key='', result=None):
            if result is None:
                result = {}

            for key, value in d.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                if isinstance(value, dict):
                    _flatten(value, new_key, result)
                else:
                    result[new_key] = value
            return result

        return _flatten(nested_dict)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
