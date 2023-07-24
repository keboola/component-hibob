"""
Template Component main class.

"""
from keboola.csvwriter import ElasticDictWriter
import logging

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException

from configuration import Configuration
from client.client import HiBobClient


SUPPORTED_ENDPOINTS = ['employment_history', 'employee_lifecycle', 'employee_work_history']


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
        self.incremental = None
        self.client = None
        self._configuration: Configuration

    def run(self):
        """
        Main execution code
        """

        self._init_configuration()

        service_user_id = self._configuration.authorization.service_user_id
        service_user_token = self._configuration.authorization.pswd_service_user_token

        load_type = self._configuration.destination.load_type
        if load_type == "incremental_load":
            self.incremental = True
        else:
            self.incremental = False

        self.client = HiBobClient(service_user_id, service_user_token)

        employee_ids = self.get_employees()

        for endpoint in self._configuration.endpoints:
            if endpoint in SUPPORTED_ENDPOINTS:
                getattr(self, "get_"+endpoint)(employee_ids)
            else:
                raise UserException(f"Unsupported endpoint: {endpoint}.")

    def get_employees(self) -> list:
        """Saves employee data from https://apidocs.hibob.com/reference/get_people into csv and returns
        a list of employee_ids."""
        table = self.create_out_table_definition('employees.csv', incremental=self.incremental, primary_key=['id'])
        employee_ids = []
        with ElasticDictWriter(table.full_path, fieldnames=[]) as wr:
            wr.writeheader()
            for employee in self.client.get_employees():
                wr.writerow(self.flatten_dictionary(employee))

                if employee.get("id"):
                    employee_ids.append(employee.get("id"))

        self.write_manifest(table)
        return employee_ids

    def get_employment_history(self, employee_ids):
        logging.info("Retrieving employment history.")

        table = self.create_out_table_definition('employment_history.csv', incremental=self.incremental,
                                                 primary_key=['id'])
        with ElasticDictWriter(table.full_path, fieldnames=[]) as wr:
            wr.writeheader()
            for employee_id in employee_ids:
                result = self.client.get_employment_history(employee_id)
                for record in result:
                    wr.writerow(self.flatten_dictionary(record))
        self.write_manifest(table)

    def get_employee_lifecycle(self, employee_ids):
        logging.info("Retrieving employee lifecycle.")

        table = self.create_out_table_definition('employee_lifecycle.csv', incremental=self.incremental,
                                                 primary_key=['id'])
        with ElasticDictWriter(table.full_path, fieldnames=[]) as wr:
            wr.writeheader()
            for employee_id in employee_ids:
                result = self.client.get_employee_lifecycle(employee_id)
                for record in result:
                    wr.writerow(self.flatten_dictionary(record))
        self.write_manifest(table)

    def get_employee_work_history(self, employee_ids):
        logging.info("Retrieving employee work history.")

        table = self.create_out_table_definition('employee_work_history.csv', incremental=self.incremental,
                                                 primary_key=['id'])
        with ElasticDictWriter(table.full_path, fieldnames=[]) as wr:
            wr.writeheader()
            for employee_id in employee_ids:
                result = self.client.get_employee_work_history(employee_id)
                for record in result:
                    wr.writerow(self.flatten_dictionary(record))
        self.write_manifest(table)

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

    @sync_action("testConnection")
    def test_connection(self):
        self._init_configuration()

        service_user_id = self._configuration.authorization.service_user_id
        service_user_token = self._configuration.authorization.pswd_service_user_token

        client = HiBobClient(service_user_id, service_user_token)

        if client.test_connection():
            return None
        else:
            raise UserException("Test connection failed.")


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
