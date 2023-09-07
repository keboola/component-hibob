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
        self.state = {}

    def run(self):
        """
        Main execution code
        """

        self._init_configuration()
        self.state = self.get_state_file()

        service_user_id = self._configuration.authorization.service_user_id
        service_user_token = self._configuration.authorization.pswd_service_user_token

        self.incremental = self._configuration.destination.load_type == "incremental_load"

        human_readable = self._configuration.human_readable

        self.client = HiBobClient(service_user_id, service_user_token)

        employee_ids = self.get_employees(human_readable)

        for endpoint in self._configuration.endpoints:
            if endpoint in SUPPORTED_ENDPOINTS:
                client_function = getattr(self.client, f"get_{endpoint}")
                self.retrieve_data(endpoint, employee_ids, client_function)
            else:
                raise UserException(f"Unsupported endpoint: {endpoint}.")

        self.write_state_file(self.state)

    def get_employees(self, human_readable: bool) -> list:
        """Saves employee data from https://apidocs.hibob.com/reference/get_people into csv and returns
        a list of employee_ids."""
        table_name = "employees"
        columns = self.state.get(table_name, [])
        table = self.create_out_table_definition(f'{table_name}.csv', incremental=self.incremental, primary_key=['id'])
        employee_ids = []
        with ElasticDictWriter(table.full_path, fieldnames=columns, extrasaction="ignore") as wr:
            for employee in self.client.get_employees(human_readable=human_readable):
                row = self.flatten_dictionary(employee)

                self.add_col_to_state(table_name, row)

                wr.writerow(row)

                if employee.get("id"):
                    employee_ids.append(employee.get("id"))

            wr.writeheader()

        self.write_manifest(table)
        return employee_ids

    def retrieve_data(self, table_name, employee_ids, client_function) -> None:
        logging.info(f"Retrieving {table_name}.")

        columns = self.state.get(table_name, [])
        table = self.create_out_table_definition(f'{table_name}.csv', incremental=self.incremental,
                                                 primary_key=['id'])
        with ElasticDictWriter(table.full_path, fieldnames=columns, extrasaction="ignore") as wr:
            for employee_id in employee_ids:
                result = client_function(employee_id)
                for record in result:
                    row = self.flatten_dictionary(record)
                    self.add_col_to_state(table_name, row)
                    wr.writerow(row)
            wr.writeheader()

        self.write_manifest(table)

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    @staticmethod
    def flatten_dictionary(nested_dict, sep='_'):
        """
        Flatten a nested dictionary by combining nested keys using the specified separator.

        This function takes a nested dictionary as input and flattens it by combining keys using specified separator.
        If a key is longer than 64 characters, it will be truncated to ensure that created keys are of max length 64.
        Any forward slashes in the keys will be replaced with underscores, and keys will not start with an underscore.

        Parameters:
            nested_dict (dict): The nested dictionary to be flattened.
            sep (str, optional): The separator to use for combining keys. Defaults to '_'.

        Returns:
            dict: A new dictionary with flattened keys.
        """

        def _flatten(d, parent_key='', result=None):
            if result is None:
                result = {}

            for key, value in d.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                # Replace forward slash with underscore in the key
                new_key = new_key.replace("/", "_")

                # Remove the starting underscore from the key, if present
                if new_key.startswith(sep):
                    new_key = new_key[len(sep):]

                if isinstance(value, dict):
                    _flatten(value, new_key, result)
                else:
                    # Truncate the key to a maximum length of 64 characters
                    max_key_length = 64
                    truncated_key = new_key[:max_key_length]
                    result[truncated_key] = value
            return result

        return _flatten(nested_dict)

    def add_col_to_state(self, table_name: str, row: dict):
        if table_name not in self.state:
            self.state[table_name] = []
        columns = list(row.keys())

        for column in columns:
            if column not in self.state[table_name]:
                self.state[table_name].append(column)
                logging.info(f"Adding new column {column} for table {table_name} to statefile.")

    @sync_action("testConnection")
    def test_connection(self):
        """Currently not used, since it takes too long to fetch all employees."""
        self._init_configuration()

        service_user_id = self._configuration.authorization.service_user_id
        service_user_token = self._configuration.authorization.pswd_service_user_token

        client = HiBobClient(service_user_id, service_user_token)

        if not client.test_connection():
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
