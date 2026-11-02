"""Regression tests for the Destination.load_type parsing and the incremental flag.

Before the fix, Destination.load_type was typed List[str] while the UI schema stored a
string. dataconf therefore coerced "incremental_load" into a list of single characters,
so the `== "incremental_load"` comparison was never true and every table was imported as
a full load, even when the configuration asked for an incremental load.
"""
import unittest

from configuration import Configuration
from component import Component


def _params(load_type=None, with_destination=True):
    params = {
        "authorization": {"service_user_id": "id", "#service_user_token": "token"},
        "endpoints": ["employment_history"],
    }
    if with_destination:
        params["destination"] = {} if load_type is None else {"load_type": load_type}
    return params


class TestLoadTypeConfiguration(unittest.TestCase):

    def test_incremental_load_is_parsed_as_string(self):
        """The regression: the value must stay a string, not become a list of characters."""
        config = Configuration.load_from_dict(_params("incremental_load"))
        self.assertEqual("incremental_load", config.destination.load_type)

    def test_full_load_is_parsed_as_string(self):
        config = Configuration.load_from_dict(_params("full_load"))
        self.assertEqual("full_load", config.destination.load_type)

    def test_missing_load_type_defaults_to_full_load(self):
        """Configurations saved before load_type existed must still load."""
        config = Configuration.load_from_dict(_params())
        self.assertEqual("full_load", config.destination.load_type)

    def test_legacy_list_value_still_loads(self):
        """A hand-crafted configuration holding a list must not fail to load."""
        config = Configuration.load_from_dict(_params(["incremental_load"]))
        self.assertEqual(["incremental_load"], config.destination.load_type)


class TestResolveIncremental(unittest.TestCase):

    def test_incremental_load_enables_incremental(self):
        self.assertTrue(Component._resolve_incremental("incremental_load"))

    def test_full_load_disables_incremental(self):
        self.assertFalse(Component._resolve_incremental("full_load"))

    def test_legacy_list_value_enables_incremental(self):
        self.assertTrue(Component._resolve_incremental(["incremental_load"]))

    def test_legacy_list_full_load_disables_incremental(self):
        self.assertFalse(Component._resolve_incremental(["full_load"]))

    def test_empty_and_unknown_values_fall_back_to_full_load(self):
        for value in ([], "", "something_else", None):
            with self.subTest(value=value):
                self.assertFalse(Component._resolve_incremental(value))

    def test_end_to_end_incremental_flag_from_configuration(self):
        """The whole path config dict -> dataclass -> incremental flag."""
        config = Configuration.load_from_dict(_params("incremental_load"))
        self.assertTrue(Component._resolve_incremental(config.destination.load_type))

        config = Configuration.load_from_dict(_params("full_load"))
        self.assertFalse(Component._resolve_incremental(config.destination.load_type))


if __name__ == "__main__":
    unittest.main()
