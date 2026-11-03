"""Tests for the warning raised when a table changes shape under an incremental load.

A full load recreates the table every run, so a new HiBob field simply appears. An
incremental load writes into the table that is already in Storage, which does not have
the new column. The component cannot change that table, so it warns.
"""
import logging
import unittest
from unittest import mock

from component import Component


def _component(incremental, state):
    component = Component.__new__(Component)
    component.incremental = incremental
    component.state = state
    component._column_drift_reported = set()
    return component


class TestColumnDriftWarning(unittest.TestCase):

    def test_new_column_on_an_existing_table_warns_when_incremental(self):
        component = _component(True, {"employees": ["id", "name"]})

        with self.assertLogs(level="WARNING") as logs:
            component.add_col_to_state("employees", {"id": 1, "name": "a", "nickname": "b"})

        self.assertIn("nickname", "".join(logs.output))
        self.assertIn("employees", self.state_tables(component))

    def test_no_warning_under_full_load(self):
        component = _component(False, {"employees": ["id", "name"]})

        with mock.patch.object(logging, "warning") as warning:
            component.add_col_to_state("employees", {"id": 1, "name": "a", "nickname": "b"})

        warning.assert_not_called()

    def test_no_warning_on_the_first_run(self):
        """The first run builds the table, so every column is new and that is normal."""
        component = _component(True, {})

        with mock.patch.object(logging, "warning") as warning:
            component.add_col_to_state("employees", {"id": 1, "name": "a"})

        warning.assert_not_called()

    def test_no_warning_when_the_columns_are_unchanged(self):
        component = _component(True, {"employees": ["id", "name"]})

        with mock.patch.object(logging, "warning") as warning:
            component.add_col_to_state("employees", {"id": 1, "name": "a"})

        warning.assert_not_called()

    def test_warning_is_logged_once_per_table(self):
        component = _component(True, {"employees": ["id"]})

        with self.assertLogs(level="WARNING") as logs:
            component.add_col_to_state("employees", {"id": 1, "a": 1, "b": 2})
            component.add_col_to_state("employees", {"id": 1, "c": 3})

        drift_warnings = [line for line in logs.output if "load type is incremental" in line]
        self.assertEqual(1, len(drift_warnings))

    def test_every_new_column_still_reaches_the_state(self):
        component = _component(True, {"employees": ["id"]})

        with self.assertLogs(level="WARNING"):
            component.add_col_to_state("employees", {"id": 1, "a": 1, "b": 2})

        self.assertEqual(["id", "a", "b"], component.state["employees"])

    @staticmethod
    def state_tables(component):
        return list(component.state)


if __name__ == "__main__":
    unittest.main()
