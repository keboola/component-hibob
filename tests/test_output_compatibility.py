"""End-to-end check that the bulk endpoints keep the output exactly as it was.

The component is run twice over the same fake HiBob data: once with the bulk
endpoints available, and once with the bulk endpoints refused so the per-employee
fallback is taken. Both runs must produce the same CSV rows and the same manifests,
because existing configurations must keep their tables, columns and primary keys.
"""
import csv
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import requests

EMPLOYEES = {"employees": [{"id": "1", "displayName": "A"}, {"id": "2", "displayName": "B"}]}
HISTORY = {
    "1": [{"id": "e1", "effectiveDate": "2020-01-01", "reason": {"id": "r1", "name": "Hire"}}],
    "2": [{"id": "e2", "effectiveDate": "2021-02-02", "reason": {"id": "r2", "name": "Promo"}}],
}


def _forbidden():
    response = requests.Response()
    response.status_code = 403
    return requests.exceptions.HTTPError(response=response)


class TestOutputCompatibility(unittest.TestCase):

    def _run_component(self, bulk_available, load_type="full_load"):
        """Run the component against fake HiBob data and return its output folder content."""
        data_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, data_dir, True)
        os.makedirs(os.path.join(data_dir, "out", "tables"))
        config = {
            "parameters": {
                "authorization": {"service_user_id": "u", "#service_user_token": "t"},
                "endpoints": ["employment_history"],
                "destination": {"load_type": load_type},
            }
        }
        with open(os.path.join(data_dir, "config.json"), "w") as config_file:
            json.dump(config, config_file)

        def fake_get(_self, endpoint, params=None, **kwargs):
            if endpoint.startswith("bulk/"):
                if not bulk_available:
                    raise _forbidden()
                ids = params["employeeIds"].split(",")
                # The bulk specification nests the entries inside a
                # {"values": [...], "restricted_columns": ...} wrapper.
                return {
                    "results": [
                        {"employeeId": i, "values": [{"values": HISTORY[i], "restricted_columns": {}}]}
                        for i in ids
                    ],
                    "response_metadata": {"next_cursor": None},
                }
            if not bulk_available:
                return {"values": HISTORY[endpoint.split("/")[1]], "restricted_columns": {}}
            raise AssertionError(f"The per-employee endpoint must not be used: {endpoint}")

        for module in ("component", "configuration", "client.client", "client"):
            sys.modules.pop(module, None)
        with mock.patch.dict(os.environ, {"KBC_DATADIR": data_dir}):
            from component import Component
            import client.client as client_module

            with mock.patch.object(client_module.HiBobClient, "get", fake_get), \
                    mock.patch.object(client_module.HiBobClient, "post", lambda *a, **k: EMPLOYEES):
                Component().run()

        tables_dir = os.path.join(data_dir, "out", "tables")
        output = {}
        for name in sorted(os.listdir(tables_dir)):
            path = os.path.join(tables_dir, name)
            with open(path) as handle:
                output[name] = json.load(handle) if name.endswith(".manifest") else list(csv.DictReader(handle))
        return output

    def test_bulk_and_per_employee_fallback_produce_identical_output(self):
        via_bulk = self._run_component(bulk_available=True)
        via_fallback = self._run_component(bulk_available=False)
        self.assertEqual(via_bulk, via_fallback)

    def test_rows_columns_and_primary_key_are_unchanged(self):
        output = self._run_component(bulk_available=True)
        rows = output["employment_history.csv"]
        manifest = output["employment_history.csv.manifest"]

        self.assertEqual(["id", "employee_id"], manifest["primary_key"])
        self.assertEqual(2, len(rows))
        # The employee_id column and the flattened nested fields are kept as they were.
        self.assertEqual(
            {"id", "effectiveDate", "reason_id", "reason_name", "employee_id"},
            set(rows[0]),
        )
        self.assertEqual({"1", "2"}, {row["employee_id"] for row in rows})



if __name__ == "__main__":
    unittest.main()
