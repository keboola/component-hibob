"""Tests for the HiBob bulk history endpoints and the per-employee fallback.

The important property is that the bulk path produces exactly the same
(employee_id, record) pairs as the per-employee path it replaces, so existing
output tables keep the same rows, the same employee_id column and the same
[id, employee_id] primary key.
"""
import logging
import unittest
from unittest import mock

import requests

from client.client import (
    HiBobClient,
    HiBobException,
    HiBobBulkUnavailableException,
    MAX_PAGE_SIZE,
)
from component import Component


def _client():
    return HiBobClient("service-user-id", "service-user-token")


def _bulk_page(results, next_cursor=None, errors=None):
    page = {"results": results, "response_metadata": {"next_cursor": next_cursor}}
    if errors is not None:
        page["errors"] = errors
    return page


def _echo_page(next_cursor=None):
    """Mimic HiBob: one result object per requested employee, with no history entries."""
    def _responder(_endpoint, params=None, **_kwargs):
        ids = params["employeeIds"].split(",")
        return _bulk_page([{"employeeId": i, "values": []} for i in ids], next_cursor=next_cursor)
    return _responder


def _http_error(status_code):
    response = requests.Response()
    response.status_code = status_code
    return requests.exceptions.HTTPError(response=response)


class TestEndpointMapping(unittest.TestCase):

    def test_every_supported_endpoint_has_a_bulk_and_a_single_path(self):
        """Guards against the component and the client drifting apart."""
        from client.client import HISTORY_ENDPOINTS
        from component import SUPPORTED_ENDPOINTS

        self.assertEqual(sorted(SUPPORTED_ENDPOINTS), sorted(HISTORY_ENDPOINTS))


class TestBulkBatching(unittest.TestCase):

    def test_employee_ids_are_sent_in_batches_of_200(self):
        employee_ids = [str(i) for i in range(450)]
        client = _client()

        with mock.patch.object(HiBobClient, "get", side_effect=_echo_page()) as get:
            list(client.get_history_in_bulk("employment_history", employee_ids))

        self.assertEqual(3, get.call_count)
        sent_batches = [call.kwargs["params"]["employeeIds"].split(",") for call in get.call_args_list]
        self.assertEqual([200, 200, 50], [len(batch) for batch in sent_batches])
        # Every employee is requested exactly once, in the original order.
        self.assertEqual(employee_ids, [i for batch in sent_batches for i in batch])

    def test_request_targets_the_bulk_path_with_the_max_page_size(self):
        client = _client()
        with mock.patch.object(HiBobClient, "get", side_effect=_echo_page()) as get:
            list(client.get_history_in_bulk("employee_work_history", ["1"]))

        self.assertEqual("bulk/people/work", get.call_args.args[0])
        self.assertEqual(MAX_PAGE_SIZE, get.call_args.kwargs["params"]["limit"])

    def test_no_cursor_is_sent_on_the_first_request(self):
        client = _client()
        with mock.patch.object(HiBobClient, "get", side_effect=_echo_page()) as get:
            list(client.get_history_in_bulk("employee_lifecycle", ["1"]))

        self.assertNotIn("cursor", get.call_args.kwargs["params"])


class TestBulkPagination(unittest.TestCase):

    def test_cursor_is_followed_until_next_cursor_is_empty(self):
        pages = [
            _bulk_page([{"employeeId": "1", "values": [{"id": "a"}]}], next_cursor="CURSOR-2"),
            _bulk_page([{"employeeId": "1", "values": [{"id": "b"}]}], next_cursor=None),
        ]
        client = _client()

        with mock.patch.object(HiBobClient, "get", side_effect=pages) as get:
            records = list(client.get_history_in_bulk("employment_history", ["1"]))

        self.assertEqual(2, get.call_count)
        self.assertEqual("CURSOR-2", get.call_args_list[1].kwargs["params"]["cursor"])
        # The employee ids stay on the paged request, so the page cannot widen its scope.
        self.assertEqual("1", get.call_args_list[1].kwargs["params"]["employeeIds"])
        self.assertEqual([("1", {"id": "a"}), ("1", {"id": "b"})], records)

    def test_repeating_cursor_fails_instead_of_looping(self):
        """A cursor that never advances would duplicate a page and truncate the batch."""
        client = _client()
        looping = _bulk_page([{"employeeId": "1", "values": [{"id": "a"}]}], next_cursor="SAME")

        with mock.patch.object(HiBobClient, "get", return_value=looping):
            with self.assertRaises(HiBobException) as raised:
                list(client.get_history_in_bulk("employment_history", ["1"]))

        self.assertIn("same page cursor twice", str(raised.exception))

    def test_missing_response_metadata_ends_pagination(self):
        client = _client()
        page = {"results": [{"employeeId": "1", "values": []}]}
        with mock.patch.object(HiBobClient, "get", return_value=page):
            self.assertEqual([], list(client.get_history_in_bulk("employment_history", ["1"])))


class TestBulkResponseParsing(unittest.TestCase):

    def test_flat_values_are_returned_as_entries(self):
        page = _bulk_page([{"employeeId": "7", "values": [{"id": "a"}, {"id": "b"}]}])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            records = list(client.get_history_in_bulk("employment_history", ["7"]))

        self.assertEqual([("7", {"id": "a"}), ("7", {"id": "b"})], records)

    def test_nested_values_wrapper_is_unwrapped(self):
        """The bulk specification describes values[] ambiguously; both shapes are accepted."""
        page = _bulk_page([
            {"employeeId": "7", "values": [{"values": [{"id": "a"}], "restricted_columns": {}}]}
        ])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            records = list(client.get_history_in_bulk("employment_history", ["7"]))

        self.assertEqual([("7", {"id": "a"})], records)

    def test_wrapper_with_an_extra_field_is_still_unwrapped(self):
        """HiBob may add a field to the wrapper; the entries must still come out."""
        page = _bulk_page([
            {"employeeId": "7", "values": [
                {"values": [{"id": "a"}], "restricted_columns": {}, "someNewField": 1}
            ]}
        ])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            self.assertEqual([("7", {"id": "a"})], list(client.get_history_in_bulk("employment_history", ["7"])))

    def test_entry_with_its_own_values_field_is_not_unwrapped(self):
        """A real entry is never mistaken for the wrapper."""
        entry = {"id": "a", "values": [{"id": "inner"}]}
        page = _bulk_page([{"employeeId": "7", "values": [entry]}])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            records = list(client.get_history_in_bulk("employment_history", ["7"]))

        self.assertEqual([("7", entry)], records)

    def test_employee_without_history_yields_no_rows(self):
        page = _bulk_page([{"employeeId": "7", "values": []}, {"employeeId": "8", "values": None}])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            self.assertEqual([], list(client.get_history_in_bulk("employment_history", ["7", "8"])))

    def test_result_without_employee_id_is_skipped(self):
        page = _bulk_page([{"values": [{"id": "a"}]}, {"employeeId": "8", "values": [{"id": "b"}]}])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            records = list(client.get_history_in_bulk("employment_history", ["7", "8"]))

        self.assertEqual([("8", {"id": "b"})], records)

    def test_refused_employees_are_reported_and_the_run_continues(self):
        """In HiBob the employee scope and the table permissions are set separately, so a
        partially readable tenant is legitimate. The run keeps the employees it did get."""
        page = _bulk_page(
            [{"employeeId": "8", "values": [{"id": "b"}]}],
            errors=[{"employeeId": {"error": "MISSING_PERMISSION", "message": "no permission"}}],
        )
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            with self.assertLogs(level="WARNING") as logs:
                records = list(client.get_history_in_bulk("employment_history", ["7", "8"]))

        self.assertEqual([("8", {"id": "b"})], records)
        output = "".join(logs.output)
        self.assertIn("MISSING_PERMISSION", output)
        self.assertIn("incomplete", output)

    def test_all_employees_refused_stops_the_run(self):
        """Nothing came back at all, so a full load would empty the whole table."""
        page = _bulk_page(
            [],
            errors=[{"employeeId": {"error": "MISSING_PERMISSION", "message": "no permission"}}],
        )
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            with self.assertRaises(HiBobException) as raised:
                list(client.get_history_in_bulk("employment_history", ["7"]))

        self.assertIn("returned no employee at all", str(raised.exception))

    def test_empty_errors_field_does_not_stop_the_run(self):
        page = _bulk_page([{"employeeId": "8", "values": [{"id": "b"}]}], errors=[])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            self.assertEqual([("8", {"id": "b"})], list(client.get_history_in_bulk("employment_history", ["8"])))

    def test_unrequested_employee_id_stops_the_run(self):
        """employee_id must keep matching the employees table."""
        page = _bulk_page([{"employeeId": "999", "values": [{"id": "b"}]}])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            with self.assertRaises(HiBobException) as raised:
                list(client.get_history_in_bulk("employment_history", ["7", "8"]))

        self.assertIn("999", str(raised.exception))


class TestEmptyResponseGuard(unittest.TestCase):
    """A response shape mismatch must not look like 'this tenant has no history'."""

    def test_no_results_at_all_stops_the_run(self):
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=_bulk_page([])):
            with self.assertRaises(HiBobException) as raised:
                list(client.get_history_in_bulk("employment_history", ["1", "2"]))

        self.assertIn("nothing at all", str(raised.exception))

    def test_results_with_empty_history_are_accepted(self):
        """A tenant where nobody has history is legitimate and must not fail."""
        client = _client()
        with mock.patch.object(HiBobClient, "get", side_effect=_echo_page()):
            self.assertEqual([], list(client.get_history_in_bulk("employment_history", ["1", "2"])))

    def test_no_employees_requested_does_not_fail(self):
        client = _client()
        with mock.patch.object(HiBobClient, "get") as get:
            self.assertEqual([], list(client.get_history_in_bulk("employment_history", [])))
        get.assert_not_called()


class TestRestrictedColumns(unittest.TestCase):

    def test_restricted_columns_are_reported_once(self):
        """Field-level permissions silently drop columns, so the run must say so."""
        page = _bulk_page([
            {"employeeId": "1", "values": [{"values": [{"id": "a"}], "restricted_columns": {"salary": True}}]},
            {"employeeId": "2", "values": [{"values": [{"id": "b"}], "restricted_columns": {"salary": True}}]},
        ])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            with self.assertLogs(level="WARNING") as logs:
                records = list(client.get_history_in_bulk("employment_history", ["1", "2"]))

        self.assertEqual([("1", {"id": "a"}), ("2", {"id": "b"})], records)
        warnings = [line for line in logs.output if "partial column permissions" in line]
        self.assertEqual(1, len(warnings), "the warning must be logged once, not per employee")

    def test_no_warning_when_no_columns_are_restricted(self):
        page = _bulk_page([{"employeeId": "1", "values": [{"values": [{"id": "a"}], "restricted_columns": {}}]}])
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value=page):
            with mock.patch.object(logging, "warning") as warning:
                list(client.get_history_in_bulk("employment_history", ["1"]))
        warning.assert_not_called()


class TestRateLimits(unittest.TestCase):

    def test_documented_hibob_rate_limits_are_used(self):
        """Bulk is documented at 100/min, the per-employee history endpoints at 50/min."""
        from client.client import BULK_CALLS_PER_MINUTE, PER_EMPLOYEE_CALLS_PER_MINUTE
        self.assertEqual(100, BULK_CALLS_PER_MINUTE)
        self.assertEqual(50, PER_EMPLOYEE_CALLS_PER_MINUTE)


class TestBulkAvailability(unittest.TestCase):

    def test_permission_errors_are_raised_as_bulk_unavailable(self):
        for status_code in (403, 404):
            with self.subTest(status_code=status_code):
                client = _client()
                with mock.patch.object(HiBobClient, "get", side_effect=_http_error(status_code)):
                    with self.assertRaises(HiBobBulkUnavailableException):
                        list(client.get_history_in_bulk("employment_history", ["1"]))

    def test_rejected_credentials_stay_fatal(self):
        """A bad token would be rejected by the per-employee endpoints too, so do not fall back."""
        client = _client()
        with mock.patch.object(HiBobClient, "get", side_effect=_http_error(401)):
            with self.assertRaises(HiBobException) as raised:
                list(client.get_history_in_bulk("employment_history", ["1"]))

        self.assertNotIsInstance(raised.exception, HiBobBulkUnavailableException)
        self.assertFalse(client.bulk_unavailable)

    def test_unavailable_bulk_api_is_not_retried_for_the_next_endpoint(self):
        client = _client()
        with mock.patch.object(HiBobClient, "get", side_effect=_http_error(403)) as get:
            with self.assertRaises(HiBobBulkUnavailableException):
                list(client.get_history_in_bulk("employment_history", ["1"]))
            self.assertTrue(client.bulk_unavailable)
            with self.assertRaises(HiBobBulkUnavailableException):
                list(client.get_history_in_bulk("employee_lifecycle", ["1"]))

        self.assertEqual(1, get.call_count, "the second endpoint must not call the bulk API again")

    def test_other_http_errors_stay_fatal(self):
        client = _client()
        with mock.patch.object(HiBobClient, "get", side_effect=_http_error(500)):
            with self.assertRaises(HiBobException) as raised:
                list(client.get_history_in_bulk("employment_history", ["1"]))
        self.assertNotIsInstance(raised.exception, HiBobBulkUnavailableException)


class TestPerEmployeeFallbackPath(unittest.TestCase):

    def test_per_employee_request_uses_the_original_path(self):
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value={"values": [{"id": "a"}]}) as get:
            records = list(client.get_history_per_employee("employee_lifecycle", "7"))

        self.assertEqual("people/7/lifecycle", get.call_args.args[0])
        self.assertEqual([("7", {"id": "a"})], records)

    def test_employee_without_history_does_not_fail(self):
        """A missing values field used to raise TypeError."""
        client = _client()
        with mock.patch.object(HiBobClient, "get", return_value={}):
            self.assertEqual([], list(client.get_history_per_employee("employment_history", "7")))


class TestComponentFallback(unittest.TestCase):
    """_iterate_history is exercised directly so no data directory is needed."""

    @staticmethod
    def _component(client):
        component = Component.__new__(Component)
        component.client = client
        return component

    def test_bulk_result_is_used_when_available(self):
        client = mock.Mock()
        client.get_history_in_bulk.return_value = iter([("1", {"id": "a"})])
        component = self._component(client)

        self.assertEqual([("1", {"id": "a"})], list(component._iterate_history("employment_history", ["1"])))
        client.get_history_per_employee.assert_not_called()

    def test_falls_back_to_per_employee_when_bulk_is_unavailable(self):
        client = mock.Mock()
        client.get_history_in_bulk.side_effect = HiBobBulkUnavailableException("no bulk")
        client.get_history_per_employee.side_effect = lambda _table, employee_id: iter(
            [(employee_id, {"id": f"record-{employee_id}"})]
        )
        component = self._component(client)

        with self.assertLogs(level="WARNING"):
            records = list(component._iterate_history("employment_history", ["1", "2"]))

        self.assertEqual([("1", {"id": "record-1"}), ("2", {"id": "record-2"})], records)

    def test_failure_on_the_fallback_is_a_user_error_not_a_crash(self):
        """A permission failure must read as a user error, not exit code 2."""
        from keboola.component.exceptions import UserException

        client = _client()
        with mock.patch.object(HiBobClient, "get", side_effect=_http_error(403)):
            component = Component.__new__(Component)
            component.client = client
            with self.assertLogs(level="WARNING"):
                with self.assertRaises(UserException):
                    list(component._iterate_history("employment_history", ["1"]))

    def test_no_silent_fallback_after_partial_output(self):
        """Falling back mid-stream would duplicate rows, so the run must fail instead."""
        def partial():
            yield "1", {"id": "a"}
            raise HiBobBulkUnavailableException("lost access")

        client = mock.Mock()
        client.get_history_in_bulk.return_value = partial()
        component = self._component(client)

        with self.assertRaises(Exception) as raised:
            list(component._iterate_history("employment_history", ["1", "2"]))

        self.assertIn("stopped after 1 records", str(raised.exception))
        client.get_history_per_employee.assert_not_called()


if __name__ == "__main__":
    unittest.main()
