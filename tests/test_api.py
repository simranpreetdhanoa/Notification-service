import json
import threading
import unittest
import urllib.error
import urllib.request

from notifier.api import make_server

from tests.helpers import make_dispatcher


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = make_dispatcher()
        cls.d.start()
        cls.server = make_server(cls.d, port=0)
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.d.stop()

    def call(self, method, path, body=None, headers=None):
        req = urllib.request.Request(
            self.base + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_submit_and_track(self):
        status, receipt = self.call("POST", "/v1/notifications", {
            "channel": "sms", "recipient": "+15550100", "body": "code 123456",
            "priority": "critical"})
        self.assertEqual((status, receipt["priority"]), (202, 1))
        self.assertTrue(self.d.wait_until_idle())
        status, n = self.call("GET", f"/v1/notifications/{receipt['id']}")
        self.assertEqual((status, n["status"]), (200, "DISPATCHED"))
        self.assertEqual(n["history"][-1]["to_status"], "DISPATCHED")

    def test_idempotency_key_header(self):
        body = {"channel": "push", "recipient": "device-1", "body": "hi"}
        s1, r1 = self.call("POST", "/v1/notifications", body, {"Idempotency-Key": "abc"})
        s2, r2 = self.call("POST", "/v1/notifications", body, {"Idempotency-Key": "abc"})
        self.assertEqual((s1, s2), (202, 200))
        self.assertEqual(r1["id"], r2["id"])
        self.assertTrue(r2["duplicate"])

    def test_batch(self):
        status, resp = self.call("POST", "/v1/notifications/batch", {"notifications": [
            {"channel": "email", "recipient": f"u{i}@x.com", "subject": "Sale", "body": "b",
             "priority": 3} for i in range(5)]})
        self.assertEqual((status, len(resp["notifications"])), (202, 5))

    def test_batch_is_all_or_nothing_on_validation(self):
        before = self.call("GET", "/v1/stats")[1]["metrics"].get("accepted", 0)
        status, _ = self.call("POST", "/v1/notifications/batch", {"notifications": [
            {"channel": "sms", "recipient": "+1", "body": "ok"},
            {"channel": "fax", "recipient": "+1", "body": "nope"}]})
        self.assertEqual(status, 400)
        self.assertEqual(self.call("GET", "/v1/stats")[1]["metrics"].get("accepted", 0), before)

    def test_validation_errors(self):
        cases = [
            {"channel": "fax", "recipient": "x", "body": "b"},
            {"channel": "sms", "recipient": "", "body": "b"},
            {"channel": "sms", "recipient": "x", "body": "b", "priority": 9},
            {"channel": "email", "recipient": "a@b.c", "body": "no subject"},
        ]
        for body in cases:
            with self.subTest(body=body):
                self.assertEqual(self.call("POST", "/v1/notifications", body)[0], 400)

    def test_unknown_notification(self):
        self.assertEqual(self.call("GET", "/v1/notifications/nope")[0], 404)


if __name__ == "__main__":
    unittest.main()
