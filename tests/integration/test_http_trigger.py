"""Integration test for HTTP trigger sample."""

import requests

ENV = {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "golang",
}


def test_http_trigger_get(func_host):
    proc = func_host("httpTrigger", 7201, ENV)

    resp = requests.get(f"http://localhost:{proc.port}/api/hello", timeout=5)

    assert resp.status_code == 200
    assert resp.content == b"Hello from Go Worker!"


def test_http_trigger_post(func_host):
    proc = func_host("httpTrigger", 7202, ENV)

    resp = requests.post(f"http://localhost:{proc.port}/api/hello", data="test body", timeout=5)

    assert resp.status_code == 200
    assert resp.content == b"Hello from Go Worker!"
