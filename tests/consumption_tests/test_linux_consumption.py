# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import os
import sys
from time import sleep
from unittest import TestCase
from requests import Request
from testutils_lc import FlexConsumptionWebHostController

_DEFAULT_HOST_VERSION = "4"


class TestFlexConsumption(TestCase):

    @classmethod
    def setUpClass(cls):

        os.environ["_DUMMY_CONT_KEY"] = "MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlBQkNERUY="
        os.environ["AzureWebJobsScriptRoot"] = "/home/site/wwwroot"
        os.environ["AzureWebJobsStorage"] = "DefaultEndpointsProtocol=http;AccountName=account1;AccountKey=key1;BlobEndpoint=http://account1.blob.localhost:10000;QueueEndpoint=http://account1.queue.localhost:10001;TableEndpoint=http://account1.table.localhost:10002;"

        cls._storage = os.getenv('AzureWebJobsStorage')
        if cls._storage is None:
            raise RuntimeError('Environment variable AzureWebJobsStorage is '
                               'required before running Flex Consumption test')

    def test_placeholder_mode_root_returns_ok(self):
        """In any circumstances, a placeholder container should returns 200
        even when it is not specialized.
        """
        with FlexConsumptionWebHostController(_DEFAULT_HOST_VERSION) as ctrl:
            req = Request('GET', ctrl.url)
            resp = ctrl.send_request(req)
            self.assertTrue(resp.ok)

    def test_http_trigger(self):
        """An HttpTrigger function app with Go worker
        should return 200.
        """
        with FlexConsumptionWebHostController(_DEFAULT_HOST_VERSION) as ctrl:
            ctrl.assign_container(env={
                "AzureWebJobsStorage": self._storage,
                "SCM_RUN_FROM_PACKAGE": self._get_function_app("HttpTrigger")
            })
            req = Request('GET', f'{ctrl.url}/api/hello')
            resp = ctrl.send_request(req)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.content, b"Hello from Go Worker!")

    def test_blob_trigger(self):
        """A BlobTrigger function app with Go worker should process blob
        and return 200.
        """
        with FlexConsumptionWebHostController(_DEFAULT_HOST_VERSION) as ctrl:
            ctrl.assign_container(env={
                "AzureWebJobsStorage": self._storage,
                "SCM_RUN_FROM_PACKAGE": self._get_function_app("BlobTrigger")
            })
            # Give some time for the function app to start and process existing blobs
            sleep(10)
            req = Request('GET', f'{ctrl.url}/api/hello')
            resp = ctrl.send_request(req)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.content, b"Hello from Go Worker!")


    @staticmethod
    def _get_function_app(scenario_name: str) -> str:
        """Return the zip filename for the given test scenario."""
        return f"{scenario_name}.zip"
