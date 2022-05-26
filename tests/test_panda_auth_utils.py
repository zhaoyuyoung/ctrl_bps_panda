"""Unit tests for PanDA authentication utilities.
"""
import os
import unittest
from unittest import mock

from lsst.ctrl.bps.panda.panda_auth_utils import panda_auth_status


class TestPandaAuthUtils(unittest.TestCase):
    def testPandaAuthStatusWrongEnviron(self):
        unwanted = {
            "PANDA_AUTH",
            "PANDA_VERIFY_HOST",
            "PANDA_AUTH_VO",
            "PANDA_URL_SSL",
            "PANDA_URL",
        }
        test_environ = {key: val for key, val in os.environ.items() if key not in unwanted}
        with mock.patch.dict(os.environ, test_environ, clear=True):
            with self.assertRaises(OSError):
                panda_auth_status()


if __name__ == "__main__":
    unittest.main()
