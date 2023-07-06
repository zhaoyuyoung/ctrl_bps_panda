"""Unit tests for PanDA authentication utilities.
"""
import os
import unittest
from unittest import mock

from lsst.ctrl.bps.panda import __version__ as version
from lsst.ctrl.bps.panda.panda_auth_utils import panda_auth_status


class VersionTestCase(unittest.TestCase):
    """Test versioning."""

    def test_version(self):
        # Check that version is defined.
        self.assertIsNotNone(version)


class TestPandaAuthUtils(unittest.TestCase):
    """Simple test of auth utilities."""

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
