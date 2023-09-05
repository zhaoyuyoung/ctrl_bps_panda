# This file is part of ctrl_bps_panda.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
