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

"""Unit tests for ctrl_bps_panda utilities.
"""
import unittest

from lsst.ctrl.bps import GenericWorkflowExec, GenericWorkflowJob
from lsst.ctrl.bps.panda.constants import PANDA_MAX_LEN_INPUT_FILE
from lsst.ctrl.bps.panda.utils import _make_pseudo_filename


class TestPandaUtils(unittest.TestCase):
    """Simple test of utilities."""

    def testTooLongPseudoFilename(self):
        # define enough of a job for this test
        myexec = GenericWorkflowExec("test_exec")
        myexec.src_uri = "/dummy/path/test_exec"
        gwjob = GenericWorkflowJob("j" * PANDA_MAX_LEN_INPUT_FILE)
        gwjob.executable = myexec
        gwjob.arguments = ""
        with self.assertRaises(RuntimeError):
            _ = _make_pseudo_filename({}, gwjob)

    def testOKPseudoFilename(self):
        # define enough of a job for this test
        myexec = GenericWorkflowExec("test_exec")
        myexec.src_uri = "/dummy/path/test_exec"
        gwjob = GenericWorkflowJob("j" * 15)
        gwjob.executable = myexec
        gwjob.arguments = ""
        name = _make_pseudo_filename({}, gwjob)
        self.assertIn("j" * 15, name)


if __name__ == "__main__":
    unittest.main()
