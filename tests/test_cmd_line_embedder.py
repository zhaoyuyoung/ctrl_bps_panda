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

"""Unit tests for cmd_line_embedder.py
"""
import unittest

from lsst.ctrl.bps import GenericWorkflowFile
from lsst.ctrl.bps.panda.cmd_line_embedder import CommandLineEmbedder
from lsst.ctrl.bps.panda.constants import PANDA_MAX_LEN_INPUT_FILE


class TestCmdLineEmbedder(unittest.TestCase):
    """Test CmdLineEmbedder class"""

    def setUp(self):
        self.cmd_line_0 = "<ENV:CTRL_MPEXEC_DIR>/mycmd -b {key1} -c {key2} -d {key3} {key4}"
        self.cmd_line_1 = "<ENV:CTRL_MPEXEC_DIR>/mycmd -b <FILE:file3> -c <FILE:file4> -d {key3} {key4}"
        self.cmd_line_2 = (
            "<ENV:CTRL_MPEXEC_DIR>/mycmd -b <FILE:sfile1> -c <FILE:file3> -d <FILE:sfile2> <FILE:file4>"
        )
        self.sfile1 = GenericWorkflowFile("sfile1", "/path/to/sfile1.yaml", False, True, True)
        self.sfile2 = GenericWorkflowFile("sfile2", "/path/to/sfile2.yaml", False, True, True)
        self.file3 = GenericWorkflowFile("file3", "/path/to/file3.yaml", True, False, False)
        self.file4 = GenericWorkflowFile("file4", "/path/to/file4.yaml", True, False, False)

        self.ans_cmd_line_0 = "<ENV:CTRL_MPEXEC_DIR>/mycmd -b {key1} -c {key2} -d {key3} {key4}"
        self.ans_cmd_line_1 = "<ENV:CTRL_MPEXEC_DIR>/mycmd -b <FILE:file3> -c <FILE:file4> -d {key3} {key4}"
        self.ans_cmd_line_2 = (
            "<ENV:CTRL_MPEXEC_DIR>/mycmd -b /path/to/sfile1.yaml -c <FILE:file3> "
            "-d /path/to/sfile2.yaml <FILE:file4>"
        )

    def testReplaceStaticFilesNoFiles(self):
        orig_cmd_line = self.cmd_line_0
        orig_cmd_line_copy = orig_cmd_line

        cmd_line_embedder = CommandLineEmbedder({})
        new_cmd_line = cmd_line_embedder.replace_static_files(orig_cmd_line, [])

        # Ensure no side effect
        self.assertEqual(orig_cmd_line, orig_cmd_line_copy)
        self.assertEqual(new_cmd_line, orig_cmd_line_copy)

    def testReplaceStaticFilesMissingFiles(self):
        orig_cmd_line = self.cmd_line_1
        orig_cmd_line_copy = orig_cmd_line

        cmd_line_embedder = CommandLineEmbedder({})
        with self.assertRaises(RuntimeError):
            _ = cmd_line_embedder.replace_static_files(orig_cmd_line, [self.sfile1, self.file3])

        self.assertEqual(orig_cmd_line, orig_cmd_line_copy)

    def testReplaceStaticFilesNone(self):
        orig_cmd_line = self.cmd_line_1
        orig_cmd_line_copy = orig_cmd_line

        cmd_line_embedder = CommandLineEmbedder({})
        new_cmd_line = cmd_line_embedder.replace_static_files(
            orig_cmd_line, [self.sfile1, self.sfile2, self.file3, self.file4]
        )

        self.assertEqual(orig_cmd_line, orig_cmd_line_copy)
        self.assertEqual(new_cmd_line, self.ans_cmd_line_1)

    def testReplaceStaticFilesSome(self):
        orig_cmd_line = self.cmd_line_2
        orig_cmd_line_copy = orig_cmd_line

        cmd_line_embedder = CommandLineEmbedder({})
        new_cmd_line = cmd_line_embedder.replace_static_files(
            orig_cmd_line, [self.sfile1, self.sfile2, self.file3, self.file4]
        )

        self.assertEqual(orig_cmd_line, orig_cmd_line_copy)
        self.assertEqual(new_cmd_line, self.ans_cmd_line_2)

    def testTooLongPseudoFilename(self):
        cmd_line_embedder = CommandLineEmbedder({})
        with self.assertRaises(RuntimeError):
            _, _ = cmd_line_embedder.substitute_command_line("", {}, "j" * PANDA_MAX_LEN_INPUT_FILE, [])

    def testOKPseudoFilename(self):
        cmd_line_embedder = CommandLineEmbedder({})
        _, name = cmd_line_embedder.substitute_command_line("", {}, "j" * 15, [])
        self.assertIn("j" * 15, name)


if __name__ == "__main__":
    unittest.main()
