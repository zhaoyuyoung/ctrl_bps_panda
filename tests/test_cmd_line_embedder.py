"""Unit tests for cmd_line_embedder.py
"""
import unittest

from lsst.ctrl.bps import GenericWorkflowFile
from lsst.ctrl.bps.panda.cmd_line_embedder import CommandLineEmbedder


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


if __name__ == "__main__":
    unittest.main()
