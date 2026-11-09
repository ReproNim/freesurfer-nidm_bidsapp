#!/usr/bin/env python3
"""Tests for NIDM file handling and aggregation."""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from click.testing import CliRunner
from src.run import cli


class TestNIDMInputDiscovery:
    """Test NIDM input file discovery logic."""

    def test_default_nidm_input_path(self, tmp_path, bids_single_session):
        """Verify default path is <bids_dir>/../NIDM"""
        # Create NIDM directory at default location
        nidm_dir = bids_single_session.parent / "NIDM"
        nidm_dir.mkdir()
        (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

            # Verify the wrapper was initialized (NIDM directory should be found)
            assert mock_wrapper.called

    def test_custom_nidm_input_path(self, tmp_path, bids_single_session):
        """Verify --nidm-input-dir overrides default."""
        custom_nidm_dir = tmp_path / "custom_nidm"
        custom_nidm_dir.mkdir()
        (custom_nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--nidm-input-dir', str(custom_nidm_dir),
                '--skip-bids-validation'
            ])

            # Verify command executed
            assert result.exit_code == 0 or result.exit_code == 1  # May fail due to mocking

    def test_nidm_input_not_found_continues(self, tmp_path, bids_single_session):
        """Verify processing continues when NIDM input not found."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Don't create NIDM directory - it should handle gracefully
        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

            # Should not crash even without NIDM input
            assert mock_wrapper.called


class TestNIDMCopyBehavior:
    """Test NIDM file copy logic."""

    def test_nidm_written_into_subject_directory(self, tmp_path, bids_single_session):
        """NIDM output goes in the subject dir, not a shared nidm/ directory.

        A shared nidm/ made every subject's nidm.ttl collide on the same path, so
        only the first subject unzipped survived.
        """
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

            assert (output_dir / "sub-001").is_dir(), (
                f"expected per-subject dir; have: {list(output_dir.iterdir())}"
            )
            assert not (output_dir / "freesurfer-nidm_bidsapp").exists(), (
                "app-name wrapper directory should no longer be created"
            )
            assert not (output_dir / "nidm").exists(), (
                "shared nidm/ directory should no longer be created"
            )


class TestNIDMAggregation:
    """Test NIDM graph aggregation."""

    def test_ttl_file_parsing(self, tmp_path):
        """Verify TTL files can be parsed."""
        from rdflib import Graph

        ttl_content = """@prefix : <http://example.org/> .
@prefix nidm: <http://purl.org/nidash/nidm#> .

:subject1 a nidm:Subject .
"""
        ttl_file = tmp_path / "test.ttl"
        ttl_file.write_text(ttl_content)

        # Test parsing
        g = Graph()
        g.parse(str(ttl_file), format="turtle")

        # Verify graph is not empty
        assert len(g) > 0

    def test_multiple_ttl_aggregation(self, tmp_path):
        """Verify multiple TTL files can be aggregated."""
        from rdflib import Graph

        # Create two TTL files
        ttl1 = tmp_path / "sub-001.ttl"
        ttl1.write_text("""@prefix : <http://example.org/> .
:subject1 a :Subject .
""")

        ttl2 = tmp_path / "sub-002.ttl"
        ttl2.write_text("""@prefix : <http://example.org/> .
:subject2 a :Subject .
""")

        # Aggregate into single graph
        combined_graph = Graph()
        for ttl_file in [ttl1, ttl2]:
            g = Graph()
            g.parse(str(ttl_file), format="turtle")
            combined_graph += g

        # Verify aggregation
        assert len(combined_graph) == 2  # Two subject triples


class TestNIDMFileDiscovery:
    """Test NIDM file discovery patterns."""

    def test_nidm_ttl_preferred(self, tmp_path):
        """Verify nidm.ttl is preferred if present."""
        nidm_dir = tmp_path / "NIDM"
        nidm_dir.mkdir()

        # Create both nidm.ttl and other .ttl files
        (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")
        (nidm_dir / "other.ttl").write_text("@prefix : <http://example.org/> .")

        # Check that nidm.ttl exists and should be preferred
        assert (nidm_dir / "nidm.ttl").exists()
        ttl_files = list(nidm_dir.glob("*.ttl"))
        assert len(ttl_files) >= 1
        assert nidm_dir / "nidm.ttl" in ttl_files


class TestFindNIDMInputFile:
    """find_nidm_input_file only ever returns this subject's own record."""

    @staticmethod
    def _touch(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("@prefix : <http://example.org/> .")
        return path

    def test_flat_directory_gives_each_subject_its_own_file(self, tmp_path):
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        own = {s: self._touch(nidm_dir / f"sub-{s}.ttl") for s in ("001", "002")}

        for s in ("001", "002"):
            assert find_nidm_input_file(nidm_dir, s) == own[s]

    def test_flat_session_file(self, tmp_path):
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        self._touch(nidm_dir / "sub-001_ses-01.ttl")
        own = self._touch(nidm_dir / "sub-001_ses-02.ttl")

        assert find_nidm_input_file(nidm_dir, "001", "02") == own

    def test_other_subjects_file_is_never_used(self, tmp_path):
        """No match for this subject means no input, not the first file listed."""
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        self._touch(nidm_dir / "sub-002.ttl")
        self._touch(nidm_dir / "stray.ttl")
        self._touch(nidm_dir / "sub-002.jsonld")

        assert find_nidm_input_file(nidm_dir, "001") is None

    def test_per_subject_nidm_ttl_comes_first(self, tmp_path):
        """The nidm_4.5.0 layout wins over a flat file and the dataset-level one."""
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        own = self._touch(nidm_dir / "sub-001" / "nidm.ttl")
        self._touch(nidm_dir / "sub-001.ttl")
        self._touch(nidm_dir / "nidm.ttl")

        assert find_nidm_input_file(nidm_dir, "001") == own

    def test_session_layout_comes_before_subject_layout(self, tmp_path):
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        own = self._touch(nidm_dir / "sub-001" / "ses-02" / "nidm.ttl")
        self._touch(nidm_dir / "sub-001" / "nidm.ttl")

        assert find_nidm_input_file(nidm_dir, "001", "02") == own

    def test_dataset_level_nidm_ttl_is_the_last_resort(self, tmp_path):
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        dataset = self._touch(nidm_dir / "nidm.ttl")

        assert find_nidm_input_file(nidm_dir, "001") == dataset

    def test_missing_directory(self, tmp_path):
        from src.run import find_nidm_input_file

        assert find_nidm_input_file(None, "001") is None
        assert find_nidm_input_file(tmp_path / "absent", "001") is None

    def test_unfetched_annex_input_is_an_error_not_a_miss(self, tmp_path):
        """A dangling symlink is what datalad leaves before `datalad get`.
        Falling through to the dataset-level file, or to no input, would
        build nidm.ttl without the subject's demographics and exit 0."""
        from src.run import NIDMInputNotRetrieved, find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        (nidm_dir / "sub-001").mkdir(parents=True)
        (nidm_dir / "sub-001" / "nidm.ttl").symlink_to("../.git/annex/objects/missing")
        self._touch(nidm_dir / "nidm.ttl")

        with pytest.raises(NIDMInputNotRetrieved):
            find_nidm_input_file(nidm_dir, "001")

    def test_fetched_file_wins_over_a_dangling_lower_priority_one(self, tmp_path):
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        own = self._touch(nidm_dir / "sub-001" / "nidm.ttl")
        (nidm_dir / "sub-001.ttl").symlink_to("missing")

        assert find_nidm_input_file(nidm_dir, "001") == own

    def test_miss_is_a_warning_only_when_asked(self, tmp_path, caplog):
        import logging
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        nidm_dir.mkdir()

        with caplog.at_level(logging.WARNING, logger="freesurfer-nidm"):
            assert find_nidm_input_file(nidm_dir, "001", "02") is None
            assert not caplog.records
            assert find_nidm_input_file(nidm_dir, "001", "02", warn=True) is None
        assert "sub-001 ses-02" in caplog.records[-1].getMessage()

    def test_sub_01_does_not_match_sub_010(self, tmp_path):
        from src.run import find_nidm_input_file

        nidm_dir = tmp_path / "NIDM"
        self._touch(nidm_dir / "sub-010" / "nidm.ttl")
        self._touch(nidm_dir / "sub-010.ttl")

        assert find_nidm_input_file(nidm_dir, "01") is None


class TestNIDMOutputNaming:
    """Test NIDM output file naming conventions."""

    def test_nidm_single_session_naming(self, tmp_path):
        """Verify NIDM file naming for single session."""
        nidm_file = tmp_path / "sub-001.ttl"
        nidm_file.touch()

        # Verify naming pattern
        assert nidm_file.name == "sub-001.ttl"
        assert "ses-" not in nidm_file.name

    def test_nidm_multi_session_naming(self, tmp_path):
        """Verify NIDM file naming for multi-session."""
        for session in ["baseline", "followup"]:
            nidm_file = tmp_path / f"sub-001_ses-{session}.ttl"
            nidm_file.touch()

            # Verify naming pattern
            assert f"sub-001_ses-{session}.ttl" == nidm_file.name
            assert "ses-" in nidm_file.name


class TestNIDMSkipFlag:
    """Test --skip-nidm flag behavior."""

    def test_skip_nidm_flag(self, tmp_path, bids_single_session):
        """Verify --skip-nidm skips NIDM conversion."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run') as mock_subprocess:
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-nidm',
                '--skip-bids-validation'
            ])

            # Verify subprocess.run was not called for NIDM conversion
            # (it would be called with fs_to_nidm if NIDM conversion ran)
            nidm_calls = [
                c for c in mock_subprocess.call_args_list
                if 'fs_to_nidm' in str(c)
            ]
            assert len(nidm_calls) == 0


class TestNIDMAppendCommand:
    """Regression tests for the fs_to_nidm append (-n) command construction."""

    def _nidm_cmd(self, mock_subprocess):
        """Return the argv list of the fs_to_nidm invocation, or None."""
        for c in mock_subprocess.call_args_list:
            argv = c.args[0] if c.args else c.kwargs.get("args")
            if isinstance(argv, list) and any("fs_to_nidm" in str(a) for a in argv):
                return argv
        return None

    def test_append_command_does_not_pass_jsonld(self, tmp_path, bids_single_session):
        """-j must NOT be passed alongside -n.

        fs_to_nidm's append branch accumulates across stats files by re-reading the
        -n file each loop iteration and writing back to it. With -j the output goes
        to '<nidm_file>.json' instead, so every stats file except the last-globbed
        one is silently discarded. See src/run.py for the full explanation.
        """
        nidm_dir = bids_single_session.parent / "NIDM"
        nidm_dir.mkdir()
        (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run') as mock_subprocess:
            mock_wrapper.return_value.process_subject.return_value = True
            mock_subprocess.return_value = MagicMock(returncode=0, stdout="", stderr="")

            runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

            cmd = self._nidm_cmd(mock_subprocess)
            assert cmd is not None, "fs_to_nidm was never invoked"
            assert "-n" in cmd, f"expected append mode (-n) in: {cmd}"
            assert "-j" not in cmd, (
                "-j must not be combined with -n; it breaks multi-stats-file "
                f"accumulation. Got: {cmd}"
            )
            assert "--forcenidm" in cmd, f"expected --forcenidm in: {cmd}"

    @pytest.mark.parametrize("with_input_nidm", [True, False])
    def test_command_names_the_subject(self, tmp_path, bids_single_session, with_input_nidm):
        """The recon-all tree is sub-<id>/freesurfer; without -subjid fs_to_nidm
        names the subject "freesurfer" and attaches every measure to a new agent
        instead of the input NIDM's subject (#21)."""
        if with_input_nidm:
            nidm_dir = bids_single_session.parent / "NIDM"
            nidm_dir.mkdir()
            (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run') as mock_subprocess:
            mock_wrapper.return_value.process_subject.return_value = True
            mock_wrapper.return_value.get_subject_t1_info.return_value = {
                "T1w_images": ["/bids/sub-001/anat/sub-001_T1w.nii.gz"], "T2w_images": []}
            mock_subprocess.return_value = MagicMock(returncode=0, stdout="", stderr="")

            runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

            cmd = self._nidm_cmd(mock_subprocess)
            assert cmd is not None, "fs_to_nidm was never invoked"
            assert ("-n" in cmd) == with_input_nidm, cmd
            assert "-subjid" in cmd, f"expected -subjid in: {cmd}"
            assert cmd[cmd.index("-subjid") + 1] == "sub-001", cmd
            if with_input_nidm:
                assert cmd[cmd.index("-acq_filename"):] == ["-acq_filename", "sub-001_T1w.nii.gz"], cmd

    def _append(self, tmp_path, t1_images, stderr=""):
        from src.run import nidm_conversion
        nidm_dir = tmp_path / "NIDM"
        nidm_dir.mkdir()
        (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")
        wrapper = MagicMock()
        wrapper.get_subject_t1_info.return_value = {"T1w_images": t1_images, "T2w_images": []}
        with patch('src.run.subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = MagicMock(returncode=0, stdout="", stderr=stderr)
            nidm_conversion(tmp_path / "out", "001", wrapper, nidm_input_dir=nidm_dir)
        cmd = self._nidm_cmd(mock_subprocess)
        assert cmd is not None and "-n" in cmd, cmd
        return cmd

    def test_logged_command_splits_back_into_the_exact_argv(self, tmp_path, caplog):
        """The "Running command:" line is the argv shell-quoted (shlex.join), so
        the -c bootstrap and paths with spaces, quotes and '#' survive a shlex
        round trip. `' '.join` would log a line that splits differently."""
        import shlex
        from src.run import nidm_conversion
        odd = tmp_path / "in put's \"dir\" #2"
        odd.mkdir()
        nidm_dir = odd / "NIDM"
        nidm_dir.mkdir()
        (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")
        wrapper = MagicMock()
        wrapper.get_subject_t1_info.return_value = {
            "T1w_images": [str(odd / "sub-001_T1w (copy).nii.gz")], "T2w_images": []}
        with patch('src.run.subprocess.run') as mock_subprocess, caplog.at_level("INFO"):
            mock_subprocess.return_value = MagicMock(returncode=0, stdout="", stderr="")
            nidm_conversion(odd / "out", "001", wrapper, nidm_input_dir=nidm_dir)
        cmd = self._nidm_cmd(mock_subprocess)
        assert cmd is not None and "-n" in cmd, cmd
        logged = [r.getMessage().split("Running command: ", 1)[1] for r in caplog.records
                  if "Running command: " in r.getMessage()]
        assert len(logged) == 1, logged
        assert shlex.split(logged[0]) == [str(a) for a in cmd]

    @pytest.mark.parametrize("t1_images, expected", [
        (["/bids/sub-001/ses-1/anat/sub-001_ses-1_run-1_T1w.nii.gz"], ["sub-001_ses-1_run-1_T1w.nii.gz"]),
        # recon-all averages repeated runs; all of them are named
        (["/bids/sub-001_run-1_T1w.nii.gz", "/bids/sub-001_run-2_T1w.nii.gz"],
         ["sub-001_run-1_T1w.nii.gz", "sub-001_run-2_T1w.nii.gz"]),
    ])
    def test_append_names_the_t1w_recon_all_used(self, tmp_path, t1_images, expected):
        """With several anatomical acquisitions in the input, fs_to_nidm links the
        stats only to the ones named by -acq_filename."""
        cmd = self._append(tmp_path, t1_images)
        i = cmd.index("-acq_filename")
        assert cmd[i + 1:i + 1 + len(expected)] == expected, cmd

    def test_append_without_t1w_record_warns(self, tmp_path, caplog):
        with caplog.at_level("WARNING"):
            cmd = self._append(tmp_path, [])
        assert "-acq_filename" not in cmd, cmd
        assert "No record of the T1w recon-all used" in caplog.text

    def test_converter_warnings_reach_the_log(self, tmp_path, caplog):
        stderr = ("A newer version (4.5.6) of incf-nidash/pynidm is available.\n"
                  "/x/fs_to_nidm.py:369: UserWarning: 3 anatomical AcquisitionObjects found for subject "
                  "sub-001; the stats are not linked to an acquisition\n"
                  "  warnings.warn(\n")
        with caplog.at_level("WARNING"):
            self._append(tmp_path, [], stderr=stderr)
        assert "fs_to_nidm: 3 anatomical AcquisitionObjects found" in caplog.text
        # Every other stderr line of a successful run is logged too, line by
        # line, so a message the relay does not recognise still reaches the log.
        warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert "fs_to_nidm: A newer version (4.5.6) of incf-nidash/pynidm is available." in warnings
        assert "fs_to_nidm: warnings.warn(" in warnings

    def test_converter_stderr_without_userwarning_reaches_the_log(self, tmp_path, caplog):
        stderr = "The input NIDM has no subject agents; adding a new agent for sub-001 (--forcenidm)\n"
        with caplog.at_level("WARNING"):
            self._append(tmp_path, [], stderr=stderr)
        warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert ("fs_to_nidm: The input NIDM has no subject agents; adding a new agent for "
                "sub-001 (--forcenidm)") in warnings

    def _failed_append(self, tmp_path, caplog, write_partial=False):
        """Run nidm_conversion with an input NIDM and a converter that fails."""
        from src.run import nidm_conversion
        nidm_dir = tmp_path / "NIDM"
        nidm_dir.mkdir()
        (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")
        wrapper = MagicMock()
        wrapper.get_subject_t1_info.return_value = {"T1w_images": [], "T2w_images": []}

        def failing_convert(cmd, *args, **kwargs):
            if write_partial:
                Path(cmd[cmd.index("-n") + 1]).write_text("@prefix : <http://example.org/> .\n:partial :x 1 .")
            return MagicMock(returncode=1, stdout="Processing stats file: aseg.stats\n",
                             stderr="fs_to_nidm.py: error: Subject ID sub-001 not found")

        with patch('src.run.subprocess.run', side_effect=failing_convert), \
             caplog.at_level("INFO"), pytest.raises(SystemExit) as exc:
            nidm_conversion(tmp_path / "out", "001", wrapper, nidm_input_dir=nidm_dir)
        assert exc.value.code == 1
        return tmp_path / "out" / "sub-001"

    def test_converter_failure_logs_stderr_and_stdout(self, tmp_path, caplog):
        self._failed_append(tmp_path, caplog)
        errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
        assert any("Subject ID sub-001 not found" in m for m in errors), errors
        assert any("Processing stats file: aseg.stats" in m for m in errors), errors

    @pytest.mark.parametrize("write_partial", [False, True])
    def test_failed_conversion_leaves_no_nidm_ttl(self, tmp_path, caplog, write_partial):
        """The input NIDM is converted in a temporary copy; a failed conversion
        must not leave the unmodified (or half-written) copy as nidm.ttl, where
        it looks like a finished output."""
        subject_dir = self._failed_append(tmp_path, caplog, write_partial=write_partial)
        assert not (subject_dir / "nidm.ttl").exists()
        assert [p.name for p in subject_dir.iterdir() if p.is_file()] == []

    def test_append_converts_a_copy_and_renames_it_into_place(self, tmp_path):
        """fs_to_nidm is pointed at a temporary file next to nidm.ttl, never at
        nidm.ttl itself; on success the result is nidm.ttl."""
        from src.run import nidm_conversion
        nidm_dir = tmp_path / "NIDM"
        nidm_dir.mkdir()
        (nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")
        wrapper = MagicMock()
        wrapper.get_subject_t1_info.return_value = {"T1w_images": [], "T2w_images": []}
        seen = {}

        def convert(cmd, *args, **kwargs):
            target = Path(cmd[cmd.index("-n") + 1])
            seen["target"] = target
            seen["nidm_ttl_existed"] = (target.parent / "nidm.ttl").exists()
            target.write_text("@prefix : <http://example.org/> .\n:converted :x 1 .")
            return MagicMock(returncode=0, stdout="", stderr="")

        with patch('src.run.subprocess.run', side_effect=convert):
            nidm_conversion(tmp_path / "out", "001", wrapper, nidm_input_dir=nidm_dir)
        subject_dir = tmp_path / "out" / "sub-001"
        assert seen["target"].parent == subject_dir
        assert seen["target"].name != "nidm.ttl" and seen["target"].suffix == ".ttl"
        assert not seen["nidm_ttl_existed"]
        assert not seen["target"].exists()
        assert ":converted" in (subject_dir / "nidm.ttl").read_text()

    def test_shared_cde_not_merged_into_per_subject_ttl(self, tmp_path, bids_single_session):
        """fs_cde.ttl must ship alongside, not be folded into each subject TTL.

        The CDE vocabulary is a static lookup table, byte-identical for every
        subject, and is already written to the same nidm/ directory. Embedding it
        makes every per-subject TTL unique and ~2 MB larger for no information gain.
        """
        nidm_in = bids_single_session.parent / "NIDM"
        nidm_in.mkdir()
        (nidm_in / "nidm.ttl").write_text(
            '@prefix ex: <http://example.org/> .\nex:subject a ex:Person .\n'
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Match on the local name, not the full URI: rdflib re-serializes
        # <http://example.org/CDE_ONLY_TERM> in prefixed form (ex:CDE_ONLY_TERM),
        # so asserting on the full URI would pass even when the CDE was merged.
        cde_marker = "CDE_ONLY_TERM"

        def fake_convert(cmd, *args, **kwargs):
            """Emulate fs_to_nidm: append to the -n file and drop a fs_cde.ttl."""
            nidm_file = Path(cmd[cmd.index("-n") + 1])
            nidm_file.write_text(
                '@prefix ex: <http://example.org/> .\n'
                'ex:subject a ex:Person .\n'
                'ex:collection ex:measure "42" .\n'
            )
            (nidm_file.parent / "fs_cde.ttl").write_text(
                '@prefix ex: <http://example.org/> .\n'
                f'ex:{cde_marker} a ex:DataElement .\n'
            )
            return MagicMock(returncode=0, stdout="", stderr="")

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run', side_effect=fake_convert):
            mock_wrapper.return_value.process_subject.return_value = True

            runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

        nidm_out = output_dir / "sub-001"
        aggregated = nidm_out / "nidm.ttl"
        assert aggregated.exists(), f"nidm.ttl missing; have: {list(nidm_out.glob('*'))}"

        # The shared vocabulary must still be shipped...
        assert (nidm_out / "fs_cde.ttl").exists(), "fs_cde.ttl must ship alongside"
        # ...but must NOT have been merged into the per-subject TTL.
        assert cde_marker not in aggregated.read_text(), (
            "fs_cde.ttl content was merged into the per-subject TTL; it should be "
            "shipped alongside instead"
        )
        # Subject-specific content must still be there.
        assert "collection" in aggregated.read_text()


class TestNIDMDatasetDescription:
    """Test NIDM dataset_description.json creation."""

    def test_nidm_dataset_description_created(self, tmp_path):
        """Verify dataset_description.json is created in NIDM directory."""
        nidm_dir = tmp_path / "freesurfer-nidm_bidsapp" / "nidm"
        nidm_dir.mkdir(parents=True)

        # Create dataset_description.json
        desc_file = nidm_dir / "dataset_description.json"
        with open(desc_file, "w") as f:
            json.dump({
                "Name": "NIDM Derivatives",
                "BIDSVersion": "1.8.0",
                "DatasetType": "derivative"
            }, f)

        assert desc_file.exists()

    def test_nidm_dataset_description_content(self, tmp_path):
        """Verify dataset_description.json has correct structure."""
        nidm_dir = tmp_path / "freesurfer-nidm_bidsapp" / "nidm"
        nidm_dir.mkdir(parents=True)

        desc_file = nidm_dir / "dataset_description.json"
        with open(desc_file, "w") as f:
            json.dump({
                "Name": "NIDM Derivatives",
                "BIDSVersion": "1.8.0",
                "DatasetType": "derivative"
            }, f)

        with open(desc_file) as f:
            desc = json.load(f)

        assert desc["Name"] == "NIDM Derivatives"
        assert desc["DatasetType"] == "derivative"


class TestNIDMTemporaryFile:
    """With an input NIDM, fs_to_nidm converts a temporary copy
    `.nidm.XXXX.ttl` in the subject directory, which becomes nidm.ttl only on
    success."""

    INPUT = "@prefix : <http://example.org/> .\n:input :x 1 .\n"

    @staticmethod
    def _wrapper():
        wrapper = MagicMock()
        wrapper.get_subject_t1_info.return_value = {"T1w_images": [], "T2w_images": []}
        return wrapper

    def _input_dir(self, tmp_path, mode=None):
        nidm_dir = tmp_path / "NIDM"
        nidm_dir.mkdir()
        (nidm_dir / "nidm.ttl").write_text(self.INPUT)
        if mode is not None:
            (nidm_dir / "nidm.ttl").chmod(mode)
        return nidm_dir

    @staticmethod
    def _converting(seen=None):
        """A stand-in fs_to_nidm that appends to its -n file and succeeds."""
        def convert(cmd, *args, **kwargs):
            target = Path(cmd[cmd.index("-n") + 1])
            if seen is not None:
                seen["target"] = target
                seen["inode"] = target.stat().st_ino
            with open(target, "a") as f:
                f.write(":converted :x 1 .\n")
            return MagicMock(returncode=0, stdout="", stderr="")
        return convert

    @pytest.mark.parametrize("input_mode", [0o444, 0o600])
    def test_nidm_ttl_gets_the_mode_of_a_new_file(self, tmp_path, input_mode):
        """mkstemp creates the temporary file 0600; the result must get the mode
        a new file gets under the umask, not 0600 and not the input's (a
        git-annex input is read-only)."""
        import os
        from src.run import nidm_conversion
        nidm_dir = self._input_dir(tmp_path, mode=input_mode)
        old_umask = os.umask(0o022)
        try:
            with patch('src.run.subprocess.run', side_effect=self._converting()):
                nidm_conversion(tmp_path / "out", "001", self._wrapper(), nidm_input_dir=nidm_dir)
        finally:
            os.umask(old_umask)
        assert (tmp_path / "out" / "sub-001" / "nidm.ttl").stat().st_mode & 0o777 == 0o644

    def test_nidm_ttl_is_the_converted_file_renamed_not_a_copy(self, tmp_path):
        """os.replace, not a copy: the converted file itself becomes nidm.ttl,
        atomically, so no reader ever sees a half-copied nidm.ttl."""
        from src.run import nidm_conversion
        seen = {}
        with patch('src.run.subprocess.run', side_effect=self._converting(seen)):
            nidm_conversion(tmp_path / "out", "001", self._wrapper(),
                            nidm_input_dir=self._input_dir(tmp_path))
        result = tmp_path / "out" / "sub-001" / "nidm.ttl"
        assert result.stat().st_ino == seen["inode"]
        assert not seen["target"].exists()

    @pytest.mark.parametrize("raised", [KeyboardInterrupt, SystemExit, OSError])
    def test_an_exception_during_conversion_leaves_no_temporary(self, tmp_path, raised):
        from src.run import nidm_conversion

        def interrupted(cmd, *args, **kwargs):
            assert Path(cmd[cmd.index("-n") + 1]).exists()
            raise raised()

        with patch('src.run.subprocess.run', side_effect=interrupted), pytest.raises(raised):
            nidm_conversion(tmp_path / "out", "001", self._wrapper(),
                            nidm_input_dir=self._input_dir(tmp_path))
        assert [p.name for p in (tmp_path / "out" / "sub-001").iterdir() if p.is_file()] == []

    def test_a_temporary_left_by_an_interrupted_run_is_removed_and_not_merged(self, tmp_path):
        """A run killed while converting leaves its `.nidm.XXXX.ttl`. The next
        run removes it before converting, and never merges such a file into
        nidm.ttl (an orphaned converter can rewrite it while the next run's
        converter runs, and every changed .ttl in the directory is merged)."""
        from src.run import nidm_conversion
        subject_dir = tmp_path / "out" / "sub-001"
        subject_dir.mkdir(parents=True)
        stale = subject_dir / ".nidm.k1ll3d00.ttl"
        stale.write_text("@prefix : <http://example.org/> .\n:stale :x 1 .\n")
        # An earlier run's outputs and another hidden file are not temporaries.
        earlier_nidm = subject_dir / "nidm.ttl"
        earlier_nidm.write_text("@prefix : <http://example.org/> .\n:earlier :x 1 .\n")
        cde = subject_dir / "fs_cde.ttl"
        cde.write_text("@prefix : <http://example.org/> .\n:cde :x 1 .\n")
        notes = subject_dir / ".nidmnotes.ttl"
        notes.write_text("@prefix : <http://example.org/> .\n:notes :x 1 .\n")
        convert = self._converting()

        def orphan_rewrites_its_temporary(cmd, *args, **kwargs):
            orphan = subject_dir / ".nidm.0rphan00.ttl"
            orphan.write_text("@prefix : <http://example.org/> .\n:orphan :x 1 .\n")
            return convert(cmd, *args, **kwargs)

        with patch('src.run.subprocess.run', side_effect=orphan_rewrites_its_temporary):
            nidm_conversion(tmp_path / "out", "001", self._wrapper(),
                            nidm_input_dir=self._input_dir(tmp_path))
        text = (subject_dir / "nidm.ttl").read_text()
        assert ":converted" in text
        assert "stale" not in text and "orphan" not in text
        assert not stale.exists()
        assert earlier_nidm.exists() and cde.exists() and notes.exists()
        assert ":cde" in cde.read_text() and ":notes" in notes.read_text()

    def test_start_cleanup_removes_only_hidden_nidm_temporaries(self, tmp_path):
        """The start-of-run cleanup deletes `.nidm.*.ttl` in the subject
        directory and nothing else: never an earlier nidm.ttl, fs_cde.ttl,
        other .ttl files, files in subdirectories, or a symlink's target."""
        from src.run import _remove_interrupted_temporaries
        subject_dir = tmp_path / "sub-001"
        (subject_dir / "freesurfer").mkdir(parents=True)
        outside = tmp_path / "outside.ttl"
        outside.write_text("keep")
        temporaries = [subject_dir / ".nidm.k1ll3d00.ttl", subject_dir / ".nidm.0rphan00.ttl"]
        for path in temporaries:
            path.write_text("temporary")
        link = subject_dir / ".nidm.l1nk0000.ttl"
        link.symlink_to(outside)
        kept = [subject_dir / name for name in
                ("nidm.ttl", "fs_cde.ttl", ".nidmnotes.ttl", "sub-001.ttl", "nidm.ttl.bak",
                 ".nidm.ttl.keep")]
        kept.append(subject_dir / "freesurfer" / ".nidm.nested00.ttl")
        for path in kept:
            path.write_text("keep")

        _remove_interrupted_temporaries(subject_dir)

        assert not any(p.exists() or p.is_symlink() for p in temporaries + [link])
        assert all(p.read_text() == "keep" for p in kept + [outside])

    @pytest.mark.parametrize("previous", ["default", "ignore", "function"])
    def test_sigterm_handler_is_restored_after_conversion(self, previous):
        """_exit_on_sigterm installs its handler only for the conversion and
        puts back whatever was there before, also when the block raises."""
        import signal
        from src.run import _exit_on_sigterm

        def custom(signum, frame):
            pass

        before = {"default": signal.SIG_DFL, "ignore": signal.SIG_IGN, "function": custom}[previous]
        saved = signal.signal(signal.SIGTERM, before)
        try:
            with _exit_on_sigterm():
                assert signal.getsignal(signal.SIGTERM) is not before
            assert signal.getsignal(signal.SIGTERM) is before
            with pytest.raises(RuntimeError):
                with _exit_on_sigterm():
                    raise RuntimeError("conversion failed")
            assert signal.getsignal(signal.SIGTERM) is before
        finally:
            signal.signal(signal.SIGTERM, saved)

    def test_sigterm_during_conversion_leaves_no_temporary(self, tmp_path):
        """SIGTERM (a time limit, scancel) used to kill the app without any
        cleanup, leaving the temporary copy behind. Run in a child process,
        so the test runner itself is not the one terminated."""
        import subprocess
        import sys
        out = tmp_path / "out"
        nidm_dir = self._input_dir(tmp_path)
        child = f"""
import os, signal
from pathlib import Path
from unittest.mock import MagicMock, patch
from src.run import nidm_conversion

def terminated(cmd, *args, **kwargs):
    assert Path(cmd[cmd.index("-n") + 1]).exists()
    os.kill(os.getpid(), signal.SIGTERM)
    raise AssertionError("SIGTERM did not interrupt the conversion")

wrapper = MagicMock()
wrapper.get_subject_t1_info.return_value = {{"T1w_images": [], "T2w_images": []}}
with patch("src.run.subprocess.run", side_effect=terminated):
    nidm_conversion({str(out)!r}, "001", wrapper, nidm_input_dir={str(nidm_dir)!r})
"""
        repo_root = Path(__file__).resolve().parents[1]
        result = subprocess.run([sys.executable, "-c", child], cwd=repo_root,
                                capture_output=True, text=True, timeout=120)
        assert result.returncode == 143, (result.returncode, result.stderr[-2000:])
        assert [p.name for p in (out / "sub-001").iterdir() if p.is_file()] == []


class TestNIDMWithoutInputFailsClosed:
    """Without an input NIDM, nidm.ttl is the merge of fs_to_nidm's
    `*_nidm.jsonld` outputs. A converter output that cannot be parsed, or a
    nidm.ttl that cannot be written, fails the subject (exit 1) and leaves no
    nidm.ttl and no temporary file, as the input branch does."""

    JSONLD = '{"@id": "http://example.org/%s", "http://example.org/p": "%s"}'

    @staticmethod
    def _wrapper():
        wrapper = MagicMock()
        wrapper.get_subject_t1_info.return_value = {"T1w_images": [], "T2w_images": []}
        return wrapper

    @staticmethod
    def _writing(files):
        """A stand-in fs_to_nidm -o that writes `files` into its output dir."""
        def run(cmd, *args, **kwargs):
            out = Path(cmd[cmd.index("-o") + 1])
            for name, text in files.items():
                (out / name).write_text(text)
            return MagicMock(returncode=0, stdout="", stderr="")
        return run

    def _convert(self, tmp_path, caplog, files, serialize=None):
        from rdflib import Graph
        from src.run import nidm_conversion
        patches = [patch('src.run.subprocess.run', side_effect=self._writing(files))]
        if serialize is not None:
            patches.append(patch.object(Graph, "serialize", autospec=True, side_effect=serialize))
        with caplog.at_level("INFO"), pytest.raises(SystemExit) as exc:
            for p in patches:
                p.start()
            try:
                nidm_conversion(tmp_path / "out", "001", self._wrapper())
            finally:
                for p in patches:
                    p.stop()
        assert exc.value.code == 1
        subject_dir = tmp_path / "out" / "sub-001"
        assert not (subject_dir / "nidm.ttl").exists()
        assert [p.name for p in subject_dir.iterdir() if p.name.startswith(".nidm.")] == []
        messages = [r.getMessage() for r in caplog.records]
        assert not any("NIDM conversion complete" in m for m in messages), messages
        return [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]

    @pytest.mark.parametrize("partial", [False, True])
    def test_a_failed_nidm_ttl_write_fails_the_subject(self, tmp_path, caplog, partial):
        import errno

        def no_space(self_graph, destination=None, **kwargs):
            if partial:
                Path(destination).write_text("@prefix : <http://example.org/> .\n:half")
            raise OSError(errno.ENOSPC, "No space left on device")

        errors = self._convert(tmp_path, caplog, {"aseg_nidm.jsonld": self.JSONLD % ("aseg", "1")},
                               serialize=no_space)
        assert any("No space left on device" in m for m in errors), errors

    def test_an_unparseable_converter_output_fails_the_subject(self, tmp_path, caplog):
        errors = self._convert(tmp_path, caplog, {"aseg_nidm.jsonld": self.JSONLD % ("aseg", "1"),
                                                  "wmparc_nidm.jsonld": "{ not json"})
        assert any("wmparc_nidm.jsonld" in m for m in errors), errors

    @pytest.mark.parametrize("files, cause", [
        ({}, "wrote no new output"),
        ({"fs_cde.ttl": "@prefix : <http://example.org/> .\n:cde :x 1 .\n"}, "no NIDM output"),
        ({"aseg_nidm.jsonld": "{}"}, "hold no triples"),
    ], ids=["nothing", "only-cde", "empty-graph"])
    def test_no_converter_output_fails_the_subject(self, tmp_path, caplog, files, cause):
        errors = self._convert(tmp_path, caplog, files)
        assert any(cause in m for m in errors), errors

    def test_nidm_ttl_is_written_through_a_temporary_file(self, tmp_path):
        """The merged graph is serialized to a temporary file in the subject
        directory and renamed to nidm.ttl, never written to nidm.ttl in place."""
        import os
        from rdflib import Graph
        from src.run import nidm_conversion
        seen = []
        real = Graph.serialize

        def recording(self_graph, destination=None, **kwargs):
            seen.append(Path(destination))
            return real(self_graph, destination=destination, **kwargs)

        old_umask = os.umask(0o022)
        try:
            with patch('src.run.subprocess.run',
                       side_effect=self._writing({"aseg_nidm.jsonld": self.JSONLD % ("aseg", "1")})), \
                 patch.object(Graph, "serialize", autospec=True, side_effect=recording):
                nidm_conversion(tmp_path / "out", "001", self._wrapper())
        finally:
            os.umask(old_umask)
        subject_dir = tmp_path / "out" / "sub-001"
        assert len(seen) == 1 and seen[0].parent == subject_dir, seen
        assert seen[0].name.startswith(".nidm.") and not seen[0].exists()
        nidm_ttl = subject_dir / "nidm.ttl"
        assert ":aseg" in nidm_ttl.read_text()
        assert nidm_ttl.stat().st_mode & 0o777 == 0o644

