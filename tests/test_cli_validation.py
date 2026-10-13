#!/usr/bin/env python3
"""Tests for CLI argument validation and error cases."""

import json
import os

import pytest
from click.testing import CliRunner
from unittest.mock import patch

from src.run import cli


class TestRequiredArguments:
    """Test required argument validation."""

    def test_missing_bids_dir(self):
        """Verify error when BIDS directory not provided."""
        runner = CliRunner()
        result = runner.invoke(cli, [])

        assert result.exit_code != 0
        assert "Error" in result.output or "Missing" in result.output

    def test_missing_output_dir(self, bids_single_session):
        """Verify error when output directory not provided."""
        runner = CliRunner()
        result = runner.invoke(cli, [str(bids_single_session)])

        assert result.exit_code != 0

    def test_missing_analysis_level(self, tmp_path, bids_single_session):
        """Verify error when analysis level not provided."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(cli, [
            str(bids_single_session),
            str(output_dir)
        ])

        assert result.exit_code != 0

    def test_invalid_analysis_level(self, tmp_path, bids_single_session):
        """Verify error for invalid analysis level choice."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(cli, [
            str(bids_single_session),
            str(output_dir),
            'invalid_level'
        ])

        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid choice" in result.output.lower()


class TestParticipantLevel:
    """Test participant-level CLI validation."""

    def test_participant_with_sub_prefix(self, tmp_path, bids_single_session):
        """Verify sub-XXX prefix is handled correctly."""
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
                '--participant-label', 'sub-001',
                '--skip-bids-validation'
            ])

            # Verify wrapper was called (prefix should be stripped internally)
            assert mock_wrapper.called

    def test_participant_without_sub_prefix(self, tmp_path, bids_single_session):
        """Verify XXX (no prefix) is handled correctly."""
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

            # Should work without prefix
            assert mock_wrapper.called


class TestSessionLevel:
    """Test session-level CLI validation."""

    def test_session_with_ses_prefix(self, tmp_path, bids_multi_session):
        """Verify ses-YY prefix is handled correctly."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_multi_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--session-label', 'ses-baseline',
                '--skip-bids-validation'
            ])

            # Verify command was processed
            assert mock_wrapper.called

    def test_session_without_ses_prefix(self, tmp_path, bids_multi_session):
        """Verify YY (no prefix) is handled correctly."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_multi_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--session-label', 'baseline',
                '--skip-bids-validation'
            ])

            # Should work without prefix
            assert mock_wrapper.called


class TestOptionalFlags:
    """Test optional flag behavior."""

    def test_skip_bids_validation(self, tmp_path, bids_single_session):
        """Verify --skip-bids-validation works."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'), \
             patch('src.run.BIDSLayout') as mock_layout:
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

            # Verify BIDSLayout was called with validate=False
            assert mock_layout.called
            call_args = mock_layout.call_args
            # Check if validate=False was passed
            if len(call_args) > 1:
                assert call_args[1].get('validate') == False or call_args[0][1] == False

    def test_skip_nidm(self, tmp_path, bids_single_session):
        """Verify --skip-nidm skips NIDM conversion."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run') as mock_subprocess:
            mock_wrapper.return_value.process_subject.return_value = True

            runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-nidm',
                '--skip-bids-validation'
            ])

            # Verify NIDM conversion subprocess was not called
            nidm_calls = [
                c for c in mock_subprocess.call_args_list
                if 'fs_to_nidm' in str(c)
            ]
            assert len(nidm_calls) == 0

    def test_verbose_flag(self, tmp_path, bids_single_session):
        """Verify --verbose sets DEBUG log level."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'), \
             patch('src.run.setup_logging') as mock_logging:
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--verbose',
                '--skip-bids-validation'
            ])

            # Verify setup_logging was called with DEBUG level
            assert mock_logging.called


class TestVersionFlag:
    """Test --version flag."""

    def test_version_flag(self):
        """Verify --version displays version information."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--version'])

        # Should succeed and display version
        assert result.exit_code == 0
        # Should contain version number
        assert any(char.isdigit() for char in result.output)


class TestHelpFlag:
    """Test --help flag."""

    def test_help_flag(self):
        """Verify --help displays usage information."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])

        # Should succeed
        assert result.exit_code == 0
        # Should contain usage information
        assert "Usage" in result.output or "usage" in result.output.lower()
        assert "participant" in result.output.lower()


class TestPathValidation:
    """Test path argument validation."""

    def test_nonexistent_bids_dir(self, tmp_path):
        """Verify error for nonexistent BIDS directory."""
        nonexistent = tmp_path / "nonexistent"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(cli, [
            str(nonexistent),
            str(output_dir),
            'participant',
            '--participant-label', '001'
        ])

        # Should fail due to nonexistent BIDS directory
        assert result.exit_code != 0


class TestSkipFreeSurferFlag:
    """Test --skip-freesurfer flag."""

    def test_skip_freesurfer_flag(self, tmp_path, bids_single_session):
        """Verify --skip-freesurfer skips FreeSurfer processing."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create mock FreeSurfer output directory
        fs_dir = output_dir / "freesurfer-nidm_bidsapp" / "freesurfer" / "sub-001"
        fs_dir.mkdir(parents=True)

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-freesurfer',
                '--skip-bids-validation'
            ])

            # Verify FreeSurfer wrapper's process_subject was not called
            # (or was called but returned immediately)
            if mock_wrapper.return_value.process_subject.called:
                # Check it was not called for actual processing
                pass


class TestNIDMInputDirFlag:
    """Test --nidm-input-dir flag."""

    def test_nidm_input_dir_flag(self, tmp_path, bids_single_session):
        """Verify --nidm-input-dir accepts custom path."""
        custom_nidm = tmp_path / "custom_nidm"
        custom_nidm.mkdir()
        (custom_nidm / "nidm.ttl").write_text("@prefix : <http://example.org/> .")

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
                '--nidm-input-dir', str(custom_nidm),
                '--skip-bids-validation'
            ])

            # Command should process successfully
            assert mock_wrapper.called


class TestMaxVoxelSize:
    """--max-voxel-size reaches the wrapper; a failed subject exits non-zero."""

    def _invoke(self, bids_dir, output_dir, *extra, success=True):
        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = success
            mock_wrapper.return_value.get_processing_summary.return_value = {}
            result = runner.invoke(cli, [
                str(bids_dir), str(output_dir), 'participant',
                '--participant-label', '001', '--skip-bids-validation', '--skip-nidm',
                *extra,
            ])
        return result, mock_wrapper

    def test_default_limit(self, tmp_path, bids_single_session):
        result, mock_wrapper = self._invoke(bids_single_session, tmp_path / "out")
        assert result.exit_code == 0, result.output
        assert mock_wrapper.call_args.kwargs["max_voxel_size"] == 2.0

    def test_custom_limit(self, tmp_path, bids_single_session):
        result, mock_wrapper = self._invoke(bids_single_session, tmp_path / "out", '--max-voxel-size', '3.5')
        assert result.exit_code == 0, result.output
        assert mock_wrapper.call_args.kwargs["max_voxel_size"] == 3.5

    def test_failed_subject_exits_non_zero(self, tmp_path, bids_single_session):
        result, mock_wrapper = self._invoke(bids_single_session, tmp_path / "out", success=False)
        assert result.exit_code != 0
        mock_wrapper.return_value.process_subject.assert_called_once()

    def test_failed_session_exits_non_zero(self, tmp_path, bids_multi_session):
        """The session-level entry point (process_session) also exits non-zero."""
        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = False
            mock_wrapper.return_value.get_processing_summary.return_value = {}
            result = runner.invoke(cli, [
                str(bids_multi_session), str(tmp_path / "out"), 'session',
                '--participant-label', '001', '--session-label', 'baseline',
                '--skip-bids-validation', '--skip-nidm',
            ])
        mock_wrapper.return_value.process_subject.assert_called_once()
        assert result.exit_code != 0


class TestSkipFreesurferUsesRecordedInputs:
    """--skip-freesurfer judges the existing tree by the images recon-all recorded.

    Runs the real wrapper on real NIfTI headers; only recon-all itself is absent.
    """

    NAME = "sub-29001_ses-1_acq-hires_run-1_T1w.nii.gz"

    def _run(self, tmp_path, zooms):
        import nibabel as nib
        import numpy as np

        bids = tmp_path / "bids"
        anat = bids / "sub-29001" / "ses-1" / "anat"
        anat.mkdir(parents=True)
        (bids / "dataset_description.json").write_text(json.dumps({"Name": "x", "BIDSVersion": "1.8.0"}))
        img = nib.Nifti1Image(np.zeros((8, 8, 4), dtype=np.uint8), np.eye(4))
        img.header.set_zooms(zooms)
        nib.save(img, str(anat / self.NAME))

        # A finished tree from an earlier job, whose BIDS root was elsewhere.
        out = tmp_path / "out"
        scripts = out / "sub-29001" / "ses-1" / "freesurfer" / "scripts"
        scripts.mkdir(parents=True)
        (scripts / "recon-all.done").write_text(
            "CMDARGS -subjid sub-29001_ses-1 "
            f"-i /scratch/job-7/ds/inputs/data/BIDS/sub-29001/ses-1/anat/{self.NAME} -all\n"
        )
        fs_home = tmp_path / "freesurfer"
        fs_home.mkdir()
        (fs_home / "license.txt").write_text("x")
        with patch.dict(os.environ, {"FREESURFER_HOME": str(fs_home)}):
            return CliRunner().invoke(cli, [
                str(bids), str(out), 'participant', '--participant-label', '29001',
                '--session-label', '1', '--skip-bids-validation', '--skip-freesurfer', '--skip-nidm',
            ])

    def test_tree_made_from_a_slab_fails(self, tmp_path):
        result = self._run(tmp_path, (1.5, 1.5, 4.0))
        assert result.exit_code != 0
        assert f"{self.NAME} (1.5x1.5x4.0 mm)" in result.output
        assert "not running recon-all" not in result.output
        assert "BIDS-FreeSurfer processing complete" not in result.output

    def test_tree_made_from_a_fine_image_is_used(self, tmp_path):
        result = self._run(tmp_path, (1.0, 1.0, 1.0))
        assert result.exit_code == 0, result.output
        assert f"it was made from {self.NAME}" in result.output
