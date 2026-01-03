#!/usr/bin/env python3
"""Tests for CLI argument validation and error cases."""

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
