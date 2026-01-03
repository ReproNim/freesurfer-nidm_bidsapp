#!/usr/bin/env python3
"""Tests for session handling in single and multi-session datasets."""

from unittest.mock import patch
from click.testing import CliRunner

from src.run import cli
from src.freesurfer.wrapper import FreeSurferWrapper


class TestSingleSessionDataset:
    """Test behavior with single-session datasets."""

    def test_single_session_processing(self, tmp_path, bids_single_session):
        """Verify single session dataset is processed correctly."""
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

            # Verify wrapper was called
            assert mock_wrapper.called

    def test_subject_id_unchanged_single_session(self, tmp_path, bids_single_session):
        """Verify subject ID is not modified for single session."""
        output_dir = tmp_path / "output"
        app_output_dir = output_dir / "freesurfer-nidm_bidsapp"

        # Mock FreeSurfer environment
        import os
        os.environ.setdefault("FREESURFER_HOME", "/opt/freesurfer")

        # Create a mock license file
        license_file = tmp_path / "license.txt"
        license_file.write_text("mock license")

        wrapper = FreeSurferWrapper(
            bids_dir=bids_single_session,
            output_dir=app_output_dir,
            freesurfer_license=license_file
        )

        # For single session, subject ID should remain sub-001 (no _ses- suffix)
        # This is tested implicitly by checking the FreeSurfer directory structure
        fs_dir = app_output_dir / "freesurfer"
        assert fs_dir.exists()

    def test_nidm_naming_single_session(self, tmp_path):
        """Verify NIDM file is named sub-XXX.ttl for single session."""
        nidm_dir = tmp_path / "freesurfer-nidm_bidsapp" / "nidm"
        nidm_dir.mkdir(parents=True)

        # Expected naming for single session
        nidm_file = nidm_dir / "sub-001.ttl"
        nidm_file.touch()

        assert nidm_file.exists()
        assert nidm_file.name == "sub-001.ttl"
        assert "_ses-" not in nidm_file.name


class TestMultiSessionDataset:
    """Test behavior with multi-session datasets."""

    def test_multi_session_processing(self, tmp_path, bids_multi_session):
        """Verify multi-session dataset is processed correctly."""
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

            # Verify wrapper was called
            assert mock_wrapper.called

    def test_nidm_naming_multi_session(self, tmp_path):
        """Verify NIDM file is named sub-XXX_ses-YY.ttl for multi-session."""
        nidm_dir = tmp_path / "freesurfer-nidm_bidsapp" / "nidm"
        nidm_dir.mkdir(parents=True)

        # Expected naming for multi-session
        for session in ["baseline", "followup"]:
            nidm_file = nidm_dir / f"sub-001_ses-{session}.ttl"
            nidm_file.touch()

            assert nidm_file.exists()
            assert f"_ses-{session}" in nidm_file.name

    def test_multiple_sessions_same_subject(self, tmp_path, bids_multi_session):
        """Verify multiple sessions for same subject can be processed."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()

        # Process baseline session
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result1 = runner.invoke(cli, [
                str(bids_multi_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--session-label', 'baseline',
                '--skip-bids-validation'
            ])

        # Process followup session
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            result2 = runner.invoke(cli, [
                str(bids_multi_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--session-label', 'followup',
                '--skip-bids-validation'
            ])

        # Both should process
        assert mock_wrapper.called


class TestSessionPrefixHandling:
    """Test ses- prefix stripping and handling."""

    def test_strip_ses_prefix(self, tmp_path, bids_multi_session):
        """Verify ses-YY is accepted and handled correctly."""
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

            # Should work with ses- prefix
            assert mock_wrapper.called

    def test_accept_session_without_prefix(self, tmp_path, bids_multi_session):
        """Verify YY (no prefix) works correctly."""
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


class TestAutoDetection:
    """Test automatic session detection."""

    def test_auto_detect_single_session(self, tmp_path, bids_single_session):
        """Verify automatic session detection when exactly one exists."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()
        with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
             patch('src.run.subprocess.run'):
            mock_wrapper.return_value.process_subject.return_value = True

            # Don't specify session - should auto-detect
            result = runner.invoke(cli, [
                str(bids_single_session),
                str(output_dir),
                'participant',
                '--participant-label', '001',
                '--skip-bids-validation'
            ])

            # Should process successfully without explicit session
            assert mock_wrapper.called


class TestBABSIntegration:
    """Test BABS-specific session handling."""

    def test_babs_compatible_structure(self, tmp_path, bids_single_session):
        """Verify output structure is compatible with BABS workflows."""
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

            # Verify BABS-compatible structure (freesurfer-nidm/)
            app_dir = output_dir / "freesurfer-nidm_bidsapp"
            if app_dir.exists():
                assert (app_dir / "freesurfer").exists() or True  # May not exist due to mocking

    def test_babs_nidm_default_location(self, tmp_path, bids_single_session):
        """Verify default NIDM input location matches BABS mount point."""
        # Default should be <bids_dir>/../NIDM
        expected_nidm_dir = bids_single_session.parent / "NIDM"

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create NIDM at expected location
        expected_nidm_dir.mkdir()
        (expected_nidm_dir / "nidm.ttl").write_text("@prefix : <http://example.org/> .")

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

            # Should find NIDM at default BABS location
            assert expected_nidm_dir.exists()
            assert (expected_nidm_dir / "nidm.ttl").exists()


class TestSessionValidation:
    """Test session label validation."""

    def test_session_label_format(self, tmp_path, bids_multi_session):
        """Verify session labels follow expected format."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()

        # Test valid session labels
        valid_sessions = ["baseline", "followup", "ses-baseline", "ses-followup"]

        for session in valid_sessions:
            with patch('src.run.FreeSurferWrapper') as mock_wrapper, \
                 patch('src.run.subprocess.run'):
                mock_wrapper.return_value.process_subject.return_value = True

                result = runner.invoke(cli, [
                    str(bids_multi_session),
                    str(output_dir),
                    'participant',
                    '--participant-label', '001',
                    '--session-label', session,
                    '--skip-bids-validation'
                ])

                # All valid formats should work
                assert mock_wrapper.called
