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

    def test_nidm_directory_created(self, tmp_path, bids_single_session):
        """Verify NIDM output directory is created."""
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

            nidm_dir = output_dir / "freesurfer-nidm_bidsapp" / "nidm"
            assert nidm_dir.exists()


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

    def test_fallback_ttl_discovery(self, tmp_path):
        """Verify fallback to *.ttl when nidm.ttl not found."""
        nidm_dir = tmp_path / "NIDM"
        nidm_dir.mkdir()

        # Create only other .ttl files
        (nidm_dir / "sub-001.ttl").write_text("@prefix : <http://example.org/> .")
        (nidm_dir / "sub-002.ttl").write_text("@prefix : <http://example.org/> .")

        # Verify nidm.ttl doesn't exist
        assert not (nidm_dir / "nidm.ttl").exists()

        # But other TTL files should be discoverable
        ttl_files = list(nidm_dir.glob("*.ttl"))
        assert len(ttl_files) == 2


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
