#!/usr/bin/env python3
"""nidm_conversion with the real fs_to_nidm, not a mocked subprocess.

The other NIDM tests check the command line the app builds. These check what
the converter then writes: which subject the FreeSurfer measures attach to
(#21), which T1w they are linked to, and that a re-run replaces the output.
They need the src/segstats_jsonld submodule checked out.
"""

import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import XSD

from src.run import CONVERTER_BOOTSTRAP, CONVERTER_MODULE, nidm_conversion

REPO_ROOT = Path(__file__).resolve().parents[1]
ASEG_STATS = REPO_ROOT / "src" / "segstats_jsonld" / "segstats_jsonld" / "tests" / "testdata" / "aseg.stats"

PROV = Namespace("http://www.w3.org/ns/prov#")
NDAR_ID = URIRef("https://ndar.nih.gov/api/datadictionary/v2/dataelement/src_subject_id")
SIO_SUBJECT = URIRef("http://semanticscience.org/ontology/sio.owl#Subject")
SUBJECT = URIRef("http://iri.nidash.org/subj")
ACQ_RUN1 = URIRef("http://iri.nidash.org/acq-run-1")
ACQ_HIRES = URIRef("http://iri.nidash.org/acq-hires")

# The input subject has two anatomical acquisitions; recon-all used run-1.
INPUT_TTL = f"""
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix ndar: <https://ndar.nih.gov/api/datadictionary/v2/dataelement/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix nidm: <http://purl.org/nidash/nidm#> .
@prefix nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#> .
<http://iri.nidash.org/project> a nidm:Project, prov:Activity .
<{SUBJECT}> a prov:Agent, prov:Person ; ndar:src_subject_id "1"^^xsd:string .
<{ACQ_RUN1}> a nidm:AcquisitionObject, prov:Entity ;
  nidm:hadAcquisitionModality nidm:MagneticResonanceImaging ; nidm:hadImageUsageType nidm:Anatomical ;
  nfo:filename "bids::sub-001/anat/sub-001_run-1_T1w.nii.gz"^^xsd:string ;
  prov:wasGeneratedBy <http://iri.nidash.org/act-run-1> .
<http://iri.nidash.org/act-run-1> a prov:Activity ;
  prov:qualifiedAssociation [ a prov:Association ; prov:agent <{SUBJECT}> ] .
<{ACQ_HIRES}> a nidm:AcquisitionObject, prov:Entity ;
  nidm:hadAcquisitionModality nidm:MagneticResonanceImaging ; nidm:hadImageUsageType nidm:Anatomical ;
  nfo:filename "bids::sub-001/anat/sub-001_acq-hires_run-1_T1w.nii.gz"^^xsd:string ;
  prov:wasGeneratedBy <http://iri.nidash.org/act-hires> .
<http://iri.nidash.org/act-hires> a prov:Activity ;
  prov:qualifiedAssociation [ a prov:Association ; prov:agent <{SUBJECT}> ] .
"""


def recon_all_tree(output_dir):
    """<output_dir>/sub-001/freesurfer with one stats file, as the app lays it out."""
    fs_dir = output_dir / "sub-001" / "freesurfer"
    (fs_dir / "stats").mkdir(parents=True)
    shutil.copy(ASEG_STATS, fs_dir / "stats" / "aseg.stats")
    # without a build stamp fs_to_nidm prompts for the FreeSurfer version
    (fs_dir / "scripts").mkdir()
    (fs_dir / "scripts" / "build-stamp.txt").write_text(
        "freesurfer-linux-centos7_x86_64-7.4.1-20230614-7eb8460\n")


def convert(output_dir, nidm_input_dir=None):
    wrapper = MagicMock()
    wrapper.get_subject_t1_info.return_value = {
        "T1w_images": ["/bids/sub-001/anat/sub-001_run-1_T1w.nii.gz"], "T2w_images": []}
    nidm_conversion(str(output_dir), "001", wrapper,
                    nidm_input_dir=str(nidm_input_dir) if nidm_input_dir else None)
    return Graph().parse(output_dir / "sub-001" / "nidm.ttl", format="turtle")


def stats_subject_agents(g):
    """Agents the FreeSurfer stats activities are associated with as subject."""
    return {a for assoc in g.subjects(PROV.hadRole, SIO_SUBJECT) for a in g.objects(assoc, PROV.agent)}


def subject_ids(g):
    return {str(o) for o in g.objects(None, NDAR_ID)}


def test_converter_testdata_present():
    assert ASEG_STATS.is_file(), (
        f"{ASEG_STATS} missing: check out the submodule (git submodule update --init)")


def test_measures_attach_to_the_input_subject_and_the_t1w_used(tmp_path):
    nidm_in = tmp_path / "NIDM" / "sub-001"
    nidm_in.mkdir(parents=True)
    (nidm_in / "nidm.ttl").write_text(INPUT_TTL)
    out = tmp_path / "out"
    recon_all_tree(out)

    g = convert(out, nidm_input_dir=tmp_path / "NIDM")

    assert stats_subject_agents(g) == {SUBJECT}
    assert subject_ids(g) == {"1"}, "no new subject agent should be minted"
    stats_activities = {act for assoc in g.subjects(PROV.hadRole, SIO_SUBJECT)
                        for act in g.subjects(PROV.qualifiedAssociation, assoc)}
    used = {u for act in stats_activities for u in g.objects(act, PROV.used)}
    assert ACQ_RUN1 in used and ACQ_HIRES not in used, used


@pytest.mark.parametrize("with_input_nidm", [True, False])
def test_successful_conversion_logs_no_runtime_warning(tmp_path, with_input_nidm):
    """Run as `python -m segstats_jsonld.fs_to_nidm`, runpy warned on every
    conversion that the module was already in sys.modules (the package
    __init__ imports it), and the app relayed that as two WARNING lines in
    every subject's log."""
    import logging

    nidm_input_dir = None
    if with_input_nidm:
        nidm_input_dir = tmp_path / "NIDM"
        (nidm_input_dir / "sub-001").mkdir(parents=True)
        (nidm_input_dir / "sub-001" / "nidm.ttl").write_text(INPUT_TTL)
    out = tmp_path / "out"
    recon_all_tree(out)
    records = []
    handler = logging.Handler(level=logging.WARNING)
    handler.emit = records.append
    app_logger = logging.getLogger("freesurfer-nidm")
    app_logger.addHandler(handler)
    try:
        convert(out, nidm_input_dir=nidm_input_dir)
    finally:
        app_logger.removeHandler(handler)
    warnings = [r.getMessage() for r in records]
    assert [w for w in warnings if "RuntimeWarning" in w or "sys.modules" in w] == [], warnings


def test_without_input_nidm_the_subject_is_not_named_freesurfer(tmp_path):
    out = tmp_path / "out"
    recon_all_tree(out)

    g = convert(out)

    assert stats_subject_agents(g), "no stats activity has a subject agent"
    assert subject_ids(g) == {"sub-001"}


def test_rerun_without_input_nidm_replaces_the_previous_output(tmp_path):
    """Regenerating NIDM with --skip-freesurfer reruns the converter into the
    same directory; the earlier nidm.ttl must not survive it."""
    out = tmp_path / "out"
    recon_all_tree(out)
    convert(out)
    nidm_ttl = out / "sub-001" / "nidm.ttl"
    # stand in for an output written by 0.1.0-0.1.2
    stale = Graph()
    stale.add((URIRef("http://iri.nidash.org/stale"), NDAR_ID, Literal("freesurfer", datatype=XSD.string)))
    stale.serialize(destination=str(nidm_ttl), format="turtle")

    g = convert(out)

    assert "freesurfer" not in subject_ids(g)
    assert subject_ids(g) == {"sub-001"}


SUBMODULE = REPO_ROOT / "src" / "segstats_jsonld"


def _parse_with_checked_out_converter(tmp_path, with_input_nidm):
    """Build the fs_to_nidm argv exactly as nidm_conversion does, then hand it
    to the argument parser of the converter checked out at src/segstats_jsonld,
    including main()'s own checks of the parsed arguments. Return the parsed
    namespace. An option the pinned converter does not accept makes its parser
    exit 2, which fails the test."""
    import argparse
    import importlib
    import sys
    from unittest.mock import patch

    assert (SUBMODULE / "segstats_jsonld" / "fs_to_nidm.py").is_file(), (
        f"{SUBMODULE} missing: check out the submodule (git submodule update --init)")

    nidm_input_dir = None
    if with_input_nidm:
        nidm_input_dir = tmp_path / "NIDM"
        (nidm_input_dir / "sub-001").mkdir(parents=True)
        (nidm_input_dir / "sub-001" / "nidm.ttl").write_text(INPUT_TTL)
    out = tmp_path / "out"
    recon_all_tree(out)
    wrapper = MagicMock()
    wrapper.get_subject_t1_info.return_value = {
        "T1w_images": ["/bids/sub-001/anat/sub-001_run-1_T1w.nii.gz"], "T2w_images": []}
    def converted(cmd, *args, **kwargs):
        # without an input NIDM, a run that writes nothing fails the subject
        if "-o" in cmd:
            (Path(cmd[cmd.index("-o") + 1]) / "aseg_nidm.jsonld").write_text(
                '{"@id": "http://example.org/aseg", "http://example.org/p": "1"}')
        return MagicMock(returncode=0, stdout="", stderr="")

    with patch("src.run.subprocess.run", side_effect=converted) as run:
        nidm_conversion(str(out), "001", wrapper,
                        nidm_input_dir=str(nidm_input_dir) if nidm_input_dir else None)
    cmd = next(c.args[0] for c in run.call_args_list if CONVERTER_BOOTSTRAP in c.args[0])
    assert cmd[:3] == [sys.executable, "-c", CONVERTER_BOOTSTRAP], cmd

    saved_path = list(sys.path)
    saved = {n: m for n, m in sys.modules.items()
             if n == "segstats_jsonld" or n.startswith("segstats_jsonld.")}
    for n in saved:
        del sys.modules[n]
    sys.path.insert(0, str(SUBMODULE))
    try:
        converter = importlib.import_module(CONVERTER_MODULE)
        assert Path(converter.__file__).resolve().is_relative_to(SUBMODULE.resolve()), converter.__file__
        parsed = {}
        with patch.object(converter, "_convert", lambda args: parsed.setdefault("args", args)), \
             patch.object(sys, "argv", ["fs_to_nidm.py", *cmd[3:]]):
            try:
                converter.main()
            except SystemExit as exc:
                pytest.fail(f"the checked-out fs_to_nidm rejected the app's argv (exit {exc.code}): {cmd[3:]}")
        assert "args" in parsed, "fs_to_nidm main() returned without converting"
        return parsed["args"]
    finally:
        sys.path[:] = saved_path
        for n in [n for n in sys.modules if n == "segstats_jsonld" or n.startswith("segstats_jsonld.")]:
            del sys.modules[n]
        sys.modules.update(saved)


def test_checked_out_converter_accepts_the_argv_with_an_input_nidm(tmp_path):
    args = _parse_with_checked_out_converter(tmp_path, with_input_nidm=True)
    assert args.acq_filename == ["sub-001_run-1_T1w.nii.gz"]
    assert args.forcenidm and args.nidm_file and args.subjid == "sub-001"


def test_checked_out_converter_accepts_the_argv_without_an_input_nidm(tmp_path):
    args = _parse_with_checked_out_converter(tmp_path, with_input_nidm=False)
    assert args.nidm_file is None and args.jsonld and args.subjid == "sub-001"


def test_each_converter_warning_is_logged_once(tmp_path):
    """Python prints a warning as two lines, `file:line: UserWarning: msg`
    and the source line, and the app logs every stderr line, so each
    converter warning was logged twice, the second time as code with raw
    `{subjid}` placeholders. The conversion prints warnings as one line."""
    import logging

    nidm_in = tmp_path / "NIDM" / "sub-001"
    nidm_in.mkdir(parents=True)
    # a project and no subjects: --forcenidm adds the subject, with a warning;
    # -acq_filename names a T1w the input does not have, with another
    (nidm_in / "nidm.ttl").write_text(
        "@prefix nidm: <http://purl.org/nidash/nidm#> .\n"
        "@prefix prov: <http://www.w3.org/ns/prov#> .\n"
        "<http://iri.nidash.org/project> a nidm:Project, prov:Activity .\n")
    out = tmp_path / "out"
    recon_all_tree(out)
    records = []
    handler = logging.Handler(level=logging.WARNING)
    handler.emit = records.append
    app_logger = logging.getLogger("freesurfer-nidm")
    app_logger.addHandler(handler)
    try:
        convert(out, nidm_input_dir=tmp_path / "NIDM")
    finally:
        app_logger.removeHandler(handler)
    relayed = [r.getMessage() for r in records if r.getMessage().startswith("fs_to_nidm: ")]
    assert any("which has no subjects" in m for m in relayed), relayed
    assert not any("warnings.warn(" in m or "{subjid}" in m or "{want}" in m for m in relayed), relayed
    assert len(relayed) == len(set(relayed)), relayed


@pytest.mark.parametrize("with_input_nidm", [True, False])
def test_conversion_logs_the_pynidm_it_loaded(tmp_path, with_input_nidm):
    """The converter switches to PyNIDM 4.x conventions without a word when
    nidm.linkml is absent, so a PyNIDM from a host PYTHONPATH would go
    unnoticed. The log names, once per run, the converter file and the
    PyNIDM version and file the conversion imported."""
    import logging
    import os

    import nidm

    nidm_input_dir = None
    if with_input_nidm:
        nidm_input_dir = tmp_path / "NIDM"
        (nidm_input_dir / "sub-001").mkdir(parents=True)
        (nidm_input_dir / "sub-001" / "nidm.ttl").write_text(INPUT_TTL)
    out = tmp_path / "out"
    recon_all_tree(out)
    records = []
    handler = logging.Handler(level=logging.INFO)
    handler.emit = records.append
    app_logger = logging.getLogger("freesurfer-nidm")
    app_logger.addHandler(handler)
    old_level = app_logger.level
    app_logger.setLevel(logging.INFO)
    try:
        convert(out, nidm_input_dir=nidm_input_dir)
    finally:
        app_logger.removeHandler(handler)
        app_logger.setLevel(old_level)
    found = [r for r in records if r.getMessage().startswith("NIDM conversion uses")]
    lines = [r.getMessage() for r in found]
    assert len(lines) == 1, lines
    assert found[0].levelno == logging.INFO, lines
    assert f"PyNIDM {nidm.__version__} from {os.path.realpath(nidm.__file__)}" in lines[0], lines
    assert "fs_to_nidm.py" in lines[0], lines
