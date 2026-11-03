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

from src.run import nidm_conversion

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
