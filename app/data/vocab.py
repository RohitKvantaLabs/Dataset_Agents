"""
Controlled vocabularies + SPDX license map (§3.3, §4.8).

Populated 2026-08-04 (query-first stabilization, Issue 3): repository
candidates often declare ``modality = []`` while their title/description
clearly describe MEG / MRI / EEG / fMRI / PET etc. Stage 3 of the quality
pipeline reads these vocabularies via exact (word-boundary) token matching
and fills empty structured fields — reusing the existing enrichment
architecture (no duplicate normalization system). Empty vocab = no backfill;
the pipeline degrades gracefully.

Usage contract
--------------
- ``MODALITY_VOCAB``  ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``SPECIES_VOCAB``   ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``REGION_TERMS``    ``list[str]``             accepted region tokens (lowercase)
- ``AGE_TERMS``       ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``DISEASE_TERMS``   ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``LICENSE_SPDX_MAP`` ``dict[str, str]``       raw license string -> SPDX id

Stage 3 reads these via exact (word-boundary) token matching and only
sets a field when a vocabulary token matches — never LLM, never inferred.
Modality labels mirror the synonym families already approved in
``app.connectors.base.MODALITY_SYNONYMS`` (a token maps to the same label a
synonym family would match).
"""

# Modality label -> accepted raw tokens (lowercase). Mirrors the approved
# MODALITY_SYNONYMS families in app/connectors/base.py.
MODALITY_VOCAB: dict[str, list[str]] = {
    "meg": ["meg", "magnetoencephalography", "magnetoencephalogram"],
    "eeg": ["eeg", "electroencephalography", "electroencephalogram"],
    "ieeg": ["ieeg", "intracranial eeg", "intracranial electroencephalography"],
    "ecog": ["ecog", "electrocorticography"],
    "mri": ["mri", "magnetic resonance imaging"],
    "fmri": ["fmri", "functional mri", "functional magnetic resonance imaging"],
    "smri": ["smri", "structural mri", "structural magnetic resonance imaging", "t1-weighted", "t2-weighted"],
    "pet": ["pet", "positron emission tomography"],
    "dti": ["dti", "diffusion tensor imaging", "diffusion-weighted imaging", "diffusion mri"],
    "nirs": ["nirs", "near-infrared spectroscopy"],
    "fnirs": ["fnirs", "functional near-infrared spectroscopy"],
}

# Species label -> accepted raw tokens (lowercase).
SPECIES_VOCAB: dict[str, list[str]] = {
    "human": [
        "human", "humans", "participant", "participants", "volunteer", "volunteers",
        "subject", "subjects", "patients", "healthy controls", "adult volunteers",
    ],
    "mouse": ["mouse", "mice", "murine"],
    "rat": ["rat", "rats"],
    "macaque": ["macaque", "macaques", "rhesus", "monkey", "monkeys", "primate", "primates", "nhp", "non-human primate", "nonhuman primate"],  # nhp = Allen Brain Atlas abbreviation for non-human primate
    "zebrafish": ["zebrafish", "danio rerio"],
    "drosophila": ["drosophila", "fruit fly", "fruit flies"],
    "c.elegans": ["c. elegans", "caenorhabditis elegans"],
}

# Flat region tokens (lowercase) — first exact match wins. More specific
# terms are listed first so e.g. "prefrontal cortex" wins over "cortex".
REGION_TERMS: list[str] = [
    "dorsolateral prefrontal cortex",
    "ventromedial prefrontal cortex",
    "prefrontal cortex",
    "somatosensory cortex",
    "motor cortex",
    "visual cortex",
    "auditory cortex",
    "cingulate cortex",
    "entorhinal cortex",
    "parahippocampal gyrus",
    "parahippocampal cortex",
    "insula",
    "insular cortex",
    "frontal cortex",
    "temporal cortex",
    "parietal cortex",
    "occipital cortex",
    "cerebral cortex",
    "basal ganglia",
    "nucleus accumbens",
    "substantia nigra",
    "ventral tegmental area",
    "corpus callosum",
    "white matter",
    "gray matter",
    "grey matter",
    "default mode network",
    "hippocampus",
    "hippocampal",
    "amygdala",
    "thalamus",
    "hypothalamus",
    "striatum",
    "cerebellum",
    "cerebellar",
    "brainstem",
    "whole brain",
    "whole-brain",
]

# Age label -> accepted raw tokens (lowercase).
AGE_TERMS: dict[str, list[str]] = {
    "infant": ["infant", "infants", "newborn", "newborns", "neonatal", "neonates"],
    "child": ["child", "children", "pediatric", "pediatrics", "school-age"],
    "adolescent": ["adolescent", "adolescents", "teenager", "teenagers", "youth"],
    "adult": ["adult", "adults"],
    "elderly": ["elderly", "older adult", "older adults", "older-adult", "geriatric", "aged"],
}

# Disease label -> accepted raw tokens (lowercase). Short, ambiguous
# abbreviations (pd, ad, ms, als, mci, tbi) are intentionally NOT included:
# exact word-boundary matching would risk false positives.
DISEASE_TERMS: dict[str, list[str]] = {
    "parkinson": ["parkinson", "parkinson's", "parkinsons", "parkinson disease", "parkinson's disease"],
    "alzheimer": ["alzheimer", "alzheimer's", "alzheimers", "alzheimer disease", "alzheimer's disease"],
    "mild cognitive impairment": ["mild cognitive impairment", "cognitive impairment"],
    "dementia": ["dementia", "demented", "lewy body"],
    "adhd": ["adhd", "attention deficit hyperactivity disorder", "attention deficit disorder"],
    "schizophrenia": ["schizophrenia", "schizophrenic", "psychosis", "psychotic"],
    "bipolar": ["bipolar", "bipolar disorder", "manic depression"],
    "depression": ["depression", "depressive", "major depressive disorder", "mdd"],
    "anxiety": ["anxiety", "anxious", "generalized anxiety"],
    "autism": ["autism", "autistic", "asd", "autism spectrum disorder"],
    "epilepsy": ["epilepsy", "epileptic", "epileptiform", "seizure", "seizures"],
    "multiple sclerosis": ["multiple sclerosis"],
    "amyotrophic lateral sclerosis": ["amyotrophic lateral sclerosis"],
    "huntington": ["huntington", "huntington's", "huntingtons", "huntington disease"],
    "stroke": ["stroke", "strokes", "ischemic stroke", "cerebrovascular accident"],
    "traumatic brain injury": ["traumatic brain injury", "head injury", "concussion"],
    "migraine": ["migraine", "migraines"],
    "insomnia": ["insomnia", "sleep disorder", "sleep disorders"],
    "obesity": ["obesity", "obese"],
    "diabetes": ["diabetes", "diabetic", "type 2 diabetes", "type 1 diabetes"],
    "covid": ["covid", "covid-19", "sars-cov-2", "coronavirus"],
    "tinnitus": ["tinnitus"],
    "chronic pain": ["chronic pain", "neuropathic pain", "fibromyalgia"],
}

# Unimplemented — pending approved SPDX license map. Connector-declared
# license strings are preserved unchanged.
LICENSE_SPDX_MAP: dict[str, str] = {}
