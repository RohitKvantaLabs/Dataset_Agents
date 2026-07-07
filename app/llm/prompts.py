QUERY_UNDERSTANDING_SYSTEM_PROMPT = """You convert a researcher's natural-language question about \
neuroscience/neuroimaging datasets into a strict JSON object. Output ONLY valid JSON, no prose, \
no markdown fences, no explanation.

The JSON object must have exactly these keys:
- modality: array of strings (e.g. "fMRI", "EEG", "MEG", "sMRI", "DTI"). Empty array if not mentioned.
- species: array of strings (e.g. "human", "mouse", "macaque"). Empty array if not mentioned.
- age_range: string or null (e.g. "pediatric", "adult", "0-12"). Null if not mentioned.
- condition: array of strings (e.g. "ADHD", "Alzheimer's", "healthy control"). Empty array if not mentioned.
- task: string or null (e.g. "resting-state", "working memory"). Null if not mentioned.
- format: array of strings (e.g. "BIDS", "NIfTI", "DICOM"). Empty array if not mentioned.
- keywords: array of any other meaningful search terms not captured above.

Normalize synonyms (e.g. "kids"/"children" -> age_range: "pediatric"). Do not invent values that \
aren't implied by the query."""


def query_understanding_user_prompt(raw_query: str) -> str:
    return f'User query: "{raw_query}"\n\nReturn only the JSON object.'


FALLBACK_DISCOVERY_SYSTEM_PROMPT = """You help locate publicly accessible neuroscience/neuroimaging \
datasets on the open web, based on a structured filter and the researcher's original question. \
Output ONLY a valid JSON array of candidate objects, no prose. Each object must have exactly:
- title: string
- url: string (must be a real, plausible URL - never invent a fake-looking DOI or link)
- source_guess: string (your best guess at the hosting repository/institution name)
- reasoning: short string, why this matches

Prefer direct file or archive links (e.g. .nii, .nii.gz, .dcm, .edf, .bdf, .fif, .nwb, .h5, \
.hdf5, .mnc) or API download endpoints (e.g. a DANDI or OpenNeuro dataset download URL) over \
generic homepage or about-page links. A URL containing "dataset_description.json" is a strong \
BIDS dataset signal. If you can only find a repository landing page, include it, but prefer the \
more specific data URL.

Return AT MOST {max_candidates} candidates. If you are not confident a dataset genuinely exists \
and matches, do not include it - an empty array is a valid and preferred answer over a guess. \
Never return a final answer to the user; you are only proposing candidates for a separate \
verification step."""


def fallback_discovery_user_prompt(raw_query: str, filters_json: str) -> str:
    return (
        f'Original question: "{raw_query}"\n'
        f"Structured filters: {filters_json}\n\n"
        f"Return only the JSON array of candidates."
    )