"""Shared HCP/CCF fixtures for catalog tests (pure data — no I/O).

Page shapes mirror the LIVE humanconnectome.org Drupal pages (verified
2026-08-16):

- ``index_html()``: a study-index page (``/lifespan-studies`` or
  ``/disease-studies``) with ``<a href="/study/<slug>">`` cards.
- ``study_html()``: a Study landing page with the ``<h1 class="study-title">``
  title, ``Study Overview`` body, and ``<div class="investigator principal">``
  blocks (name in the img ``alt``, ``- Institution`` + role in the ``<h3>``).
- ``releases_html()``: a data-releases page with ``<div class="investigator">``
  release entries (img ``alt`` = release name, ``Released on MM/DD/YYYY`` span).
- ``publications_html()``: a publications page with ``dx.doi.org/10.…`` links.
"""

import html as _html

# The 20 verified Study slugs (both index pages list exactly these).
HCP_STUDY_SLUGS = [
    "alzheimers-disease-connectome-project",
    "amish-connectome-project",
    "changes-visual-cortical-connectivity-following-central-visual-field-loss",
    "connectomes-related-anxiety-depression",
    "connectomic-imaging-familial-sporadic-frontotemporal-degeneration",
    "connectomics-brain-aging-and-dementia",
    "crhd-dimensional-connectomics-anxious-misery",
    "crhd-human-connectomes-low-vision-blindness-and-sight-restoration",
    "crhd-mapping-connectomes-disordered-mental-states",
    "crhd-neural-disconnection-errant-visual-perception-psychotic-psychopathology",
    "crhd-perturbation-treatment-resistant-depression-connectome-fast-acting-therapies",
    "dual-mechanisms-cognitive-control",
    "epilepsy-connectome-project",
    "hcp-lifespan-aging",
    "hcp-lifespan-development",
    "hcp-young-adult",
    "human-connectome-project-for-early-psychosis",
    "lifespan-baby-connectome-project",
    "lifespan-developing-human-connectome-project",
    "structural-functional-connectome-across-ad-subtypes",
]

_TITLES = {
    "hcp-young-adult": "HCP Young Adult",
    "hcp-lifespan-aging": "HCP Aging / Aging Adult Brain Connectome (AABC)",
    "hcp-lifespan-development": "HCP Development",
    "alzheimers-disease-connectome-project": "Alzheimer's Disease Connectome Project",
    "epilepsy-connectome-project": "Epilepsy Connectome Project",
    "human-connectome-project-for-early-psychosis": "Human Connectome Project for Early Psychosis",
}

_PIS = {
    "hcp-young-adult": [
        ("Kamil Ugurbil", "UMinn"),
        ("David Van Essen", "WashU"),
    ],
    "alzheimers-disease-connectome-project": [
        ("Barbara Bendlin", "UWisc"),
        ("Shi-Jiang Li", "MCW"),
    ],
}

_DESCRIPTIONS = {
    "hcp-young-adult": (
        "Mapping the human brain is one of the great scientific challenges of the 21st century. "
        "The Human Connectome Project (HCP) has tackled key aspects of this challenge by charting "
        "the neural pathways that underlie brain function and behavior, including high-quality "
        "neuroimaging data in over 1100 healthy young adults."
    ),
    "hcp-lifespan-aging": (
        "The HCP-Aging study (Aging Adult Brain Connectome) collects multimodal imaging and "
        "behavioral data across the adult lifespan."
    ),
    "alzheimers-disease-connectome-project": (
        "The Alzheimer's Disease Connectome Project (ADCP) will collect data from participants "
        "who range from cognitively healthy to those with dementia due to Alzheimer's disease."
    ),
}


def _slug_title(slug: str) -> str:
    return _TITLES.get(slug, " ".join(p.capitalize() for p in slug.split("-")))


def _slug_pis(slug: str) -> list[tuple[str, str]]:
    return _PIS.get(slug, [("Jane Doe", "WashU"), ("John Smith", "UMN")])


def _slug_description(slug: str) -> str:
    return _DESCRIPTIONS.get(slug, f"Study description for {_slug_title(slug)}.")


def index_html(slugs: list[str]) -> str:
    """A study-index page listing the given study CARDS (mirrors Drupal markup).

    Cards live inside ``<div class="study-content"><h3><a href="/study/<slug>">``
    — the authoritative listing region. Nav/sidebar chrome is NOT emitted here
    (the fixture pages are pure card listings).
    """
    cards = "\n".join(
        f'<div class="full-width-study-info"><div class="study-info">'
        f'<div class="study-content"><h3><a href="/study/{s}">{_slug_title(s)}</a></h3>'
        f"</div></div></div>"
        for s in slugs
    )
    return f"""<!DOCTYPE html>
<html><head><title>Studies</title></head><body>
<div class="row">
  <h1>Studies</h1>
  {cards}
</div>
</body></html>"""


def _investigator_block(name: str, institution: str) -> str:
    return (
        '<div class="investigator principal">\n'
        f'    <div class="investigator__image"><img src="https://www.humanconnectome.org/storage/'
        f'x.png" alt="{name}"></div>\n'
        f"    <h3>{name}, Ph.D.  - {institution}\n\n"
        '                <span>Principal Investigator</span>\n'
        "    </h3>\n"
        "</div>"
    )


def study_html(
    slug: str,
    *,
    title: str | None = None,
    description: str | None = None,
    pis: list[tuple[str, str]] | None = None,
) -> str:
    """A Study landing page (mirrors the live Drupal page structure)."""
    title = title or _slug_title(slug)
    description = description if description is not None else _slug_description(slug)
    pis = pis if pis is not None else _slug_pis(slug)
    blocks = "\n".join(_investigator_block(n, i) for n, i in pis)
    return f"""<!DOCTYPE html>
<html><head>
<meta name="description" content="{_html.escape(description[:120])}">
</head><body>
<h1 class="study-title" onclick="window.location.assign('/study/{slug}')">{title}</a></h1>
<div class="field-name-body">
  <p>{_html.escape(description)}</p>
</div>
<div class="row singlepage-study-section">
  <h2>Study Overview</h2>
  <p>{_html.escape(description)}</p>
</div>
<div class="row singlepage-study-section">
  <h2>Principal Investigators</h2>
  {blocks}
  <p><strong><a href="/study/{slug}/investigators">See All Project Investigators</a></strong></p>
</div>
<div class="row singlepage-study-section">
  <h2>Current Data Releases</h2>
  <a href="/study/{slug}/data-releases">Data Releases</a>
</div>
<a href="/study/{slug}/project-protocols">Study Protocols</a>
</body></html>"""


def releases_html(releases: list[tuple[str, str]]) -> str:
    """A data-releases page (name, MM/DD/YYYY) — mirrors live release entries."""
    entries = []
    for name, date in releases:
        entries.append(
            '<div class="investigator">\n'
            f'    <div class="investigator__image"><img src="https://www.humanconnectome.org/'
            f'themes/uar_washu/assets/images/icons/icon-release-MR.png" alt="{name}"></div>\n'
            "    <h3>\n"
            '        <a href="https://www.humanconnectome.org/study/hcp-young-adult/document/'
            + name.lower().replace(' ', '-') + f'">{name}</a>\n'
            f"        <span>Released on {date}</span>\n"
            "    </h3>\n"
            "</div>"
        )
    # The CTA block (no release date) must be excluded by the parser.
    entries.append(
        '<div class="investigator">\n'
        '    <h3>"Let me explore the dataset" All HCP data can be downloaded from ConnectomeDB</h3>\n'
        "</div>"
    )
    return (
        '<!DOCTYPE html><html><body><div class="col-lg-6">'
        "<h1>Current Data Releases</h1>" + "\n".join(entries) + "</div></body></html>"
    )


def publications_html(dois: list[str]) -> str:
    """A publications page with dx.doi.org links (mirrors live markup)."""
    items = "\n".join(
        f'<li><a href="http://dx.doi.org/{doi}">{doi}</a></li>' for doi in dois
    )
    return (
        '<!DOCTYPE html><html><body><h1>Publications</h1><ul>' + items + "</ul></body></html>"
    )


def study_dict(
    slug: str,
    *,
    releases: list[tuple[str, str]] | None = None,
    publications: list[str] | None = None,
) -> dict:
    """A fully-assembled parsed study dict (as the ingestion would produce it)."""
    s = {
        "slug": slug,
        "name": _slug_title(slug),
        "url": f"https://www.humanconnectome.org/study/{slug}",
        "description": _slug_description(slug),
        "principalInvestigators": [
            {"name": n, "institution": i, "role": "Principal Investigator"}
            for n, i in _slug_pis(slug)
        ],
        "protocols": [f"https://www.humanconnectome.org/study/{slug}/project-protocols"],
        "dataUseTermsUrl": f"https://www.humanconnectome.org/study/{slug}/data-use-terms",
        "lastmod": None,
        "dataReleases": (
            [{"name": n, "date": d, "url": f"https://www.humanconnectome.org/study/{slug}"}
             for n, d in releases]
            if releases is not None
            else [
                {"name": "Q1 Subjects Data Release", "date": "03/05/2013",
                 "url": f"https://www.humanconnectome.org/study/{slug}"},
                {"name": "1200 Subjects Data Release", "date": "03/01/2017",
                 "url": f"https://www.humanconnectome.org/study/{slug}"},
            ]
        ),
        "publications": publications or ["10.1038/nature18933", "10.1038/sdata.2017.10"],
    }
    return s
