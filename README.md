# Perils Atlas

Interactive Quarto website for exploring ecological jurisprudence initiatives across regions, legal forms, actor types, and jurisprudential framings.

Live site: https://shrutirkashyap.github.io/perils/

## Project notes

- Public site output is published from `docs/` for GitHub Pages.
- Raw research data is kept under `data/01_raw/`.
- Analysis-ready derived data is generated under `data/02_cleaned/`.
- Public-facing pages should avoid repository path references; technical workflow details belong here, not on the website itself.

## Local workflow

- Render the full site locally with `powershell -ExecutionPolicy Bypass -File render.ps1`
- Preview the rendered site with `powershell -ExecutionPolicy Bypass -File preview.ps1`
- Rebuild the cleaned data with `python scripts/clean_ejm.py`
