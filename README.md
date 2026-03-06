# cicd-databricks
This repository is for exploring different CI/CD best practices for Databricks.

## Branching Strategy
We use a `dev`-first workflow, with `dev` kept in sync with `main`.

- Create branches from `dev` and open PRs back to `dev`.
- Merges to `dev` trigger **dev** deployments.
- After validation, merge `dev` into `main` to trigger **stg** deployments.
- Releases (tags) trigger **prod** deployments.

Release tags follow SemVer (e.g., `v1.2.3`).

## Databricks Asset Bundles
Bundles live under [bundles/](bundles) and define workspace resources (pipelines, schemas, volumes) and source code references.

Key files:

- [bundles/resorts/databricks.yml](bundles/resorts/databricks.yml): bundle definition and targets.
- [bundles/resorts/resources](bundles/resorts/resources): pipelines, schemas, and volumes.
- [bundles/resorts/src](bundles/resorts/src): bundle-scoped source code.

Common commands (run from repo root):

- Validate: `databricks bundle validate -t <target>`
- Deploy: `databricks bundle deploy -t <target>`
- Run: `databricks bundle run -t <target> <resource>`

Targets should map to environments (e.g., `dev`, `staging`, `prod`) and use environment-specific variables in the bundle definition.

## Building Wheel Files
The Python package lives under [src/infinitive](src/infinitive). Build wheels using the project metadata in [pyproject.toml](pyproject.toml).

Build a wheel locally:

1. Create/activate a virtual environment.
2. Install build tooling: `pip install build`.
3. Build: `python -m build`.

Artifacts land in `dist/` (e.g., `*.whl` and `*.tar.gz`). Use the wheel in Databricks jobs or bundle workflows by adding it to cluster libraries or task libraries.
