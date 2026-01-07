# PHDS Tagging Process (`phds/vX.Y.Z`)

This repo uses SemVer-style git tags with the prefix `phds/v`, e.g. `phds/v1.0.7`.

## 1) Work in a Feature Branch

- Create or switch to your feature branch.
- Make your changes and open a PR.

## 2) Run CI Testing (Fabric Notebook)

1. Open the Fabric notebook named **DBT with params ssh CI**.
2. Update the notebook parameter `tuva_feature_branch` to the name of your feature branch.
3. Start the run and wait for it to finish successfully.

## 3) Validate Results (Data Quality)

1. Query `phds_ci_testing.data_quality.base_head_compare`.
2. Export/save the results to a CSV.
3. Open the UI viewer at `ui/data_quality_base_head_compare.html`.
4. Verify:
   - The run dates/timestamps are what you expect (recent).
   - The rows/marts that changed match what you expect for your PR.
5. Take a screenshot and attach it to the PR with a short explanation of what you verified.

## 4) Get PR Approved (and Merged)

- Request review/approval.
- Squash and merge the PR after approval.

## 5) After Merge: Pull Latest and Tag the Release

Run these commands locally from a clean working tree:

1. Fetch latest branches and tags:
   - `git fetch --prune --tags`
2. Switch to the default branch and pull the latest (use `main`):
   - `git checkout main`
   - `git pull --ff-only`
3. Confirm you’re on the exact commit you want to tag:
   - `git log -1 --oneline`

## 6) Pick the Next Version

1. See existing tags (newest first):
   - `git tag -l 'phds/v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -n 20`
2. Choose the next `X.Y.Z` per SemVer (major.minor.patch as appropriate).

## 7) Create and Push the Tag

Create an annotated tag (recommended):

- `git tag -a phds/vX.Y.Z -m "phds vX.Y.Z"`
- `git push origin phds/vX.Y.Z`

## 8) Verify the Tag

- `git show phds/vX.Y.Z`
- `git tag -l 'phds/v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -n 5`

## 9) Update Orchestration

1. Update the orchestration config file ( phds_lakehouse_test/Files/pipeline_configuration/pipeline_run_version.json ) to run the tuva project with the latest tag we just created