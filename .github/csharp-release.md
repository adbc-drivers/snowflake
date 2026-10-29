# C# releases

The **CSharp Release** workflow is `.github/workflows/csharp_release.yaml`.
It prepares and publishes `AdbcDrivers.Snowflake`; Go releases remain separate.

## Repository setup

- `RELEASE_PLEASE_TOKEN`: repository secret for Release Please, with access to
  read and write repository contents and pull requests.
- `NUGET_USER`: repository Actions variable containing the NuGet username of the
  trusted publishing policy creator.
- `nuget-release`: GitHub environment with the required publication reviewers.
- NuGet trusted publishing policy: repository `adbc-drivers/snowflake`, workflow
  filename `csharp_release.yaml`, and environment `nuget-release`.
- `PREVIEW_RETENTION_DAYS`: optional repository Actions variable, default `28`.
  Accepts 1–90 days, subject to repository and organization retention limits.
  Changes apply to new preview uploads, not existing artifacts or release assets.

## Normal release flow

1. Merge a code PR using a conventional commit message, such as `fix: ...`,
   `feat: ...`, or `feat!: ...`. With squash merges, check the final commit title.
2. Release Please maintains one open release PR with the proposed version and
   changelog. Commits that do not warrant a release do not start a preview build;
   the workflow summary explains when no releasable changes were found.
3. The workflow builds a preview from the exact release PR head, including its
   proposed version. The package version is `VERSION-preview.PR_NUMBER`, or
   `VERSION-preview.SHA` for a direct push with no associated merged PR.
   Download the Actions artifact using the link in the release PR description.
   It includes the package, symbols, and `preview-source.json`. Previews are not
   published to NuGet. Arrow retains its own dependency version.
4. Review and merge the release PR. The workflow creates a draft `csharp/vVERSION`
   release, builds the final package and symbols from that merged commit, and
   attaches both packages and their signed provenance to the draft.
5. Follow the release PR's workflow link, choose **Review deployments**, select
   **nuget-release**, and **Approve and deploy**. The publishing job downloads
   and verifies the staged assets, publishes both packages to NuGet using OIDC,
   and publishes the GitHub release. No stored NuGet API key is required.

While a final draft awaits publication, new code remains for the next release;
it does not replace the selected final candidate. Successful publication starts
preparation of the next release PR. Published release assets do not use preview
retention.

## Manual runs and recovery

Run the workflow against `main` with optional inputs empty to prepare a release.
To test an existing release PR, supply `preview_pr`; `trigger_pr` can identify a
merged code PR for a manual preview test.

To rebuild an existing draft before publication starts, supply its `release_tag`
and exact `release_sha`. Set `publish` to false for a staging-only test. After
publication starts, resume the original run with **Re-run failed jobs** so that
successful jobs and the reviewed packages are reused.

Rejecting a deployment approval stops that attempt; it does not delete the
draft. To abandon a candidate known to have published neither package to NuGet,
delete its draft on the GitHub Releases page, then run normal preparation on
`main`. Leave published releases and tags alone. The next release PR may reuse
the abandoned version. If either NuGet upload succeeded, complete the original
release instead of abandoning it.

Re-running a run retains its original workflow filename. After a workflow rename,
an old run still needs a NuGet policy matching that old filename.

NuGet retries use `--skip-duplicate`. This handles duplicate responses but does
not compare the content of an already published NuGet package. The workflow
verifies source, digests, and provenance of the staged GitHub assets.
