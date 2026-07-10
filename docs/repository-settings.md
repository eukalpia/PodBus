# Repository settings

Apply these settings to the `main` branch before publishing a stable release.

## Branch protection

Require a pull request before merging and require the following checks:

- `Production gate`
- `Dart 3.12.0`
- `Dart stable`
- `CodeQL workflow scan`
- `Dependency review`
- `Secret scan`

Also enable:

- dismissal of stale approvals after new commits;
- conversation resolution before merge;
- linear history or squash merging;
- protection for administrators;
- deletion of head branches after merge;
- prevention of force pushes and branch deletion.

Require one approving review for alpha and beta releases. Increase this for stable releases or security-sensitive changes.

## Actions

Restrict Actions to trusted publishers and actions pinned to immutable commit SHAs. Keep workflow permissions read-only by default and grant write permissions only to the exact release or security job that needs them.

Create a protected `release` environment with manual approval. Store no long-lived pub.dev token when trusted publishing through OIDC is available.

## Security

Enable:

- dependency graph;
- Dependabot alerts and security updates;
- secret scanning and push protection;
- private vulnerability reporting;
- code scanning alerts;
- branch protection for workflow files.

Dependency review requires the repository dependency graph. A failing review because the graph is disabled is a repository configuration failure, not a reason to make the check optional.

## Releases

Create releases only from signed or protected tags matching the workspace version, for example `v0.1.0-alpha.1`. Do not move a published tag. If a release is wrong, publish a new version.
