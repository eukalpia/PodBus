# Serverpod contribution validation

This directory is a temporary validation harness for `serverpod/serverpod#5255`.
It does not modify PodBus itself.

The workflow checks out the latest Serverpod `main` branch, applies
`5255-ci-generation.patch`, commits the patch inside the temporary checkout, and
runs the actual Serverpod generation test on the same operating systems and
Flutter versions used by the upstream `serverpod_generate` matrix.

## What the patch changes

- Moves clean-state setup into `util/run_tests_serverpod_generate`.
- Deletes `tests/serverpod_test_server/lib/src/generated` before the existing
  full generation pass.
- Verifies that the generated directory is recreated.
- Keeps `util/ensure_no_changes` as the deterministic-output check.
- Removes the second standalone generator invocation from the workflow.
- Adds root-directory and stricter shell validation to the test script.

## Validation matrix

- Ubuntu + Flutter 3.38.4
- Ubuntu + Flutter 3.41.5
- Windows Git Bash + Flutter 3.38.4
- Windows Git Bash + Flutter 3.41.5

The patch is committed in the temporary Serverpod checkout before running the
script. This matters because `util/ensure_no_changes` expects the repository to
be clean before generation, just as it would be for a real pull-request commit.

This branch is only a test stand. A final upstream pull request still requires a
fork of `serverpod/serverpod` under the contributor's account.
