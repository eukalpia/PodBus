# Security policy

## Supported versions

PodBus is currently in alpha. Security fixes are applied to the latest commit on `main` and included in the next tagged release.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability reporting for this repository. Include:

- affected package and version or commit;
- a minimal reproduction;
- expected impact;
- broker and deployment details;
- any suggested mitigation.

Avoid including production credentials, customer payloads, or personal data. Acknowledgement and remediation timing depend on severity and reproducibility.

## Security expectations

Applications remain responsible for broker authentication, TLS, authorization, secret storage, payload classification, and handler idempotency. Dead-letter payload inclusion is disabled by default because messages may contain sensitive data.
