# Security policy

## Supported versions

Security fixes go into the latest release of each module (`rategen`,
`rategen/redis` and `rategen/prometheus`). Before v1.0.0, only the latest
minor version is supported.

Build with a supported Go release at its latest patch level. Older patch
releases of the standard library have known vulnerabilities.

## Reporting a vulnerability

Please do **not** open a public issue. Report privately through GitHub:
**Security → Report a vulnerability** on this repository. Include a
description, the affected versions, and a reproduction if possible.

Fixes are released together with a GitHub security advisory, crediting the
reporter unless they prefer otherwise.

## Scope

Bypassing a configured limit, leaking keys or identities, unbounded memory
growth, and crashes caused by request input are all in scope. The threat
model and the mitigations rategen relies on are described in
[docs/security.md](docs/security.md).
