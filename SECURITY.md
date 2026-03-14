# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in adit, **please do not open a public
issue.** Instead, report it privately:

1. Email the maintainer directly (currently danny@mundy.sh), or
2. Use GitHub's private vulnerability reporting if available on this repository

Include as much detail as you can: steps to reproduce, affected versions, and
any potential impact.

## Scope

Adit runs on localhost by default and handles personal messaging data. Security
issues we care about include:

- Unauthorized access to the REST API or WebSocket stream
- Bypass of origin checks or bearer token authentication
- SQLite cache data exposure
- Bluetooth protocol-level issues that could leak data

## Response

You'll get an acknowledgment within a few days. Fixes for confirmed
vulnerabilities will be prioritized and released as soon as practical.

## Current security model

See the [Security Model](README.md#security-model) section in the README for
details on how adit handles authentication and access control.
