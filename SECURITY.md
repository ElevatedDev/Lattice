# Security Policy

## Supported Versions

Lattice has not shipped a stable release yet. Treat the current repository as
pre-release software.

| Version | Supported |
| --- | --- |
| `main` / current development branch | Best-effort pre-release review |
| `< 1.0` source snapshots | No sustained security support |
| Published Maven artifacts | Not available yet |

## Reporting A Vulnerability

Do not publish suspected vulnerabilities in public issues, pull requests, or
discussion threads before maintainers have had a chance to respond privately.

Use GitHub private vulnerability reporting if it is enabled for the repository.
If it is not enabled, contact the maintainers privately through the existing
project channel and include "security" in the subject or opening line.

Useful report details include:

- affected commit, branch, or version;
- operating system, JVM, and native backend status;
- graph shape, edge policies, and placement settings involved;
- whether the issue requires untrusted payloads, untrusted graph definitions,
  or local host access;
- minimal reproduction steps;
- observed impact, such as crash, data loss, ordering violation, resource leak,
  denial of service, or native placement misuse.

## Security-Relevant Areas

Pay particular attention to:

- JNI loading and native library path configuration;
- CPU affinity and NUMA placement permissions;
- bounded queue overflow behavior;
- slab-handle ownership and release paths;
- graph failure and abort behavior;
- user-provided stage logic and exception handlers;
- denial-of-service risks from backpressure, joins, and retained state.

Lattice does not sandbox user stage code. Applications must treat stage logic,
copiers, key extractors, stamp extractors, join combiners, and exception
handlers as trusted application code.
