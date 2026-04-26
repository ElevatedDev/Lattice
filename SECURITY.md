# Security Policy

## Supported Versions

Lattice has not shipped a 1.0 release yet. The current repository state should
be treated as pre-release software.

| Version | Supported |
| --- | --- |
| `main` / current working branch | Best-effort pre-release review |
| Published Maven artifacts | Not available yet |

## Reporting A Vulnerability

This repository does not yet define a public security contact or private
advisory workflow. Before public release, maintainers should add a monitored
security address or enable GitHub private vulnerability reporting.

Until that exists, report suspected vulnerabilities privately through the
maintainers' existing project channel. Do not include exploit details in a
public issue before maintainers have acknowledged the report.

Useful report details include:

- affected commit or version;
- operating system, JVM, and native backend status;
- graph shape and edge policies involved;
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
