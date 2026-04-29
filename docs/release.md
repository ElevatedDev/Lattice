# Release Process

Maintainer recipe for cutting a Lattice release.

## Pre-Release Checklist

- [ ] Public-API changes reviewed against `docs/api.md` and the stable-surface
      list in `CONTRIBUTING.md`.
- [ ] `CHANGELOG.md` has a section for the new version with a date.
- [ ] `docs/compatibility-matrix.md` reflects current JDK / OS support.
- [ ] Generated Javadoc builds cleanly, reflects the public surface, and is
      refreshed with `./gradlew docsJavadoc`.
- [ ] `./gradlew releaseCheck` passes locally on Linux and Windows.
- [ ] `./gradlew jcstress` passes (long-running; nightly CI is acceptable).
- [ ] Native backend builds on Linux. Build macOS and Windows native artifacts
      before advertising host-native binaries for those platforms.
- [ ] Benchmark summary recorded under `docs/benchmark-results/<version>/`;
      raw JMH JSON/JFR artifacts and generated figures are attached to the
      release evidence if numbers are cited publicly.

## Cut The Release

```bash
# 1. Bump version
echo "releaseVersion=1.0.0" >> gradle.properties

# 2. Tag
git commit -am "release: 1.0.0"
git tag -s v1.0.0 -m "Lattice 1.0.0"
git push origin main v1.0.0
```

The `Release Artifacts` workflow builds the release bundle:

1. Builds the Maven artifacts (`jar`, `sourcesJar`, `javadocJar`) with
   `-PreleaseVersion=1.0.0`.
2. Builds the Linux native library.
3. Uploads the staged artifact bundle for maintainer inspection.

Publishing to Maven Central and attaching native binaries to a public GitHub
Release are explicit maintainer actions until signing and Central Portal
credentials are configured in repository secrets.

## Required Repository Secrets

| Secret | Purpose |
| --- | --- |
| `CENTRAL_USERNAME` / `CENTRAL_PASSWORD` | Sonatype Central Portal credentials, when Maven Central publishing is enabled. |
| `SIGNING_KEY` | ASCII-armored PGP private key. |
| `SIGNING_KEY_ID` | 8- or 16-char key id. |
| `SIGNING_PASSWORD` | Passphrase for the signing key. |

## Post-Release

- [ ] Verify the artifact on Maven Central after propagation.
- [ ] Verify GitHub Pages serves `api/1.0.0/`.
- [ ] Open a PR that bumps `gradle.properties` back to a `-SNAPSHOT` line for
      the next development cycle.
- [ ] Announce in `Discussions -> Announcements`.

## Reproducibility

The build is configured for reproducible archives:

- `preserveFileTimestamps=false`
- `reproducibleFileOrder=true`
- `options.encoding=UTF-8`
- `--release 21`
- Manifest carries pinned `Implementation-*` / `Specification-*` attributes.
- The Gradle wrapper records `distributionSha256Sum`.

Re-running `./gradlew clean :verifyReleaseArtifacts -PreleaseVersion=1.0.0`
on the same JDK should produce byte-identical jars.
