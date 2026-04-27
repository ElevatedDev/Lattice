# Release Process

Maintainer recipe for cutting a Lattice release.

## Pre-Release Checklist

- [ ] Public-API changes reviewed against `docs/api.md` and the stable-surface
      list in `CONTRIBUTING.md`.
- [ ] `CHANGELOG.md` has a section for the new version with a date.
- [ ] `docs/compatibility-matrix.md` reflects current JDK / OS support.
- [ ] Generated Javadoc builds cleanly and reflects the public surface.
- [ ] `./gradlew releaseCheck` passes locally on Linux *and* Windows.
- [ ] `./gradlew jcstress` passes (long-running; nightly CI is acceptable).
- [ ] Native backend builds on Linux + macOS + Windows runners.
- [ ] Benchmark snapshot recorded under `docs/benchmark-results/<version>/`.

## Cut The Release

```bash
# 1. Bump version
echo "releaseVersion=1.0.0" >> gradle.properties

# 2. Tag
git commit -am "release: 1.0.0"
git tag -s v1.0.0 -m "Lattice 1.0.0"
git push origin main v1.0.0
```

The release workflow should take over from here:

1. Builds the Maven artifacts (`jar`, `sourcesJar`, `javadocJar`) with
   `-PreleaseVersion=1.0.0`.
2. Signs every artifact with the configured GPG key.
3. Publishes to the Sonatype Central Portal.
4. Builds native libraries on Linux / macOS / Windows runners and attaches
   them to the GitHub Release as `libstatic_topology_native-<os>-<arch>.zip`.
5. Generates GitHub Release notes from the matching `CHANGELOG.md` section.
6. Builds Javadoc and publishes to GitHub Pages under `api/1.0.0/` and
   `api/latest/`.

## Required Repository Secrets

| Secret | Purpose |
| --- | --- |
| `OSSRH_USERNAME` / `OSSRH_PASSWORD` | Sonatype Central Portal credentials. |
| `SIGNING_KEY` | ASCII-armored PGP private key. |
| `SIGNING_KEY_ID` | 8- or 16-char key id. |
| `SIGNING_PASSWORD` | Passphrase for the signing key. |

## Post-Release

- [ ] Verify the artifact on Maven Central (≈30 min propagation).
- [ ] Verify GitHub Pages serves `api/1.0.0/`.
- [ ] Open a PR that bumps `gradle.properties` back to a `-SNAPSHOT` line
      for the next development cycle.
- [ ] Announce in `Discussions → Announcements`.

## Reproducibility

The build is configured for reproducible archives:

- `preserveFileTimestamps=false`
- `reproducibleFileOrder=true`
- `options.encoding=UTF-8`
- `--release 21`
- Manifest carries pinned `Implementation-*` / `Specification-*` attributes.

Re-running `./gradlew clean :verifyReleaseArtifacts -PreleaseVersion=1.0.0`
on the same JDK should produce byte-identical jars.
