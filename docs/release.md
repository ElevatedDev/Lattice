# Release Process

Maintainer recipe for cutting a Lattice release.

## Pre-Release Checklist

- [ ] Public-API changes reviewed against `docs/api.md` and the stable-surface
      list in `CONTRIBUTING.md`.
- [ ] `CHANGELOG.md` has a section for the new version with a date.
- [ ] `docs/compatibility-matrix.md` reflects current JDK / OS support.
- [ ] Generated Javadoc builds cleanly, reflects the public surface, and is
      refreshed with `./gradlew docsJavadoc`.
- [ ] README and supporting docs are reviewed for current Maven coordinates,
      native packaging behavior, benchmark caveats, and trust evidence.
- [ ] Production-use wording remains conservative: Lattice is used in
      production for drone telemetry workloads, with no customer or scale
      claim unless separate approval/evidence exists.
- [ ] `./gradlew releaseCheck` passes locally on Linux and Windows.
- [ ] JaCoCo coverage report is reviewed, and
      `./gradlew jacocoTestCoverageVerification` passes the 80% line /
      65% branch release-scope floor.
- [ ] `./gradlew jcstress` passes (long-running; nightly CI is acceptable).
- [ ] Native backend tests pass, and native backend builds on Linux, Windows,
      and macOS release targets before publishing a jar that advertises bundled
      native loading. Capability bits remain platform-specific; Linux is the
      full affinity/NUMA target.
- [ ] Benchmark summary recorded under `docs/benchmark-results/<version>/`;
      raw JMH JSON/JFR artifacts and generated figures are attached to the
      release evidence if numbers are cited publicly.
- [ ] CodeQL, OpenSSF Scorecard, Dependency Review, Gradle wrapper validation,
      SBOM generation, checksum generation, and artifact attestation complete.

## Cut The Release

```bash
# 1. Start from the release branch
git switch <release-branch>
git pull --ff-only origin <release-branch>

# 2. Verify locally
./gradlew clean releaseCheck -PreleaseVersion=1.0.0
./gradlew nativeTest

# 3. Review and stage the exact release contents
git status --short
git add README.md CHANGELOG.md SECURITY.md CONTRIBUTING.md docs/ build.gradle \
  gradle.properties native/static-topology-native/ .github/
git status --short

# 4. Commit and tag
git commit -m "release: 1.0.0"
git tag -s v1.0.0 -m "Lattice 1.0.0"

# 5. Push the release commit and tag
git push origin <release-branch> v1.0.0
```

The `Release Artifacts` workflow builds the release bundle:

1. Tests and builds the platform native libraries.
2. Stages them under `META-INF/native/lattice/<os>-<arch>/`.
3. Builds the Maven artifacts (`jar`, `sourcesJar`, `javadocJar`) with
   `-PreleaseVersion=1.0.0`.
4. Runs release checks, coverage verification, SBOM generation, checksum
   generation, and artifact attestation.
5. Uploads the staged artifact bundle for maintainer inspection.

Publishing to Maven Central is explicit: run the workflow manually from the
`v1.0.0` tag ref with `publishToCentral=true` after the tag build has passed
and the `release` environment approval is granted. The workflow refuses Central
publishing from non-tag refs so a manual dispatch cannot publish
`1.0-SNAPSHOT`. It runs `publishToMavenCentral`, which uploads a validated
deployment to Central Portal for manual publication.

## Required Repository Secrets

| Secret | Purpose |
| --- | --- |
| `CENTRAL_USERNAME` / `CENTRAL_PASSWORD` | Sonatype Central Portal user-token credentials, exported to Gradle as `mavenCentralUsername` / `mavenCentralPassword`. |
| `SIGNING_KEY` | ASCII-armored PGP private key. |
| `SIGNING_KEY_ID` | Optional 8- or 16-char key id, exported as `signingInMemoryKeyId`. |
| `SIGNING_PASSWORD` | Passphrase for the signing key, exported as `signingInMemoryKeyPassword`. |

## Post-Release

- [ ] Verify the artifact on Maven Central after propagation.
- [ ] Verify a clean Gradle consumer resolves
      `io.github.elevateddev:lattice:1.0.0` from Maven Central.
- [ ] Verify bundled native extraction/loading on Linux, Windows, and macOS, and
      verify `-Dlattice.native.enabled=false` and
      `-Dlattice.native.library.path=...` still override the bundled path.
- [ ] Verify GitHub Pages serves `api/latest/`.
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
