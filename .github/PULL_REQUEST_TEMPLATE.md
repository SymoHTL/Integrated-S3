# Summary

<!-- What does this PR change and why? Link the issue it addresses. -->

Fixes #

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Documentation
- [ ] CI / build / packaging
- [ ] Refactoring (no behavior change)

## Checklist

- [ ] `dotnet build src/IntegratedS3/IntegratedS3.slnx` passes with zero warnings (warnings are errors)
- [ ] `dotnet test src/IntegratedS3/IntegratedS3.slnx` passes
- [ ] New/changed behavior is covered by tests, and each new test was seen failing without the change (say where below)
- [ ] AOT/trimming compatibility preserved: `heavy` dispatched and green when the CI section of `CLAUDE.md` asks for it (it runs `eng/Invoke-AotPublishValidation.ps1`)
- [ ] Documentation updated where affected (`README.md`, `docs/`, XML doc comments)
- [ ] Provider capability matrix (`docs/protocol-compatibility.md`) updated if provider support changed
- [ ] `CHANGELOG.md` `Unreleased` section updated for user-visible changes
- [ ] Verification pass run at the head sha; each finding fixed or answered below (`CLAUDE.md`, Git & PRs)

## Notes for reviewers

<!-- Anything that needs special attention: design trade-offs, follow-ups, migration notes. -->
