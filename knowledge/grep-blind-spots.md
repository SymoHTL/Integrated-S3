---
name: grep-blind-spots
description: Text search misses real code here - CorrelationIdValidationTests.cs holds a raw NUL byte, so git, grep and ripgrep treat it as binary, and five test files declare private classes named like the production ScopeBasedIntegratedS3AuthorizationService. Use grep -a, and check the path of a class-name hit.
metadata:
  type: reference
---

**The NUL byte.** `IntegratedS3.Tests/CorrelationIdValidationTests.cs:86` has a literal NUL
inside an `[InlineData]` string. It is the only tracked text file with one, and the repo has no
`.gitattributes`. As a result:

- `git grep` and `grep` print only "Binary file … matches";
- `git diff --numstat` shows `-  -`, so PR #176 (4126751), which added the file, shows no lines;
- ripgrep-based search tools skip the file entirely, so a search for
  `GetOrCreateCorrelationId_MissingHeader_UsesServerGeneratedId` finds nothing, although the test
  exists.

Use `grep -a` (or `rg -a`) when that file should match. The fix is to write `\0` instead of the raw
byte (#270).

**Same-named classes.** Production has `internal sealed class ScopeBasedIntegratedS3AuthorizationService`
in `IntegratedS3.Core/Services`. Five test files declare a private class with the same name:
`IntegratedS3HttpEndpointsTests`, `IntegratedS3SigV4ConformanceTests`,
`IntegratedS3AwsSdkCompatibilityTests`, `IntegratedS3AwsSdkEscapedPathCompatibilityTests` and
`IntegratedS3CoreOrchestrationTests`. Check the file path before trusting a class-name hit.

**The big file.** `IntegratedS3EndpointRouteBuilderExtensions.cs` is over 600 KB and 12,550 lines. Grep
it, then read line ranges; do not read it whole.

**Why:** a search that returns nothing is read as "no such code", and that is how a caller or a test
gets missed ([[subresource-needs-every-registration-point]]).

**How to apply:** when a search for something that must exist comes back empty, repeat it with
`grep -a`, and check the hit's path.

Gate: none. #270 tracks a check for control characters in tracked text files.
