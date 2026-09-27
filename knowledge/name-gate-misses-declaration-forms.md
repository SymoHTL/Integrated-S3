---
name: name-gate-misses-declaration-forms
description: A source gate that finds copies by their declared name misses every declaration form it does not match - each of the six verification passes over #303 compiled a copy that SharedSourceConventionTests passed, from a tuple literal's element to a class named after a shared method and one that only a build with preprocessor symbols compiles. Check every gated name against every kind the gate knows, parse every preprocessor view, and put a compiled probe of each form through it.
metadata:
  type: reference
---

**What happened.** `SharedSourceConventionTests` (#303) fails when a file outside
`src/IntegratedS3/Shared/` declares a type, member, parameter or variable with one of the shared
names. The compiler does not catch a copy (a private one shadows the linked one and builds with 0
warnings), so the gate is the only check. Each of its six verification passes compiled a copy that
it passed:

- pass 1: a source folder named `bin` or `obj` below a project, code under an inactive `#if`, a
  delegate, and a property, field or local holding a lambda;
- pass 2: a parameter, a record's positional parameter, an event, a pattern or `out` variable, a
  tuple element, a `foreach` variable and an anonymous-type member, and it left two forms as
  limits: a declaration split by a nested `#if` inside an active one, and a file outside
  `src/IntegratedS3/` linked into a project;
- pass 3: a tuple literal's element, query `from` and `let` variables, two declarations split by
  `#if`, and a `.cs` file under the project's own `obj/` named by a `<Compile>` item;
- pass 4: a class or struct named after a shared method, and a method, field or local function
  named after the shared type, because each name was checked against its own kind only; and a file
  a `<Compile>` item names without a `.cs` extension;
- pass 5: a class that a comment opened in an `#else` branch hides from a parse with no symbols,
  and a method that a `#region` line splits inside an inactive branch, because the gate read each
  file once with no symbols and each inactive run on its own, which also left passes 2 and 3's
  `#if` splits as limits. It also read that on Linux, where CI runs, the scan skipped a folder
  named `shared` by a case-insensitive match and never counted it;
- pass 6: a second definition under `Shared/` that only a build with symbols compiles, because the
  check that `Shared/` declares each name exactly once still parsed each file once with no
  symbols.

**Why:** a declaration kind the gate does not match, and a file it does not read, are holes that
no build, test or review shows.

**How to apply:** when you add a name to a name gate, or write a new one, check every name against
every kind the gate knows, and put a compiled probe of each form through the gate before calling it
done. Parse each file once for every combination of the preprocessor symbols its `#if` and `#elif`
directives name, so that every build's view is read whole. A renamed copy stays invisible to any
name gate: list it as HAZARD beside the rule.

Gate: `SharedSourceConventionTests`, for the forms and files it matches; HAZARD (#307) for the
rest.
