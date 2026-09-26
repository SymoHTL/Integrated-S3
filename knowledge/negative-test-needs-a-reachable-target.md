---
name: negative-test-needs-a-reachable-target
description: A "must not" test proves nothing unless its input gets past validation and the forbidden target exists. In #298 two path-traversal cases resolved to an empty drive root and a delete case never reached the path it named, so removing the guard left every test green. Plant the target where the escape lands and use inputs the validator accepts.
metadata:
  type: project
---

Two misses in one PR (#298, the `IBlobStore` contract and `LocalDiskBlobStore`):

- **The traversal cases hit nothing.** The contract suite asserted that `OpenReadAsync("../outside")`
  and `OpenReadAsync("..\outside")` throw `BlobNotFoundException`. The store builds a path as
  `Path.Combine(root, locator[..2], locator[2..4], locator)`, and `locator[2..4]` is `/o`, a rooted
  segment, so `Path.Combine` starts over there and the path lands on the drive root, where no
  `outside` file exists. The first verification pass, at c0bf1ca, removed the locator check from
  `OpenReadAsync` in one mutant and from `DeleteAsync` in another: each left all 34 blob store tests
  green (its run of 49 also held 15 convention tests), while `abcd/../../../../outside.txt` reached
  the root's parent and could read or delete a file there.
- **The delete case never reached its path.** "Deleting a locator the store never issued succeeds"
  used malformed locators, which the validator rejects before any file system call. A mutant that
  let `DirectoryNotFoundException` escape from `File.Delete` passed the whole contract suite; only
  a store-specific test with a well-formed locator whose directories do not exist caught it.

**Why:** a negative assertion ("throws not-found", "the file is unchanged", "nothing is listed")
passes both when the guard works and when the input never got near the guarded code, or when the
thing it must not touch does not exist. Nothing in the report tells the two apart.

**How to apply:** for every negative test, name the guard it proves and make the mutant that
removes the guard fail it. Use inputs the validator accepts, so the guarded path runs. Create the
forbidden target where the escape would land (a file beside the root, a live blob next to the
variant), and assert afterwards that it is intact. Record the mutation in the PR, as #298 does.

Gate: `LocalDiskBlobStoreContractTests.EscapingLocators_NeitherReadNorDeleteAFileOutsideTheRoot`
and `LocalDiskBlobStoreContractTests.Delete_OfAWellFormedLocatorWhoseDirectoriesDoNotExist_Succeeds`,
both tests of `LocalDiskBlobStore`, so they gate these two cases in that store only. HAZARD for
other stores and other negative tests (#270): no tool checks that a negative test can fail.
