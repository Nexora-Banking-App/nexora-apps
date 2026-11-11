## What changed and why

<!-- One or two sentences. Link the issue or incident if there is one. -->

## How it was checked

- [ ] `scripts/sync-common.sh --check` passes (shared module identical in every service)
- [ ] Unit tests pass: `pytest tests/unit -v`
- [ ] If an API contract changed, the calling service and the README are updated
- [ ] No secret, token or password is logged or committed

## Risk and rollback

<!-- What could break, and how to undo it (usually: revert this PR). -->
