# verl-multi-task development rules

This independent repository contains the `multi_task_scheduler` Python package.
It is not the documentation directory of the outer repository.

- Keep the existing Git history/configuration. Do not commit or push without a user request.
- P1 provides native-entry integration, GS discovery and full relevant entity wiring; real-runtime acceptance remains pending.
- P2 names the pinned integration-plus-registration code/test snapshot, pending real-runtime validation; it does not revive the retired P2/SPI plan or mark P1 passed. Record full SHAs in handoff/progress documents, not moving branch HEADs.
- The user validates pinned P1 commits and returns evidence before P1 fixes. Feature planning may proceed in parallel; planning does not authorize implementation.
- Preserve the pinned P1 baseline. Isolate feature experiments from P1 deployments and their named GS; do not rewrite existing commits.
- P1 uses actual native subclasses/composition and preserves native training behavior.
- Necessary native entry/profile wiring is allowed; do not modify original verl class implementations or restore Impl/SPI patches.
- The earlier P1 cleanup removed unnecessary protocols, dummy services and tests; that cleanup is not continuing deletion authority.
- Do not remove unrelated documents, Git metadata, environments or historical P0 backups under this cleanup authorization.
- Keep package imports dependency-light and free of Ray initialization, actor discovery, global registration and monkey patches.
- An unimplemented runtime adapter must fail explicitly, never fall back to a fake/native class and pretend integration succeeded.
- New business features remain empty in the P1 baseline. Implement later features only within a separately approved phase; do not restore unused scaffolding.
- GS discovery and entity creation are real, not empty. Retain only simple state/handles and interfaces needed by this startup chain.
- Reports to GroupScheduler contain metadata only. Donor runtime handles must never enter borrower placement contracts.
- Resource registration runs after native initialization/first sync/optional validation and before fit. Store observed native rollout topology and training nodes only; do not infer idle GPUs or sharing permission.
- Keep one canonical task resource record and derive read-only views. An unconfirmed registration must prevent fit; only a registration timeout may retry the identical payload once.
- The manager owns local replica runtime references. Checkpoint Engine and load balancing hold separate projections.
- Preserve the single `experimental_fully_async_standalone` profile and the native verl training entry.
- Do not add a companion training entry, mirrored Hydra primary, class-FQN configuration, or old Impl/SPI dependency.
- Use `apply_patch` for source/document edits. Do not overwrite user changes.
- Use a project virtual environment and `uv` for environment management; install only the dependencies needed by selected tests.
- Tests target native entry selection, actual GS discovery, subclass construction/wiring and native delegation. Label mocked coverage explicitly.
- Selected real dependency tests may use a source copy inside verl. Never claim GPU/native-runtime success from AST or mocks.
- Source deployment must preserve unknown files, record provenance and never copy Git, environments or model data. Do not build a copy framework for this stage.
- Document each component's owner, creation point, inherited behavior and the boundary of unimplemented features.
- Report test results by layer. Do not disguise a skipped or mocked integration test as a successful runtime check.
