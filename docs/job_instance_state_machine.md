# JobInstance State Machine

## States

- `PENDING`: instance has been created and is waiting for a worker.
- `RUNNING`: instance has been accepted by a worker and started.
- `SUCCESS`: worker finished execution with exit code `0`.
- `FAILED`: worker finished execution with non-zero exit code or runtime error.
- `KILLED`: instance was terminated before normal completion (manual kill/timeout/cancel).

## Allowed Transitions

- `PENDING -> RUNNING`
- `PENDING -> KILLED`
- `RUNNING -> SUCCESS`
- `RUNNING -> FAILED`
- `RUNNING -> KILLED`

## Invalid Transitions

The following transitions must be rejected as invalid state moves:

- Any transition from terminal states (`SUCCESS`, `FAILED`, `KILLED`) to another state.
- `PENDING -> SUCCESS` / `PENDING -> FAILED` (must run first).
- `RUNNING -> PENDING`.

## Ownership

- Master is responsible for creating `PENDING` instances.
- Worker execution updates `RUNNING` and sends `ReportResult`.
- Master validates transition legality before persisting final state.

## Persistence Mapping

`job_instances.status` stores one of:

- `PENDING`
- `RUNNING`
- `SUCCESS`
- `FAILED`
- `KILLED`
