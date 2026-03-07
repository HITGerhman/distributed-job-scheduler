# JobInstance State Machine

## States

- `PENDING`: instance has been created and is waiting for a worker.
- `RUNNING`: instance has been accepted by a worker and started.
- `SUCCESS`: worker finished execution with exit code `0`.
- `FAILED`: worker finished execution with non-zero exit code or runtime error.
- `KILLED`: instance was terminated before normal completion (manual kill/timeout/cancel).

## Allowed Transitions

- `PENDING -> RUNNING`
- `PENDING -> SUCCESS`
- `PENDING -> FAILED`
- `PENDING -> KILLED`
- `RUNNING -> SUCCESS`
- `RUNNING -> FAILED`
- `RUNNING -> KILLED`

## Invalid Transitions

The following transitions must be rejected as invalid state moves:

- Any transition from terminal states (`SUCCESS`, `FAILED`, `KILLED`) to another state.
- `RUNNING -> PENDING`.

## Ownership

- Master is responsible for creating `PENDING` instances.
- Worker execution updates `RUNNING` and sends `ReportResult`.
- Worker may send repeated `RUNNING` heartbeats for the same attempt.
- Master validates transition legality before persisting final state.
- Master may move a failed/timed-out attempt back to `PENDING` internally to schedule a retry.
- `KILLED` is terminal and must not be retried automatically.

## Persistence Mapping

`job_instances.status` stores one of:

- `PENDING`
- `RUNNING`
- `SUCCESS`
- `FAILED`
- `KILLED`
