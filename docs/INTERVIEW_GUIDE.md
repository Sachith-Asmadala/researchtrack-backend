# ResearchTrack backend: code learning and interview guide

Reviewed snapshot: `dd48375a134040d04193066992acd9a9ee49b8a7`. This source guide focuses on GitHub event processing and Kafka observability. It does not certify deployment availability, test success, throughput, or ownership of all services.

## Problem and architecture

ResearchTrack supports student research-project coordination and supervisor review. Its C#/.NET backend contains Auth, Project, GitHub, Jira, Meeting, and Submission services behind a YARP gateway. EF Core persistence and MySQL provider configuration appear in service projects; shared building blocks provide API/security/observability conventions. Each service has its own startup and persistence configuration. Kafka is an optional transport path, not proof that every local request passes through a broker.

The repository is a fork with substantial code and infrastructure. Learn one vertical feature well before attempting to explain every service or claiming the whole architecture as personal work.

## Startup and component communication

[Gateway Program.cs](https://github.com/Sachith-Asmadala/researchtrack-backend/blob/dd48375a134040d04193066992acd9a9ee49b8a7/src/Gateway/ResearchTrack.Gateway/Program.cs) configures YARP routes, allowed frontend origins, rate limiting, trusted forwarding, and metrics. It forwards business requests to services; it is not the owner of their domain databases.

[GitHubService Program.cs](https://github.com/Sachith-Asmadala/researchtrack-backend/blob/dd48375a134040d04193066992acd9a9ee49b8a7/src/Services/ResearchTrack.GitHubService/Program.cs) registers API conventions, JWT authentication, GitHub persistence/features, and webhook Kafka messaging before building the web application. Kafka options determine whether the optional messaging components are active. The API exposes Prometheus metrics; that is instrumentation code, not evidence that a live Prometheus server is scraping it.

| Code area | Responsibility |
| --- | --- |
| `src/Services/ResearchTrack.GitHubService/Controllers/GitHubWebhooksController.cs` | Receive GitHub event requests |
| [GitHubWebhookIngressService.cs](https://github.com/Sachith-Asmadala/researchtrack-backend/blob/dd48375a134040d04193066992acd9a9ee49b8a7/src/Services/ResearchTrack.GitHubService/Features/Webhooks/GitHubWebhookIngressService.cs) | Bound payloads, validate headers/signature/JSON, create delivery envelope |
| `Features/Webhooks/GitHubWebhookDeliveryStore.cs` | Persist deliveries, deduplicate, and optionally create an outbox record |
| `Features/Webhooks/GitHubWebhookEventProcessor.cs` | Convert supported events into processing/synchronization plans |
| `Features/Synchronization/RepositorySyncQueue.cs` | Coalesce in-process repository synchronization requests |
| [KafkaMessagingWorkers.cs](https://github.com/Sachith-Asmadala/researchtrack-backend/blob/dd48375a134040d04193066992acd9a9ee49b8a7/src/BuildingBlocks/Kafka/KafkaMessagingWorkers.cs) | Publish outbox messages, process records, commit offsets, sample health |
| [KafkaMetrics.cs](https://github.com/Sachith-Asmadala/researchtrack-backend/blob/dd48375a134040d04193066992acd9a9ee49b8a7/src/BuildingBlocks/Kafka/KafkaMetrics.cs) | Define bounded-label counters, gauges, and duration histograms |

Paths beginning `Features/` in the table are relative to `src/Services/ResearchTrack.GitHubService/`.

## Walkthrough: accept a GitHub event and observe the Kafka handoff

1. `GitHubWebhookIngressService.AcceptAsync()` validates delivery/event headers, reads the body with a configured byte limit, verifies the signature, and parses a JSON object. It calculates a SHA-256 payload hash and constructs an envelope with delivery/installation/repository identifiers.
2. `GitHubWebhookDeliveryStore.AcceptAsync()` checks whether the delivery ID already exists. A matching replay returns a duplicate result; inconsistent reuse of an ID is checked by `EnsureSamePayload()`.
3. For an enabled Kafka path with a supported event and installation identifier, the store adds both the delivery and `KafkaOutboxMessage` before one `SaveChangesAsync()`. This avoids accepting a delivery while forgetting the work needed to publish it.
4. `KafkaOutboxWorker.PublishOneAsync()` claims an outbox row, publishes it, then marks it complete. Only after successful bookkeeping does it call `metrics.Published()`. A publication error records a bounded failure category and retains failure/retry state.
5. A consumer calls `KafkaRecordProcessor` and the service event handler for durable handoff. `KafkaConsumerWorker.HandleAndCommitAsync()` commits the Kafka offset only after processing returns. Commit failure has its own metric/exception; it is distinct from a failed handoff.
6. `KafkaMetrics` exposes separate publish, process, commit, retry, duration, and outbox-depth signals. An acknowledged publication does not mean that a remote GitHub repository synchronization is complete.
7. Service workers and synchronization logic do the later work. The in-process `RepositorySyncQueue` prevents many identical requests from producing unlimited duplicate concurrent work for the same linked repository, with a rerun flag for changes arriving during a running sync.

This describes the enabled Kafka branch. The durable webhook worker path also exists when Kafka is not used. The guide does not send a webhook, contact a broker, or execute a deployment.

## Data structures and engineering choices

Envelopes and ORM records separate input transport from persisted processing state. Delivery identifiers and hashes support idempotency: safely recognizing repeated delivery attempts. An outbox is a database-backed list of messages waiting to publish. A receipt records durable consumer handoff; it is not a promise of exactly-once external side effects.

`RepositorySyncQueue` uses a `Channel<RepositorySyncWorkItem>` with one reader and a `ConcurrentDictionary<Guid, SyncQueueState>`. Per-state locks protect transitions such as running/rerun-requested. The channel itself is in memory and unbounded; it is not the durable delivery store. Shutdown/restart recovery therefore depends on the surrounding persisted delivery/reconciliation design.

Counters represent accumulating event totals; gauges represent current values such as pending outbox depth; histograms record duration distributions. Metric labels come from a small controlled vocabulary such as service/stage/outcome. Repository IDs, delivery IDs, and raw error text would create excessive time series and may disclose data, so they are not used as metric labels.

## Errors, limitations, and useful improvements

Ingress returns controlled errors for bad headers/JSON, invalid signatures, and oversized bodies. Body size is checked while reading, so omitting Content-Length does not bypass the limit. Delivery persistence handles duplicate-key races. Workers distinguish transient/permanent publication failures, retries, processing failures, and offset-commit failures.

Distributed work can replay after a crash between a side effect and its acknowledgement. Idempotency and reconciliation matter; do not call the system end-to-end exactly-once. In-memory queues can lose queued work on restart and can grow with many distinct repositories. Metrics can be stale if sampling fails, which is why the implementation also tracks sampling success/timestamp. An “up” API does not prove integrations, broker, databases, or storage are usable.

Focused next work should follow evidence: failure-injection tests around restart/duplicate events and commit failures, queue-pressure measurement, and an operational recovery exercise in a dedicated test environment. Existing [KafkaMetricsTests.cs](https://github.com/Sachith-Asmadala/researchtrack-backend/blob/dd48375a134040d04193066992acd9a9ee49b8a7/tests/ResearchTrack.GitHubService.Tests/KafkaMetricsTests.cs) verifies actual worker/metric semantics with test doubles and test storage, including failed publication bookkeeping, duplicate handoffs, and offset failures. Other GitHub tests cover ingress and queue behavior. They were read, not rerun for this guide.

## Contribution evidence

GitHub attributes [commit 911cd5a](https://github.com/Sachith-Asmadala/researchtrack-backend/commit/911cd5a40bfca0c9c5a8fb7151390e2a8c206e2b) to `Sachith-Asmadala`. The diff covers Kafka metrics, workers, tests, and monitoring-related files. That supports a focused discussion of those changes, subject to confirming your role and assistance. It does not establish that you independently designed or implemented all six microservices or deployed them successfully.

## Five interview questions and model answers

1. **What problem does an outbox solve?** It stores the delivery and the intent to publish in one database transaction. A worker can retry publication later, reducing the gap between accepting work and notifying a broker.
2. **Why separate process and commit metrics?** Durable handoff can succeed while committing the broker offset fails. Replay may then occur. Separate metrics show the failure stage without incorrectly counting it as a failed business handoff.
3. **Why not label metrics by repository or delivery ID?** Those values create an unbounded number of time series and can disclose identifiers. The implementation keeps metric labels bounded and uses other diagnostic mechanisms for specific events.
4. **Is the repository sync channel durable?** No. It is an in-memory channel with state used to coalesce requests. Durable delivery/outbox records and recovery logic are separate components.
5. **Does a successful publish prove synchronization completed?** No. Publication acknowledgement, durable handoff, offset commit, and remote synchronization are different stages. The code and tests intentionally distinguish them.

## Practice

Draw delivery → database/outbox → publisher → consumer/handoff → synchronization. Mark which steps survive process restart. Explain why one event may be observed more than once and which metric would change after an offset-commit failure.
