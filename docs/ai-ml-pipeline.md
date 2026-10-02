# DriveX AI/ML Subsystem & Vector Search Master Specification

**Document Version**: 1.0.1-REVISED  
**Status**: Revised Technical Specification (review required before production)  
**Target Architecture**: DriveX Distributed Cloud Storage & AI Intelligence Platform  
**Target Services**: `drivex-ml-workers`, `drivex-ml-api`, `drivex-rabbitmq`, `drivex-qdrant`  
**Classification**: Production Engineering Architecture & Implementation Standard  

---

## 1. Executive Summary & AI/ML Architectural Philosophy

DriveX transforms conventional cloud object storage into an active, intelligent content management and discovery platform. While traditional cloud drives treat files as passive byte streams, DriveX treats every uploaded document, image, and text artifact as an indexed semantic corpus.

The Machine Learning and Artificial Intelligence (AI/ML) subsystem is engineered around four non-negotiable architectural tenets:

### 1.1 Asynchronous Event-Driven Decoupling
Heavy computational tasks—optical character recognition (OCR), multi-format document parsing, semantic text chunking, high-dimensional vector embedding generation, perceptual image hashing, and deduplication—must **never execute on the synchronous request/response path** of user uploads. 
When a client completes an upload transaction via the C++ Drogon Control Plane (`POST /api/v1/files/upload-complete`), the Drogon API validates the S3 object in MinIO, commits the metadata transaction to MySQL 8, and immediately publishes an AMQP 0-9-1 event to RabbitMQ. The endpoint should return after durable metadata commit and reliable event publication (or transactional outbox commit). Latency targets such as p99 < 25 ms are service-level objectives to validate under representative load, not guaranteed properties.

### 1.2 Zero-Copy Storage Stream Consumption
Worker processes must never require intermediate proxy services to read document binaries. Ingestion workers authenticate directly against the MinIO S3 cluster using internal cluster-local IAM credentials, streaming byte payloads into memory or bounded disk caches via chunked HTTP/S3 streams. Workers stream or process bounded inputs where supported and enforce file-size, page-count, decompression, and memory limits. Python libraries may buffer or copy data; memory exhaustion is mitigated rather than categorically prevented.

### 1.3 Multi-Tenant Isolation & Zero Data Leakage
DriveX operates in multi-user and enterprise multi-tenant environments. A fundamental vector search security risk in naive RAG systems is cross-tenant vector leakage, where vector similarity searches return passages belonging to other users or workspaces. DriveX enforces tenant isolation through authorization checks and mandatory server-side filters; this is a security invariant that must be tested, not a mathematical guarantee:
1. Every vector upserted into the Qdrant vector database is tagged with immutable payload attributes (`owner_id`, `workspace_id`, and `file_id`).
2. Vector searches require mandatory tenant and ACL filters supplied by the trusted API after authorization. Qdrant applies payload filters during search; exact execution details depend on the Qdrant version and query plan.
3. Chunks for which the caller lacks effective file/workspace access must be excluded before results are returned. Do not rely on vector filtering alone: resolve current ACLs in the authorization layer, fail closed on uncertainty, and re-check permissions before exposing snippets or citations.

### 1.4 Idempotency & Relational State Reconciliation
All asynchronous worker tasks are strictly idempotent. If a worker process is terminated abruptly (via OOM kill or container eviction), the unacknowledged AMQP message is redelivered. Worker tasks check the authoritative MySQL database state before applying transformations, handle duplicate deliveries gracefully, and transition the relational record status atomically (`PENDING` -> `INDEXED` or `FAILED`).

---

## 2. Component & Service Boundary Matrix

The following matrix defines the boundaries, responsibilities, runtime environments, and communication interfaces for all subsystems participating in the AI/ML pipeline.

| Subsystem Component | Service Container | Primary Technology | Transport Protocol | Core Responsibilities |
|---|---|---|---|---|
| **Control Plane Producer** | `drivex-api` | C++20 Drogon Framework | AMQP 0-9-1 TCP | Publishes `file.uploaded.#`, `file.deleted`, and `file.content_updated` events to RabbitMQ topic exchange upon upload completion. |
| **Message Broker** | `drivex-rabbitmq` | RabbitMQ 3.13 (Quorum) | AMQP 0-9-1 (5672) | High-availability message routing, topic-based event fanout, message queue persistence, and TTL retry orchestration. |
| **Asynchronous Worker Pool** | `drivex-ml-workers` | Python 3.11, Celery 5.4 | AMQP & S3 Wire | Pulls tasks from RabbitMQ, streams files from MinIO, parses text, executes OCR, chunks text, computes embeddings, and performs deduplication. |
| **Vector Database** | `drivex-qdrant` | Qdrant v1.9+ Rust Engine | gRPC (6334) / REST (6333) | Stores 1024-d chunk embeddings, maintains in-memory HNSW index, executes INT8 scalar quantization, and performs tenant-filtered vector searches. |
| **ML Inference Gateway** | `drivex-ml-api` | Python 3.11, FastAPI, Uvicorn | HTTP/1.1 REST & SSE | Exposes internal `/search` semantic query endpoint and `/chat` conversational RAG SSE streaming endpoint with cross-encoder re-ranking. |
| **Relational Metadata** | `drivex-mysql` | MySQL 8.0 (InnoDB) | MySQL Wire (3306) | Stores authoritative file records, `processing_status` state, SHA-256 digests, perceptual hashes (`phash`), and FULLTEXT name/metadata search indexes. |
| **Object Storage** | `drivex-minio` | MinIO Distributed S3 | S3 REST API (9000) | Immutable binary object store hosting the `drivex-blobs` bucket accessed directly by Celery workers via S3 `GetObject`. |
| **Distributed Cache & State** | `drivex-redis` | Redis 7.0 In-Memory DB | RESP3 TCP (6379) | Celery task state tracking (DB 1), HyDE query caching (DB 2), and token rate limit windows. |

---

## 3. Visual Architecture Blueprints

### 3.1 Asynchronous Event-Driven RabbitMQ & Celery ML Ingestion Pipeline

The following diagram illustrates the complete asynchronous lifecycle of an uploaded file, detailing the interaction between the C++ Drogon Control Plane, the RabbitMQ AMQP messaging fabric, progressive TTL retry queues, Celery worker task execution, MinIO object streaming, Qdrant vector indexing, and MySQL state finalization.

```mermaid
flowchart TD
    subgraph Control_Plane["C++ Drogon Control Plane"]
        Client["Client User-Agent"]
        MinIO["MinIO S3 Cluster<br/>(Bucket: drivex-blobs)"]
        DrogonAPI["C++ Drogon REST API<br/>POST /api/v1/files/upload-complete"]
        MySQL_Master[("MySQL 8.0 Primary<br/>Table: files (status=PENDING)")]
    end

    subgraph Messaging_Fabric["RabbitMQ AMQP 0-9-1 Messaging Topology"]
        TopicEx["Topic Exchange<br/>drivex.events"]
        RetryEx["Topic Exchange<br/>drivex.events.retry"]
        DLX["Dead-Letter Exchange<br/>drivex.events.dlx"]

        Q_Ingest["Quorum Queue: file.ingest<br/>Binds: file.uploaded.#, file.content_updated"]
        Q_OCR["Quorum Queue: file.ocr<br/>Binds: file.uploaded.image.#, *.pdf"]
        Q_Embed["Quorum Queue: file.embed<br/>Binds: file.extracted.text"]
        Q_Dedup["Quorum Queue: file.dedup<br/>Binds: file.uploaded.#"]

        Q_Retry30s["TTL Queue: retry.30s<br/>x-message-ttl: 30000ms<br/>DLX: drivex.events"]
        Q_Retry5m["TTL Queue: retry.5m<br/>x-message-ttl: 300000ms<br/>DLX: drivex.events"]
        Q_DLQ["Quorum Queue: file.dlq<br/>Binds: # on drivex.events.dlx"]
    end

    subgraph Celery_Worker_Tier["Celery Distributed Worker Pool"]
        WorkerIngest["Ingestion Worker<br/>tasks.ingest_file<br/>(Stream Validation)"]
        WorkerExtract["Extraction Worker<br/>tasks.extract_document<br/>(PyMuPDF, docx, chardet)"]
        WorkerOCR["OCR Worker<br/>tasks.extract_image<br/>(pdf2image + Tesseract)"]
        WorkerChunk["Chunking Engine<br/>tasks.semantic_chunk<br/>(512 tokens / 64 overlap)"]
        WorkerEmbed["Embedding Worker<br/>tasks.generate_embeddings<br/>(BAAI/bge-large-en-v1.5)"]
        WorkerDedup["Deduplication Worker<br/>tasks.dedup_check<br/>(SHA-256, pHash/dHash)"]
        WorkerFinalize["Finalization Worker<br/>tasks.finalize_ingestion<br/>(Atomic DB Update)"]
    end

    subgraph Storage_Tier["Storage & Indexing Tier"]
        Qdrant_DB[("Qdrant Vector DB<br/>Collection: drivex_file_chunks<br/>- 1024-d Cosine<br/>- HNSW (M=16, ef=128)<br/>- INT8 Scalar Quantization")]
        MySQL_Final[("MySQL 8.0 Primary<br/>Update files:<br/>- status = INDEXED<br/>- content_hash, phash")]
    end

    %% Upload & Control Flow
    Client -->|1. Direct Pre-signed PUT| MinIO
    Client -->|2. Upload Complete| DrogonAPI
    DrogonAPI -->|3. HeadObject & INSERT files| MySQL_Master
    DrogonAPI -->|4. Publish AMQP Event| TopicEx

    %% RabbitMQ Ingestion Fanout
    TopicEx -->|file.uploaded.#| Q_Ingest
    TopicEx -->|file.uploaded.image.#| Q_OCR
    TopicEx -->|file.uploaded.#| Q_Dedup

    %% Worker Pipeline Execution
    Q_Ingest --> WorkerIngest
    WorkerIngest -->|Stream S3 Bytes| MinIO
    WorkerIngest -->|Native Doc| WorkerExtract
    WorkerExtract -->|Extracted Text| WorkerChunk

    Q_OCR --> WorkerOCR
    WorkerOCR -->|Stream Raster Image| MinIO
    WorkerOCR -->|OCR Text| WorkerChunk

    WorkerChunk -->|Publish Extracted Text| Q_Embed
    Q_Embed -->|file.extracted.text| WorkerEmbed
    WorkerEmbed -->|Upsert 1024-d Vectors| Qdrant_DB
    WorkerEmbed --> WorkerFinalize

    Q_Dedup --> WorkerDedup
    WorkerDedup -->|Calculate pHash & dHash| WorkerFinalize

    WorkerFinalize -->|status=INDEXED| MySQL_Final

    %% Retry & Dead-Letter Flow
    WorkerIngest -.->|Transient Failure (Attempt 1)| RetryEx
    WorkerOCR -.->|Transient Failure (Attempt 1)| RetryEx
    WorkerEmbed -.->|Transient Failure (Attempt 1)| RetryEx
    RetryEx -->|retry.short.*| Q_Retry30s
    Q_Retry30s -.->|30s Expiry -> Re-route| TopicEx

    WorkerIngest -.->|Transient Failure (Attempt 2)| RetryEx
    RetryEx -->|retry.long.*| Q_Retry5m
    Q_Retry5m -.->|5m Expiry -> Re-route| TopicEx

    WorkerIngest -.->|Fatal / Retry Limit >= 3| DLX
    WorkerOCR -.->|Fatal / Retry Limit >= 3| DLX
    WorkerEmbed -.->|Fatal / Retry Limit >= 3| DLX
    DLX -->|Routing #| Q_DLQ
```

---

### 3.2 Conversational RAG Assistant Query & Retrieval Sequence Flow

The following sequence diagram details the end-to-end multi-stage pipeline of the conversational "Chat with Your Drive" assistant, demonstrating query analysis, HyDE expansion, parallel hybrid vector and lexical search, Reciprocal Rank Fusion, cross-encoder re-ranking, token budgeting, and Server-Sent Events (SSE) streaming token delivery.

```mermaid
sequenceDiagram
    autonumber
    actor Client as User-Agent (Web / Mobile)
    participant Nginx as Nginx (Edge Proxy & Ingress)
    participant Gateway as Drogon API / FastAPI ML Gateway
    participant LLM_HyDE as Internal LLM (HyDE Rewriter)
    participant Qdrant as Qdrant Vector Database
    participant MySQL as MySQL 8.0 (FULLTEXT Search)
    participant Reranker as Cross-Encoder (bge-reranker-large)
    participant LLM_Gen as Streaming LLM Generator

    Client->>Nginx: POST /api/v1/chat (query, conversation_id)
    Nginx->>Gateway: Forward Request (X-Accel-Buffering: no)
    Gateway->>Gateway: Verify RS256 JWT, Extract user_id & workspace_id

    rect rgb(240, 248, 255)
        note over Gateway,LLM_HyDE: Step 1: Query Analysis & HyDE Expansion
        Gateway->>LLM_HyDE: Expand query: generate hypothetical document passage
        LLM_HyDE-->>Gateway: Return hypothetical passage (h)
        Gateway->>Gateway: Formulate query_rich = query + "\n" + h
        Gateway->>Gateway: Compute 1024-d Dense Vector (BAAI/bge-large-en-v1.5)
    end

    rect rgb(245, 255, 245)
        note over Gateway,MySQL: Step 2: Parallel Hybrid Retrieval & Reciprocal Rank Fusion (k=60)
        par Dense Vector Search
            Gateway->>Qdrant: Vector similarity search (Top 50 candidates)<br/>Filter: (owner_id == user_id OR workspace_id) + Post-Filter: Redis/MySQL permissions
            Qdrant-->>Gateway: Return 50 vector points with Cosine scores
        and Sparse Lexical Keyword Search
            Gateway->>MySQL: MATCH(name, metadata) AGAINST(:query IN BOOLEAN MODE)<br/>Filter: Permissions & ownership (Top 50 candidates)
            MySQL-->>Gateway: Return 50 file records with BM25/TF-IDF scores
        end
        Gateway->>Gateway: Merge candidates via RRF: Score = 0.70/(60+r_vec) + 0.30/(60+r_lex)
        Gateway->>Gateway: Retain Top 50 consolidated candidate chunks
    end

    rect rgb(255, 250, 240)
        note over Gateway,Reranker: Step 3: Deep Cross-Encoder Re-Ranking
        Gateway->>Reranker: Score 50 (query, chunk_text) pairs via joint cross-attention
        Reranker-->>Gateway: Return relevance probability scores [0.0 - 1.0]
        Gateway->>Gateway: Discard chunks with score < 0.40 (noise suppression)
        Gateway->>Gateway: Select Top 5-10 high-precision chunks
    end

    rect rgb(255, 245, 255)
        note over Gateway,LLM_Gen: Step 4: Token Budgeting & Grounded Prompt Assembly
        Gateway->>Gateway: Enforce 4,096 token budget (System: 300, Chunks: 2500, History: 800)
        Gateway->>Gateway: Build anti-hallucination prompt with mandatory citation markers
    end

    rect rgb(240, 255, 255)
        note over Gateway,Client: Step 5: Server-Sent Events (SSE) Streaming
        Gateway-->>Client: event: sources<br/>data: {"sources": [{"file_id": 1024, "filename": "report.pdf", "page": 4, "chunk": 2}]}
        Gateway->>LLM_Gen: Dispatch streaming completion request
        loop Token Delta Streaming
            LLM_Gen-->>Gateway: Yield token delta
            Gateway-->>Client: event: message<br/>data: {"delta": "According to [Doc: report.pdf, Page: 4, Chunk: 2]..."}
        end
        LLM_Gen-->>Gateway: Generation complete (usage metrics)
        Gateway-->>Client: event: done<br/>data: {"status": "completed", "total_tokens": 142}
    end
```

---

## 4. RabbitMQ AMQP 0-9-1 Messaging Topology

The RabbitMQ messaging backbone coordinates the asynchronous event flow across the DriveX platform. All file lifecycle events published by the C++ Drogon Control Plane are broadcast onto durable topic exchanges, routed into fault-tolerant quorum queues, and protected by progressive TTL-based retry queues and dead-letter exchanges.

### 4.1 Exchange Architecture

DriveX declares three dedicated AMQP exchanges, each serving an isolated operational responsibility:

| Exchange Name | Exchange Type | Durability | Auto-Delete | Internal | Description |
|---|---|---|---|---|---|
| `drivex.events` | `topic` | `true` | `false` | `false` | The primary operational event bus. Receives all file lifecycle events (`file.uploaded.#`, `file.deleted`, `file.content_updated`) published by the Drogon Control Plane and internal worker completion events. |
| `drivex.events.retry` | `topic` | `true` | `false` | `false` | Delayed staging exchange. Receives failed event messages flagged for exponential backoff retry. Routes messages to dead-letter TTL queues. |
| `drivex.events.dlx` | `topic` | `true` | `false` | `false` | Terminal dead-letter exchange (DLX). Receives messages that have exhausted their maximum retry limit (3 attempts) or suffered unrecoverable fatal parsing failures. |

---

### 4.2 Routing Key Hierarchy & Semantic Conventions

Routing keys adhere to a strict hierarchical dot-notation scheme:
$$\text{domain}.\text{action}.\text{category}.\text{format}$$

The canonical routing keys and their subscription patterns are defined below:

1. **`file.uploaded.<mime_category>.<mime_subtype>`**:
   - `file.uploaded.application.pdf`: Standard PDF document uploads.
   - `file.uploaded.application.vnd.openxmlformats-officedocument.wordprocessingml.document`: Microsoft Word DOCX uploads.
   - `file.uploaded.text.plain`: Plain text or ASCII code files.
   - `file.uploaded.text.markdown`: Markdown documentation files.
   - `file.uploaded.image.jpeg`: Raster JPEG images requiring OCR and perceptual hashing.
   - `file.uploaded.image.png`: Raster PNG images requiring OCR and perceptual hashing.
   - `file.uploaded.image.webp`: WebP images requiring OCR and perceptual hashing.
2. **`file.deleted`**: Emitted when a file is soft-deleted to trash or permanently purged from storage. Triggers vector point removal in Qdrant and relational tag cleanup.
3. **`file.content_updated`**: Emitted when an existing logical file receives a new binary version (`file_versions`). Triggers re-chunking, re-embedding, and perceptual hash updating.
4. **`file.extracted.text`**: Internal worker-to-worker event emitted after text extraction or OCR succeeds, targeting the embedding queue.
5. **`file.retry.requeue`**: Internal routing key assigned by TTL retry queues when dead-lettering expired retry messages back into `drivex.events`.

---

### 4.3 Quorum Queues, Bindings & Arguments

DriveX utilizes **RabbitMQ Quorum Queues** exclusively for all mission-critical worker queues. Quorum queues employ the Raft consensus protocol, providing high data safety, leader election across clustered broker nodes, and protection against network partition data loss.

| Queue Name | Queue Type | Durability | Bound Exchange | Binding Pattern | Dead-Letter Exchange (`x-dead-letter-exchange`) | Dead-Letter Routing Key (`x-dead-letter-routing-key`) | Delivery Limit (`x-delivery-limit`) |
|---|---|---|---|---|---|---|---|
| `file.ingest` | `quorum` | `true` | `drivex.events` | `file.uploaded.#`, `file.content_updated`, `file.retry.requeue` | `drivex.events.dlx` | `dead.file.ingest` | 3 |
| `file.ocr` | `quorum` | `true` | `drivex.events` | `file.uploaded.image.#`, `file.uploaded.application.pdf` | `drivex.events.dlx` | `dead.file.ocr` | 3 |
| `file.embed` | `quorum` | `true` | `drivex.events` | `file.extracted.text` | `drivex.events.dlx` | `dead.file.embed` | 3 |
| `file.dedup` | `quorum` | `true` | `drivex.events` | `file.uploaded.#` | `drivex.events.dlx` | `dead.file.dedup` | 3 |
| `file.dlq` | `quorum` | `true` | `drivex.events.dlx` | `#` | *(None - Terminal)* | *(None)* | *(None)* |

#### Quorum Queue Configuration Arguments:
```json
{
  "x-queue-type": "quorum",
  "x-max-in-memory-length": 5000,
  "x-delivery-limit": 3,
  "x-dead-letter-exchange": "drivex.events.dlx",
  "x-overflow": "reject-publish"
}
```

---

### 4.4 Progressive TTL-Based Exponential Backoff Retry Architecture

Transient errors (e.g., temporary MinIO S3 network timeouts, Qdrant memory pressure, or database lock contention) must not trigger immediate failure or overwhelm downstream services with rapid retries. DriveX implements a progressive two-stage delayed retry mechanism using RabbitMQ message Time-To-Live (TTL) and dead-letter redirection:

```
[Worker Task Failure]
         |
         |-- (Attempt 1: Transient Error) --> Publish to 'drivex.events.retry' with key 'retry.short.file.ingest'
         |                                         |
         |                                         v
         |                                   Queue: 'retry.30s' (x-message-ttl: 30000ms)
         |                                         |
         |                                         v (TTL Expires after 30s)
         |                                   Dead-letters to 'drivex.events' with key 'file.retry.requeue'
         |                                         |
         |                                         v
         |                                   Re-consumed by Queue: 'file.ingest'
         |
         |-- (Attempt 2: Second Failure) ---> Publish to 'drivex.events.retry' with key 'retry.long.file.ingest'
         |                                         |
         |                                         v
         |                                   Queue: 'retry.5m' (x-message-ttl: 300000ms)
         |                                         |
         |                                         v (TTL Expires after 5m)
         |                                   Dead-letters to 'drivex.events' with key 'file.retry.requeue'
         |
         |-- (Attempt 3: Terminal Failure) -> basic.reject(requeue=false)
                                                   |
                                                   v
                                             Routes to 'drivex.events.dlx' -> Queue: 'file.dlq'
                                             Alert fires to Prometheus / PagerDuty
```

#### Detailed Retry Queue Parameters:

1. **`retry.30s` Queue Specification**:
   - `durable`: `true`
   - `x-message-ttl`: `30000` (30,000 milliseconds = 30 seconds)
   - `x-dead-letter-exchange`: `"drivex.events"`
   - `x-dead-letter-routing-key`: `"file.retry.requeue"`
   - Bindings: Exchange `drivex.events.retry` with routing pattern `retry.short.*`
2. **`retry.5m` Queue Specification**:
   - `durable`: `true`
   - `x-message-ttl`: `300000` (300,000 milliseconds = 5 minutes)
   - `x-dead-letter-exchange`: `"drivex.events"`
   - `x-dead-letter-routing-key`: `"file.retry.requeue"`
   - Bindings: Exchange `drivex.events.retry` with routing pattern `retry.long.*`
3. **`file.dlq` Dead-Letter Queue**:
   - Stores terminally failed messages wrapped in the `DeadLetterEnvelope` schema.
   - Consumed by operator alert daemons and an automated administrative redrive CLI.

---

### 4.5 Production Draft-07 JSON Event Schemas

All messages published to RabbitMQ must conform strictly to the following draft-07 JSON schemas. Malformed payloads that fail schema validation are rejected immediately without requeue and routed to `file.dlq` to prevent poison pill loops.

#### 4.5.1 `file.uploaded` Event Payload Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FileUploadedEvent",
  "type": "object",
  "required": [
    "event_id",
    "event_type",
    "schema_version",
    "timestamp",
    "file_id",
    "owner_id",
    "storage_key",
    "mime_type",
    "size_bytes",
    "sha256_checksum",
    "version_id",
    "trace_context"
  ],
  "properties": {
    "event_id": {
      "type": "string",
      "format": "uuid",
      "description": "Cryptographically secure UUIDv4 identifying this event instance."
    },
    "event_type": {
      "type": "string",
      "enum": [
        "file.uploaded"
      ]
    },
    "schema_version": {
      "type": "string",
      "enum": [
        "1.0.0"
      ]
    },
    "timestamp": {
      "type": "integer",
      "description": "Epoch timestamp in milliseconds when the upload was finalized."
    },
    "file_id": {
      "type": "integer",
      "minimum": 1,
      "description": "Primary key of the file record in MySQL files table."
    },
    "owner_id": {
      "type": "integer",
      "minimum": 1,
      "description": "User ID of the file owner."
    },
    "workspace_id": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Nullable tenant/workspace identifier for organizational multi-tenancy."
    },
    "folder_id": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Parent folder ID; null represents the user root directory."
    },
    "filename": {
      "type": "string",
      "maxLength": 255,
      "description": "Original user-provided filename including extension."
    },
    "storage_key": {
      "type": "string",
      "maxLength": 512,
      "description": "Canonical MinIO object key, e.g. 'blobs/42/2026-09/a1b2c3d4.pdf'."
    },
    "mime_type": {
      "type": "string",
      "maxLength": 127,
      "description": "IANA media type classification of the file."
    },
    "size_bytes": {
      "type": "integer",
      "minimum": 0,
      "description": "Exact byte length of the uploaded object verified via S3 HeadObject."
    },
    "sha256_checksum": {
      "type": "string",
      "pattern": "^[a-f0-9]{64}$",
      "description": "Cryptographic SHA-256 digest computed across file contents."
    },
    "version_id": {
      "type": "integer",
      "minimum": 1,
      "description": "Monotonically increasing version number from file_versions table."
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$",
          "description": "W3C Trace Context traceparent header for distributed tracing."
        },
        "tracestate": {
          "type": "string",
          "description": "Optional W3C vendor-specific state string."
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

#### 4.5.2 `file.deleted` Event Payload Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FileDeletedEvent",
  "type": "object",
  "required": [
    "event_id",
    "event_type",
    "schema_version",
    "timestamp",
    "file_id",
    "owner_id",
    "storage_key",
    "permanent_delete",
    "trace_context"
  ],
  "properties": {
    "event_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique UUIDv4 identifier for this event."
    },
    "event_type": {
      "type": "string",
      "enum": [
        "file.deleted"
      ]
    },
    "schema_version": {
      "type": "string",
      "enum": [
        "1.0.0"
      ]
    },
    "timestamp": {
      "type": "integer",
      "description": "Epoch timestamp in milliseconds of the deletion operation."
    },
    "file_id": {
      "type": "integer",
      "minimum": 1,
      "description": "MySQL file ID target of deletion."
    },
    "owner_id": {
      "type": "integer",
      "minimum": 1,
      "description": "User ID owning the target file."
    },
    "workspace_id": {
      "type": [
        "integer",
        "null"
      ],
      "description": "Nullable workspace identifier."
    },
    "storage_key": {
      "type": "string",
      "description": "MinIO object storage key to be deleted if permanent."
    },
    "permanent_delete": {
      "type": "boolean",
      "description": "True if permanently purged from trash; false if soft-deleted to trash."
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$"
        },
        "tracestate": {
          "type": "string"
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

#### 4.5.3 `file.content_updated` Event Payload Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FileContentUpdatedEvent",
  "type": "object",
  "required": [
    "event_id",
    "event_type",
    "schema_version",
    "timestamp",
    "file_id",
    "new_version_id",
    "previous_version_id",
    "owner_id",
    "storage_key",
    "mime_type",
    "size_bytes",
    "new_sha256_checksum",
    "trace_context"
  ],
  "properties": {
    "event_id": {
      "type": "string",
      "format": "uuid"
    },
    "event_type": {
      "type": "string",
      "enum": [
        "file.content_updated"
      ]
    },
    "schema_version": {
      "type": "string",
      "enum": [
        "1.0.0"
      ]
    },
    "timestamp": {
      "type": "integer"
    },
    "file_id": {
      "type": "integer",
      "minimum": 1
    },
    "new_version_id": {
      "type": "integer",
      "minimum": 2
    },
    "previous_version_id": {
      "type": "integer",
      "minimum": 1
    },
    "owner_id": {
      "type": "integer",
      "minimum": 1
    },
    "workspace_id": {
      "type": [
        "integer",
        "null"
      ]
    },
    "storage_key": {
      "type": "string"
    },
    "mime_type": {
      "type": "string"
    },
    "size_bytes": {
      "type": "integer",
      "minimum": 0
    },
    "new_sha256_checksum": {
      "type": "string",
      "pattern": "^[a-f0-9]{64}$"
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$"
        },
        "tracestate": {
          "type": "string"
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

#### 4.5.4 `file.dlq` Dead-Letter Metadata Envelope Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "DeadLetterQueueEnvelope",
  "type": "object",
  "required": [
    "dlq_id",
    "failed_at",
    "source_queue",
    "original_routing_key",
    "retry_count",
    "error_type",
    "error_message",
    "stack_trace",
    "original_payload",
    "trace_context"
  ],
  "properties": {
    "dlq_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique identifier assigned upon DLQ dead-lettering."
    },
    "failed_at": {
      "type": "integer",
      "description": "Epoch timestamp in milliseconds when the message was permanently dead-lettered."
    },
    "source_queue": {
      "type": "string",
      "description": "The worker queue in which the failure occurred (e.g. 'file.ingest')."
    },
    "original_routing_key": {
      "type": "string",
      "description": "The routing key originally assigned to the event message."
    },
    "retry_count": {
      "type": "integer",
      "minimum": 1,
      "description": "Number of retry attempts executed prior to final dead-lettering."
    },
    "error_type": {
      "type": "string",
      "description": "Exception class or machine-readable error category."
    },
    "error_message": {
      "type": "string",
      "description": "Descriptive error message captured during the terminal execution."
    },
    "stack_trace": {
      "type": "string",
      "description": "Full serialized stack trace of the terminal worker exception."
    },
    "original_payload": {
      "type": "object",
      "description": "The complete original event JSON payload that failed processing."
    },
    "trace_context": {
      "type": "object",
      "required": [
        "traceparent"
      ],
      "properties": {
        "traceparent": {
          "type": "string",
          "pattern": "^00-[a-f0-9]{32}-[a-f0-9]{16}-01$"
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

---

## 5. Celery Worker Pipeline & Ingestion DAG

The asynchronous execution fabric is implemented using Celery 5.4 orchestrated over RabbitMQ (Broker) and Redis (Result Backend DB 1). The pipeline coordinates multi-format ingestion, optical character recognition, semantic text chunking, embedding generation, and multi-tier deduplication.

### 5.1 Production Celery Configuration (`app/celery_app.py`)

The Celery worker fleet is configured with strict concurrency, memory, and reliability boundaries:

```python
import os
from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.getenv("RABBITMQ_URL", "amqp://drivex:drivexpass@rabbitmq:5672//")
RESULT_BACKEND = os.getenv("REDIS_URL", "redis://redis:6379/1")

celery_app = Celery("drivex_ml", broker=BROKER_URL, backend=RESULT_BACKEND)

celery_app.conf.update(
    # Serialization & Security
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,

    # Reliability Invariants (Late ACKs & Single Prefetch)
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,      # Recycle workers after 100 tasks to purge PyTorch GPU/CPU memory fragments
    task_track_started=True,
    task_time_limit=900,                 # Hard limit: 15 minutes before SIGKILL
    task_soft_time_limit=840,            # Soft limit: 14 minutes before SoftTimeLimitExceeded exception

    # Result Backend Expiration
    result_expires=86400,                # Retain task status for 24 hours in Redis DB 1

    # Task Publishing Retry Policy
    task_publish_retry=True,
    task_publish_retry_policy={
        "max_retries": 3,
        "interval_start": 0.5,
        "interval_step": 1.0,
        "interval_max": 5.0,
    },

    # Dedicated Quorum Queue Topology Mapping
    task_queues=[
        Queue(
            "file.ingest",
            Exchange("drivex.events", type="topic"),
            routing_key="file.uploaded.#",
            queue_arguments={"x-queue-type": "quorum"}
        ),
        Queue(
            "file.ocr",
            Exchange("drivex.events", type="topic"),
            routing_key="file.uploaded.image.#",
            queue_arguments={"x-queue-type": "quorum"}
        ),
        Queue(
            "file.embed",
            Exchange("drivex.events", type="topic"),
            routing_key="file.extracted.text",
            queue_arguments={"x-queue-type": "quorum"}
        ),
        Queue(
            "file.dedup",
            Exchange("drivex.events", type="topic"),
            routing_key="file.uploaded.#",
            queue_arguments={"x-queue-type": "quorum"}
        ),
    ],
    task_default_queue="file.ingest",
)
```

#### Worker Pool Sizing & Workload Specialization:
To prevent heavy OCR and PyTorch transformer workloads from starving fast I/O ingestion tasks, workers are split into specialized container pools:
1. **`worker-io` (Concurrency: 8, Pool: `threads` / `prefork`)**: Consumes `file.ingest`. Handles S3 download streams, SHA-256 byte validation, and plain text/markdown parsing.
2. **`worker-ocr` (Concurrency: 4, Pool: `prefork`)**: Consumes `file.ocr`. Runs `pdf2image` and Tesseract OCR processes isolated from memory-sensitive tasks.
3. **`worker-nlp` (Concurrency: 2 per GPU or 4 on CPU, Pool: `prefork`)**: Consumes `file.embed`. Houses pinned in-memory instances of `BAAI/bge-large-en-v1.5` and executes batch vectorization.
4. **`worker-dedup` (Concurrency: 4, Pool: `prefork`)**: Consumes `file.dedup`. Computes pHash/dHash and performs relational hash lookups.

---

### 5.2 Celery Canvas Orchestration DAG

The ingestion workflow is modeled as a declarative Directed Acyclic Graph (DAG) using Celery Canvas primitives (`chain`, `chord`, `group`):

```python
from celery import chain, chord, group
from app.tasks.ingest import task_ingest_file
from app.tasks.extract import task_extract_document, task_extract_image
from app.tasks.dedup import task_dedup_check
from app.tasks.chunk import task_semantic_chunk
from app.tasks.embed import task_generate_embeddings, task_upsert_qdrant
from app.tasks.finalize import task_finalize_ingestion

def build_ingestion_workflow(event_payload: dict):
    mime_type = event_payload["mime_type"]
    is_image = mime_type.startswith("image/")
    
    # 1. Select appropriate extraction task
    extraction_task = (
        task_extract_image.s(event_payload)
        if is_image
        else task_extract_document.s(event_payload)
    )
    
    # 2. Build parallel chord: extraction runs concurrently with dedup check
    parallel_analysis = chord(
        group(
            extraction_task,
            task_dedup_check.s(event_payload)
        ),
        # Chord callback receives [extraction_result, dedup_result]
        task_semantic_chunk.s(event_payload)
    )
    
    # 3. Formulate end-to-end chain
    workflow = chain(
        task_ingest_file.s(event_payload),
        parallel_analysis,
        task_generate_embeddings.s(event_payload),
        task_upsert_qdrant.s(event_payload),
        task_finalize_ingestion.s(event_payload)
    )
    return workflow
```

---

### 5.3 Multi-Format File Ingestion Engine

The document extraction tier normalizes unstructured binary formats into structured text blocks paired with structural hierarchical metadata.

#### 5.3.1 Native PDF Extraction via PyMuPDF (`fitz`)
- Opens the PDF binary stream without writing intermediate files to disk: `fitz.open(stream=stream_bytes, filetype="pdf")`.
- Iterates through document pages:
  - Extracts text blocks, bounding boxes, font weights, and sizes.
  - Detects structural headings: blocks with font size > 1.3x document median body font size are tagged as section titles.
  - Table extraction: PyMuPDF table finder (`page.find_tables()`) identifies grid lines, extracts tabular cells, and renders them as Markdown tables to preserve tabular relationships for RAG embeddings.
- **Scanned Page Detection Fallback**:
  If a page contains less than 32 text characters but contains raster image objects occupying > 40% of page area, the page is flagged as scanned and routed to the OCR engine.

#### 5.3.2 Scanned PDF & Raster Image OCR (`pdf2image` + `PyTesseract`)
- Scanned PDF pages are rasterized to uncompressed RGB images at 300 DPI using `pdf2image.convert_from_bytes(page_bytes, dpi=300)`.
- Image Pre-processing Pipeline (Pillow & OpenCV):
  1. Convert to 8-bit grayscale: `img.convert('L')`.
  2. Contrast enhancement: CLAHE (Contrast Limited Adaptive Histogram Equalization).
  3. Binarization: Otsu's thresholding to isolate text glyphs from paper background noise.
  4. De-skewing: Calculate text orientation angle via Radon transform and rotate image to horizontal baseline.
- Tesseract Execution:
  `pytesseract.image_to_string(processed_img, lang='eng', config='--oem 1 --psm 1')`
  - `--oem 1`: Neural network LSTM engine.
  - `--psm 1`: Automatic page segmentation with Orientation and Script Detection (OSD).
- Filter: Low-confidence OCR output (mean confidence < 40%) is flagged for human review or indexed as low-confidence.

#### 5.3.3 Office Document Extraction via `python-docx`
- Reads DOCX OpenXML packages directly from memory streams: `docx.Document(io.BytesIO(stream_bytes))`.
- Traverses document structural elements:
  - Paragraphs: Maps built-in styles (`Heading 1`, `Heading 2`, `Heading 3`, `Title`, `Subtitle`) into a hierarchical section breadcrumb stack.
  - Lists: Bulleted and numbered list items are formatted with appropriate indentation.
  - Tables: Iterates over rows and cells, generating formatted Markdown table blocks (`| Col 1 | Col 2 |`).
  - Embedded Hyperlinks: Resolves relationship IDs (`rId`) to target URLs and formats as `[Anchor Text](URL)`.

#### 5.3.4 Plain Text & Code with Encoding Fallback (`chardet` + NFKC)
- Primary attempt: Strict UTF-8 decoding (`bytes.decode('utf-8')`).
- Fallback Heuristic: On `UnicodeDecodeError`, sample first 64 KB of the byte stream and invoke `chardet.detect(sample)`.
  - Supports ISO-8859-1 (Latin-1), Windows-1252, Shift-JIS, GB18030, and EUC-KR.
  - Decodes full stream using detected encoding, ignoring unmapped bytes via `errors='replace'`.
- Normalization: Apply Unicode Normalization Form KC (NFKC) via `unicodedata.normalize('NFKC', text)` to standardize full-width characters, ligature forms, and composite glyphs into canonical equivalents.
- Control Character Cleansing: Strips non-printable ASCII control characters (`[-

-]`) while preserving tabs (`	`) and line feeds (`
`).

#### 5.3.5 Image Metadata & EXIF Extraction via Pillow
- Reads image streams using Pillow (`PIL.Image.open(io.BytesIO(stream_bytes))`).
- Extracts image dimensions (width, height), color mode (RGB, RGBA, CMYK), and format (JPEG, PNG, WebP).
- Parses raw EXIF data tags via `img.getexif()`:
  - `Make`, `Model` (Camera hardware information).
  - `DateTimeOriginal` (Capture timestamp).
  - `GPSInfo`: Decodes GPS latitude and longitude from degree/minute/second tuples to signed decimal coordinates, storing them in payload attributes for geospatial filtering.

---

### 5.4 Semantic Text Chunking Engine

The chunking engine converts continuous document streams into discrete, self-contained semantic units tailored for retrieval:

#### 5.4.1 Tokenizer & Window Specifications
- **Tokenizer**: Hugging Face `AutoTokenizer.from_pretrained("BAAI/bge-large-en-v1.5")`.
- **Target Chunk Size ($W$)**: 512 tokens (~2,048 characters).
- **Stride Overlap ($O$)**: 64 tokens (~256 characters), providing a 12.5% overlap ratio.
- **Overlap Invariant**: The overlap guarantees that sentences spanning boundary edges are fully represented in at least one adjacent chunk, preventing semantic truncation.

#### 5.4.2 Structural Breadcrumb & Section Header Context Preservation
Raw text chunks frequently lose context when detached from their parent document structure (e.g., a table row saying "Expenses: $50,000" is meaningless without knowing it belongs to the "Q3 AWS Cloud Infrastructure" section).
The chunking engine prepends a canonical context header to every chunk before tokenization:
```
[Document: {filename} > Section: {heading_level_1} > Subsection: {heading_level_2} | Page: {page_number}]
{chunk_text}
```
The token budget for the context header is dynamically subtracted from the 512-token chunk capacity, reserving 450-480 tokens for the substantive body text.

#### 5.4.3 Punctuation-Aware Boundary Splitting Algorithm
Splitting follows a strict recursive boundary hierarchy to guarantee that text is never sliced mid-word or mid-sentence:
1. **Separator Tier 1**: Double Newlines (`

`) representing paragraph boundaries.
2. **Separator Tier 2**: Single Newlines (`
`) representing lines and list items.
3. **Separator Tier 3**: Sentence Terminators (`. `, `? `, `! `) preserving complete grammatical sentences.
4. **Separator Tier 4**: Clause Delimiters (`; `, `, `, ` - `).
5. **Separator Tier 5**: Word Whitespaces (` `).
6. **Fallback Tier 6**: Raw character boundary (strictly invoked only if an unbroken alphanumeric string exceeds 512 tokens).

---

### 5.5 Dense Embedding Generation Engine

- **Embedding Model**: `BAAI/bge-large-en-v1.5`
- **Output Vector Dimension ($D$)**: 1024 float32 dimensions.
- **Max Input Length**: 512 tokens.
- **$L_2$ Normalization Invariant**:
  All generated vectors $\mathbf{v}$ are normalized to unit Euclidean length prior to storage:
  $$\hat{\mathbf{v}} = 
rac{\mathbf{v}}{\|\mathbf{v}\|_2} = 
rac{\mathbf{v}}{\sqrt{\sum_{i=1}^{1024} v_i^2}}$$
  Because $\|\hat{\mathbf{v}}\|_2 = 1.0$, the Cosine similarity between two vectors $\mathbf{a}$ and $\mathbf{b}$ reduces to an inner dot product:
  $$\text{CosineSimilarity}(\hat{\mathbf{a}}, \hat{\mathbf{b}}) = \hat{\mathbf{a}} \cdot \hat{\mathbf{b}} = \sum_{i=1}^{1024} a_i b_i$$
  Normalized vectors make cosine similarity equivalent to a dot product. Actual acceleration and throughput depend on hardware, Qdrant build, quantization, data size, and workload; benchmark rather than assume a fixed speedup.

#### Batch Processing & Dynamic Tensor Padding:
- Embeddings are generated in batches of **32 chunks**.
- Dynamic padding is applied per batch to the longest chunk in that batch, eliminating wasted matrix operations on pad tokens.

#### Query vs. Passage Instruction Prefixes:
`BAAI/bge-large-en-v1.5` is used with the model’s documented query instruction where applicable; verify the exact preprocessing contract for the pinned model and inference library version:
- **Passages / Chunks**: Ingested directly **without prefix**.
- **Search Queries**: Must include the task-specific instruction prefix:
  `"Represent this sentence for searching relevant passages: " + query_text`

---

### 5.6 Dual-Tier Deduplication Engine

To maximize storage utilization and eliminate vector database clutter, DriveX enforces a multi-tier deduplication engine:

```
[Uploaded File Stream]
         |
         |---> 1. Cryptographic SHA-256 Stream Hash
         |        |
         |        v
         |     Check MySQL `files.content_hash`
         |     - Match Found: Exact Byte Duplicate -> Re-use storage_key / Create version link
         |
         |---> 2. Perceptual Image Hashing (if Image)
         |        |
         |        v
         |     Compute 64-bit pHash (DCT) & 64-bit dHash (Gradient)
         |     - Evaluate Hamming Distance against existing images
         |     - Threshold <= 6: Flag as Perceptual Duplicate / Near-Duplicate
         |
         |---> 3. Semantic Near-Duplicate Detection (if Text)
                  |
                  v
               Mean-pool all chunk vectors into document embedding
               - Evaluate Cosine Similarity against existing documents
               - Threshold >= 0.92: Tag as Semantic Variant / Revision
```

#### 5.6.1 Cryptographic Deduplication (SHA-256)
- Computed in streaming fashion as bytes pass from MinIO.
- When an exact SHA-256 match occurs within the same owner/workspace, the system registers a new logical file entry pointing to the existing MinIO `storage_key` and increments the underlying object reference count, consuming zero additional S3 disk blocks.

#### 5.6.2 Perceptual Image Deduplication (pHash & dHash)
Raster images are vulnerable to visual duplication that alters byte checksums (e.g., resizing, minor JPEG compression, color profile stripping, watermark addition).
1. **Perceptual Hash (`pHash`)**:
   - Resizes image to 32x32 pixels, converts to grayscale.
   - Computes 2D Discrete Cosine Transform (DCT).
   - Extracts the low-frequency 8x8 DCT matrix (representing basic structure).

---

## Implementation Corrections and Production Invariants (v1.0.1)

This section is normative and takes precedence over any conflicting example or claim above. The original blueprint contains useful design direction, but several examples should not be treated as production-ready without the corrections below.

### A. Reliable event publication and state transitions

- Do not implement upload completion as a MySQL commit followed by a best-effort RabbitMQ publish. A process crash between those operations can leave a `PENDING` file that is never processed. Use a transactional outbox written in the same MySQL transaction as the file state change. A relay publishes outbox rows with publisher confirms and marks them delivered only after confirmation.
- Consumers must be idempotent using a stable event ID plus an ingestion generation/version. Enforce uniqueness in the database for the relevant `(file_id, version_id, processing_generation)` identity. Duplicate delivery is expected.
- Use explicit states such as `PENDING`, `PROCESSING`, `INDEXING`, `INDEXED`, `FAILED`, and `DELETING`, with conditional updates and leases/heartbeats. Do not mark a file `INDEXED` until all required vector writes are acknowledged and the indexed generation is recorded.
- For updates, index into a generation-specific point namespace or attach a generation field, then atomically switch the active generation in metadata. Remove old points asynchronously after the new generation is active. This prevents partial re-indexing from becoming the visible version.

### B. RabbitMQ and Celery topology corrections

- The exchange/queue examples must be made consistent. The shown `file.ocr` binding includes PDFs broadly, while the prose routes only scanned pages to OCR. Prefer a single ingestion orchestrator that classifies each file/page and dispatches OCR only for pages that need it; do not route every PDF directly to OCR by MIME type.
- The shown `file.dedup` queue bound to `file.uploaded.#` can race with ingestion and does not itself provide a safe deduplication transaction. Treat hashes as idempotent metadata operations, enforce uniqueness in MySQL, and avoid making dedup a competing finalization path.
- Celery's Canvas example is not a valid general-purpose extraction/chunking contract as written: the chord callback receives the group results, and `.s(event_payload)` may prepend arguments in a way that does not match the task signatures. Define and test explicit task signatures, use immutable signatures (`.si`) where prior results must not be prepended, and pass compact object references rather than full extracted text through broker messages. Store intermediate artifacts in bounded durable storage or a database/object store.
- Avoid having two independent retry systems control the same task. Either use Celery retry (`self.retry`) with bounded exponential backoff/jitter and a defined terminal path, or implement broker-level delayed retry with a documented republish/confirm/ack sequence. A worker must not acknowledge the original message until the retry publication is confirmed.
- `x-delivery-limit` is a quorum-queue delivery safety mechanism, not an application-level attempt counter for arbitrary republished messages. Track retry attempts in a validated message header or durable task record. Configure and test DLX behavior for the exact RabbitMQ version in use.
- Do not rely on a fixed default password in a broker URL. Require secrets from the deployment secret manager, TLS where traffic crosses trust boundaries, least-privilege users/vhosts, and credential rotation.

### C. Retrieval authorization and tenant isolation

- Build tenant and ACL filters from authenticated server-side identity and current authorization data. Never accept `owner_id`, `workspace_id`, or ACL filters as authoritative client input.
- A filter such as `(owner_id == user_id OR workspace_id == workspace_id)` is insufficient: workspace membership does not imply access to every file, and the expression must bind the caller’s actual workspace membership and file-level ACLs. Apply deny-by-default authorization.
- Revalidate access before returning chunk text, filenames, metadata, source references, cached answers, or citations. Permission changes and deletions must invalidate relevant caches and remove or deactivate indexed points.
- Use opaque, non-guessable point IDs or deterministic IDs scoped to a file/version/chunk. Do not expose raw cross-tenant identifiers as a security control. Add automated negative tests proving that users cannot retrieve another tenant’s content through dense search, lexical search, reranking, cache hits, citations, or error paths.
- Redis cache keys must include tenant, principal/authorization version, normalized query, model/index version, and relevant filters. Do not share cached responses across principals unless the cache is explicitly scoped to an identical authorized corpus.

### D. Search and RAG correctness

- MySQL `FULLTEXT` is not BM25 by default. Label its score according to the actual engine and query mode. If BM25 is required, use a search engine that implements it or document the chosen lexical ranking method.
- Reciprocal Rank Fusion should use ranks, not raw dense and lexical scores. The weights are tunable parameters, not universal constants. Evaluate them on a representative, permission-filtered relevance set.
- A cross-encoder output is not automatically a calibrated probability. Treat it as a relevance score unless calibration has been measured. Avoid a universal hard cutoff such as `0.40`; tune thresholds using evaluation data and retain a safe no-context behavior.
- HyDE is optional query expansion, not a factual source. Do not cite or treat the hypothetical passage as evidence. Keep the original user query in retrieval and evaluate whether HyDE improves recall for the product’s actual document mix.
- Build the prompt budget using the selected generation model’s tokenizer and context limit. The example allocation (system, chunks, history) must sum within the model’s actual context window and reserve room for output tokens.
- Treat retrieved documents as untrusted data. Delimit retrieved content, instruct the model not to follow instructions found inside documents, and enforce tool permissions outside the model. Citations must map to stored source spans/pages and be checked against the final authorized context.
- Stream an explicit error event when generation fails after the SSE connection starts. Include heartbeat/keep-alive behavior, disconnect cancellation, bounded generation time, and no-store cache headers where responses contain private content.

### E. File parsing, OCR, and resource safety

- MIME type and filename are hints, not proof of file format. Validate signatures and parse with hardened libraries in isolated workers. Apply allowlists, malware scanning where required, decompression-bomb defenses, archive recursion limits, maximum file size, page count, pixel count, and execution timeouts.
- Avoid claiming that a PDF page can be passed directly to `pdf2image.convert_from_bytes` as a page-specific PDF without constructing a valid single-page document. Use a supported page-range/render API or create a bounded single-page PDF representation.
- OCR confidence is engine- and document-dependent. Preserve per-page confidence/quality metadata; route low-quality output to review or mark it as uncertain rather than silently treating it as authoritative.
- Do not strip control characters with an ambiguous regex or normalize all code indiscriminately. Preserve source text and apply normalization only to the retrieval representation; record the transformation/version. Keep original bytes immutable.
- EXIF GPS and capture timestamps are sensitive metadata. Extract only when required, apply access controls and retention rules, and avoid placing precise location in broadly searchable payloads by default.

### F. Embedding and vector-index versioning

- Pin model repository revision, tokenizer revision, library versions, preprocessing rules, vector dimension, distance metric, and chunker version. Store these values as an index-generation manifest. A model change requires a controlled re-embedding/migration plan.
- Verify that the selected model’s actual maximum sequence length and query instruction match the deployed model revision. Chunk limits must include any prepended context/instruction tokens.
- Do not promise a fixed 4x performance improvement from normalization or SIMD. Benchmark end-to-end latency, recall, memory, and throughput on the target hardware and corpus.
- Choose Qdrant quantization and HNSW settings through measured recall/latency/memory trade-offs. Define payload indexes for fields used in filters and test behavior at expected cardinality.

### G. Deduplication semantics

- Exact-byte deduplication must be scoped to an explicit security and ownership policy. Cross-tenant object sharing can create existence, timing, or quota side channels; default to tenant-scoped dedup unless a deliberate shared-object design addresses these risks.
- A SHA-256 match must be backed by a uniqueness constraint and race-safe transaction. Object reference counts should be derived or transactionally maintained, and object deletion must wait until no live references remain.
- Perceptual and semantic similarity are candidate signals, not proof of duplicate identity. Never merge, overwrite, or suppress user files solely because a hash distance or embedding similarity crosses a heuristic threshold. Keep them as advisory labels unless the user explicitly opts into a deduplication action.

### H. Observability, operations, and acceptance criteria

Define and measure at least: outbox age and publish failures; queue depth and oldest-message age; retry/DLQ counts; ingestion success rate and stage latency; parser/OCR failures; embedding throughput; Qdrant upsert/search latency and recall; authorization-denial tests; SSE time-to-first-token and completion rate; and per-tenant resource usage. Use trace IDs across API, outbox, broker, workers, and query services.

Treat all numeric targets in this document (including p99 latency, batch size, thresholds, concurrency, and retrieval counts) as initial configuration values or proposed SLOs until load tests and relevance/security evaluations validate them. Document the tested dataset, hardware, software versions, workload, and date alongside benchmark results.

### I. Release checklist

- [ ] Event publication is protected by a transactional outbox and publisher confirms.
- [ ] Duplicate, reordered, delayed, and redelivered events are covered by integration tests.
- [ ] Retry and DLQ paths are tested against the pinned RabbitMQ/Celery versions.
- [ ] ACL enforcement is tested across dense search, lexical search, reranking, cache, citations, and deletion.
- [ ] Partial indexing and model/chunker upgrades use generation-based cutover and rollback.
- [ ] Untrusted file parsing has resource limits and isolation.
- [ ] RAG quality, latency, and security are evaluated on representative test sets.
- [ ] Secrets, TLS, backups, retention, deletion, and incident procedures are documented.

