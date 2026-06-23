-- ---------------------------------------------------------------------------
-- Unified Connector Service (UCS) inbound gRPC request events.
--
-- These are the requests UCS itself serves (stage = GrpcRequest), as opposed to
-- the outbound connector calls (which land in `connector_events`). They have no
-- downstream target, so `destination` is null; `source` is stamped
-- `unified_connector_service`. Field mapping mirrors the UCS connector-event
-- shape: reference_id -> payment_id, resource_id -> refund_id,
-- lineage_merchant_id -> merchant_id, flow_type -> flow, connector ->
-- connector_name, request_data -> request, response_data -> masked_response,
-- timestamp(ms) -> created_at, latency_ms -> latency.
-- ---------------------------------------------------------------------------

CREATE TABLE ucs_api_events_queue
(
    `request_id` String,
    `timestamp` Int64,
    `flow_type` LowCardinality(String),
    `connector` LowCardinality(String),
    `url` Nullable(String),
    `method` Nullable(String),
    `stage` LowCardinality(String),
    `execution_mode` LowCardinality(Nullable(String)),
    `latency_ms` Nullable(UInt64),
    `status_code` Nullable(Int32),
    `request_data` Nullable(String),
    `response_data` Nullable(String),
    `error` Nullable(String),
    `reference_id` Nullable(String),
    `resource_id` Nullable(String),
    `lineage_merchant_id` Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka0:29092', kafka_topic_list = 'unified-connector-service-api-events', kafka_group_name = 'hyper', kafka_format = 'JSONEachRow', kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW ucs_api_events_parse_errors (
    `topic` String,
    `partition` Int64,
    `offset` Int64,
    `raw` String,
    `error` String
) ENGINE = MergeTree
ORDER BY
    (topic, partition, offset) SETTINGS index_granularity = 8192 AS
SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    _raw_message AS raw,
    _error AS error
FROM
    ucs_api_events_queue
WHERE
    length(_error) > 0;

CREATE TABLE ucs_api_events (
    `merchant_id` LowCardinality(String),
    `payment_id` Nullable(String),
    `connector_name` LowCardinality(String),
    `request_id` String,
    `flow` LowCardinality(String),
    `request` String,
    `response` Nullable(String),
    `masked_response` Nullable(String),
    `error` Nullable(String),
    `status_code` UInt32,
    `created_at` DateTime64(3),
    `inserted_at` DateTime DEFAULT now() CODEC(T64, LZ4),
    `latency` UInt128,
    `method` LowCardinality(String),
    `url` Nullable(String),
    `stage` LowCardinality(String),
    `refund_id` Nullable(String),
    `destination` LowCardinality(Nullable(String)),
    `execution_mode` LowCardinality(Nullable(String)),
    `source` LowCardinality(Nullable(String)),
    INDEX flowIndex flow TYPE bloom_filter GRANULARITY 1,
    INDEX connectorIndex connector_name TYPE bloom_filter GRANULARITY 1,
    INDEX statusIndex status_code TYPE bloom_filter GRANULARITY 1
) ENGINE = MergeTree PARTITION BY toStartOfDay(created_at)
ORDER BY
    (
        created_at,
        merchant_id,
        connector_name,
        flow,
        status_code
    ) TTL inserted_at + toIntervalMonth(18) SETTINGS index_granularity = 8192;

CREATE TABLE ucs_api_events_audit (
    `merchant_id` LowCardinality(String),
    `payment_id` String,
    `connector_name` LowCardinality(String),
    `request_id` String,
    `flow` LowCardinality(String),
    `request` String,
    `response` Nullable(String),
    `masked_response` Nullable(String),
    `error` Nullable(String),
    `status_code` UInt32,
    `created_at` DateTime64(3),
    `inserted_at` DateTime DEFAULT now() CODEC(T64, LZ4),
    `latency` UInt128,
    `method` LowCardinality(String),
    `url` Nullable(String),
    `stage` LowCardinality(String),
    `refund_id` Nullable(String),
    `destination` LowCardinality(Nullable(String)),
    `execution_mode` LowCardinality(Nullable(String)),
    `source` LowCardinality(Nullable(String)),
    INDEX flowIndex flow TYPE bloom_filter GRANULARITY 1,
    INDEX connectorIndex connector_name TYPE bloom_filter GRANULARITY 1,
    INDEX statusIndex status_code TYPE bloom_filter GRANULARITY 1
) ENGINE = MergeTree PARTITION BY merchant_id
ORDER BY
    (merchant_id, payment_id) TTL inserted_at + toIntervalMonth(18) SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW ucs_api_events_mv TO ucs_api_events (
    `merchant_id` String,
    `payment_id` Nullable(String),
    `connector_name` LowCardinality(String),
    `request_id` String,
    `flow` LowCardinality(String),
    `request` String,
    `response` Nullable(String),
    `masked_response` Nullable(String),
    `error` Nullable(String),
    `status_code` UInt32,
    `created_at` DateTime64(3),
    `inserted_at` DateTime DEFAULT now() CODEC(T64, LZ4),
    `latency` UInt128,
    `method` LowCardinality(String),
    `url` Nullable(String),
    `stage` LowCardinality(String),
    `refund_id` Nullable(String),
    `destination` LowCardinality(Nullable(String)),
    `execution_mode` LowCardinality(Nullable(String)),
    `source` LowCardinality(Nullable(String))
) AS
SELECT
    ifNull(lineage_merchant_id, '') AS merchant_id,
    reference_id AS payment_id,
    connector AS connector_name,
    request_id,
    flow_type AS flow,
    ifNull(request_data, '') AS request,
    response_data AS response,
    response_data AS masked_response,
    error,
    toUInt32(ifNull(status_code, 0)) AS status_code,
    fromUnixTimestamp64Milli(timestamp) AS created_at,
    now64() AS inserted_at,
    toUInt128(ifNull(latency_ms, 0)) AS latency,
    ifNull(method, '') AS method,
    url,
    stage,
    resource_id AS refund_id,
    CAST(NULL AS Nullable(String)) AS destination,
    execution_mode,
    'unified_connector_service' AS source
FROM
    ucs_api_events_queue
WHERE
    length(_error) = 0;

CREATE MATERIALIZED VIEW ucs_api_events_audit_mv TO ucs_api_events_audit (
    `merchant_id` String,
    `payment_id` String,
    `connector_name` LowCardinality(String),
    `request_id` String,
    `flow` LowCardinality(String),
    `request` String,
    `response` Nullable(String),
    `masked_response` Nullable(String),
    `error` Nullable(String),
    `status_code` UInt32,
    `created_at` DateTime64(3),
    `inserted_at` DateTime DEFAULT now() CODEC(T64, LZ4),
    `latency` UInt128,
    `method` LowCardinality(String),
    `url` Nullable(String),
    `stage` LowCardinality(String),
    `refund_id` Nullable(String),
    `destination` LowCardinality(Nullable(String)),
    `execution_mode` LowCardinality(Nullable(String)),
    `source` LowCardinality(Nullable(String))
) AS
SELECT
    ifNull(lineage_merchant_id, '') AS merchant_id,
    reference_id AS payment_id,
    connector AS connector_name,
    request_id,
    flow_type AS flow,
    ifNull(request_data, '') AS request,
    response_data AS response,
    response_data AS masked_response,
    error,
    toUInt32(ifNull(status_code, 0)) AS status_code,
    fromUnixTimestamp64Milli(timestamp) AS created_at,
    now64() AS inserted_at,
    toUInt128(ifNull(latency_ms, 0)) AS latency,
    ifNull(method, '') AS method,
    url,
    stage,
    resource_id AS refund_id,
    CAST(NULL AS Nullable(String)) AS destination,
    execution_mode,
    'unified_connector_service' AS source
FROM
    ucs_api_events_queue
WHERE
    (length(_error) = 0)
    AND (reference_id IS NOT NULL);
