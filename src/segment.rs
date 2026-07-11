use anyhow::{Context, Result};
use arrow_array::{
    ArrayRef as ArrowArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use std::sync::Arc;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::stream::ArrayStreamExt;
use vortex::buffer::ByteBufferMut;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use crate::metrics::TypedRunMetrics;
use crate::record::SpanRecord;

pub const SPAN_SEGMENT_SCHEMA_VERSION: i64 = 3;
pub const ROW_INDEXED_SPAN_SEGMENT_SCHEMA_VERSION: i64 = 3;

pub async fn encode_span_records(records: &[SpanRecord]) -> Result<Bytes> {
    let batch = records_to_batch(records)?;
    let vortex_array =
        ArrayRef::from_arrow(batch, false).context("convert Arrow span batch to Vortex")?;

    let session = VortexSession::default().with_tokio();
    let mut buffer = ByteBufferMut::empty();
    session
        .write_options()
        .write(&mut buffer, vortex_array.to_array_stream())
        .await
        .context("write Vortex span segment")?;

    Ok(Bytes::copy_from_slice(buffer.as_ref()))
}

pub async fn read_span_count(payload: Bytes) -> Result<usize> {
    let session = VortexSession::default().with_tokio();
    let array = session
        .open_options()
        .open_buffer(payload)
        .context("open Vortex buffer")?
        .scan()
        .context("scan Vortex buffer")?
        .into_array_stream()
        .context("create Vortex scan stream")?
        .read_all()
        .await
        .context("read Vortex buffer")?;
    Ok(array.len())
}

fn records_to_batch(records: &[SpanRecord]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("project_name", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_run_id", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("run_type", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("end_time_unix_nano", DataType::Int64, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("event_time_unix_nano", DataType::Int64, false),
        Field::new("row_index", DataType::Int64, false),
        Field::new("attributes_json", DataType::Utf8, false),
        Field::new("latency_nanos", DataType::Int64, false),
        Field::new("prompt_tokens", DataType::Int64, true),
        Field::new("completion_tokens", DataType::Int64, true),
        Field::new("total_tokens", DataType::Int64, true),
        Field::new("prompt_cost", DataType::Float64, true),
        Field::new("completion_cost", DataType::Float64, true),
        Field::new("total_cost", DataType::Float64, true),
        Field::new("first_token_latency_nanos", DataType::Int64, true),
        Field::new("evaluator_score", DataType::Float64, true),
        Field::new("model_name", DataType::Utf8, true),
        Field::new("provider_name", DataType::Utf8, true),
    ]));

    let parent_run_ids: Vec<Option<&str>> = records
        .iter()
        .map(|record| record.parent_run_id.as_deref())
        .collect();
    let parent_span_ids: Vec<Option<&str>> = records
        .iter()
        .map(|record| record.parent_span_id.as_deref())
        .collect();
    let metrics = records
        .iter()
        .map(TypedRunMetrics::from_record)
        .collect::<Vec<_>>();
    let columns: Vec<ArrowArrayRef> = vec![
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.project_name.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.run_id.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.trace_id.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.span_id.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(parent_run_ids)),
        Arc::new(StringArray::from(parent_span_ids)),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.name.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.run_type.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record.start_time_unix_nano)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record.end_time_unix_nano)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            records
                .iter()
                .map(|record| record.status_code)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.event_kind.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            records.iter().map(event_time_unix_nano).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            records
                .iter()
                .enumerate()
                .map(|(row_index, _)| row_index as i64)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record.attributes_json.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.latency_nanos)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.prompt_tokens)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.completion_tokens)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.total_tokens)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.prompt_cost)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.completion_cost)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.total_cost)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.first_token_latency_nanos)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            metrics
                .iter()
                .map(|metrics| metrics.evaluator_score)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            metrics
                .iter()
                .map(|metrics| metrics.model_name.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            metrics
                .iter()
                .map(|metrics| metrics.provider_name.as_deref())
                .collect::<Vec<_>>(),
        )),
    ];

    RecordBatch::try_new(schema, columns).context("build Arrow span batch")
}

fn event_time_unix_nano(record: &SpanRecord) -> i64 {
    if record.end_time_unix_nano > 0 {
        record.end_time_unix_nano
    } else {
        record.start_time_unix_nano
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn encodes_span_records_as_readable_vortex() {
        let records = vec![
            span_record("root", None),
            span_record("child", Some("1111111111111111")),
        ];

        let payload = encode_span_records(&records)
            .await
            .expect("encode Vortex segment");
        assert!(!payload.is_empty());
        assert_eq!(
            read_span_count(payload).await.expect("read Vortex segment"),
            2
        );
    }

    fn span_record(name: &str, parent_span_id: Option<&str>) -> SpanRecord {
        SpanRecord {
            project_name: "demo".to_owned(),
            run_id: String::new(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: if parent_span_id.is_some() {
                "2222222222222222".to_owned()
            } else {
                "1111111111111111".to_owned()
            },
            parent_run_id: None,
            parent_span_id: parent_span_id.map(str::to_owned),
            name: name.to_owned(),
            run_type: "span".to_owned(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            status_code: 1,
            event_kind: crate::record::RunEventKind::End,
            attributes_json: "{}".to_owned(),
            idempotency_key: None,
        }
    }
}
