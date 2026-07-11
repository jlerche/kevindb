use anyhow::{Context, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, StringViewArray};

use super::RunSummary;

pub(crate) fn run_summaries_from_batches(batches: &[RecordBatch]) -> Result<Vec<RunSummary>> {
    let mut runs = Vec::new();
    for batch in batches {
        let project_names = string_column(batch, 0, "project_name")?;
        let run_ids = string_column(batch, 1, "run_id")?;
        let trace_ids = string_column(batch, 2, "trace_id")?;
        let span_ids = string_column(batch, 3, "span_id")?;
        let parent_run_ids = string_column(batch, 4, "parent_run_id")?;
        let parent_span_ids = string_column(batch, 5, "parent_span_id")?;
        let names = string_column(batch, 6, "name")?;
        let run_types = string_column(batch, 7, "run_type")?;
        let statuses = string_column(batch, 8, "status")?;
        let start_times = int64_column(batch, 9, "start_time_unix_nano")?;
        let end_times = int64_column(batch, 10, "end_time_unix_nano")?;
        let roots = bool_column(batch, 11, "is_root")?;
        let attributes_json_values = string_column(batch, 12, "attributes_json")?;

        for row in 0..batch.num_rows() {
            runs.push(RunSummary {
                project_name: project_names.value(row).to_owned(),
                run_id: run_ids.value(row).to_owned(),
                trace_id: trace_ids.value(row).to_owned(),
                span_id: span_ids.value(row).to_owned(),
                parent_run_id: optional_string_value(&parent_run_ids, row),
                parent_span_id: optional_string_value(&parent_span_ids, row),
                name: names.value(row).to_owned(),
                run_type: run_types.value(row).to_owned(),
                status: statuses.value(row).to_owned(),
                start_time_unix_nano: start_times.value(row),
                end_time_unix_nano: end_times.value(row),
                is_root: roots.value(row),
                attributes_json: attributes_json_values.value(row).to_owned(),
            });
        }
    }

    Ok(runs)
}

fn optional_string_value(column: &StringColumn<'_>, row: usize) -> Option<String> {
    if column.is_null(row) {
        None
    } else {
        Some(column.value(row).to_owned())
    }
}

enum StringColumn<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl StringColumn<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(column) => column.is_null(row),
            Self::Utf8View(column) => column.is_null(row),
        }
    }

    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(column) => column.value(row),
            Self::Utf8View(column) => column.value(row),
        }
    }
}

fn string_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<StringColumn<'a>> {
    let column = batch.column(index);
    if let Some(column) = column.as_string_opt::<i32>() {
        return Ok(StringColumn::Utf8(column));
    }
    if let Some(column) = column.as_string_view_opt() {
        return Ok(StringColumn::Utf8View(column));
    }

    Err(anyhow::anyhow!("column {name} is not Utf8 or Utf8View"))
}

fn int64_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("column {name} is not Int64"))
}

fn bool_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a BooleanArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .with_context(|| format!("column {name} is not Boolean"))
}
