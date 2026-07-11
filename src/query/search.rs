use std::collections::{BTreeSet, HashSet};
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;

use crate::search::{
    SEARCH_INDEX_HEADER_LEN, SEARCH_INDEX_SCHEMA_VERSION, SearchIndexChunks, SearchIndexDirectory,
    SearchIndexGroupBytes, SearchIndexGroupDictionary, SearchIndexGroupDirectory,
    SearchIndexGroupTermSlices, SearchIndexRange, SearchIndexTermInfo, SearchIndexTermSlices,
    SearchPredicate, decode_search_group_dictionary, decode_search_index_chunks,
    decode_search_index_directory, decode_search_index_header, encode_search_term_infos,
    select_search_index_groups, select_search_index_terms,
};

use super::RunQuery;
use super::object_store_stats::ObjectStoreReadSnapshot;
use super::planner::{RunKey, RunQueryPlan, SegmentCandidateRow, SegmentSource};

pub(crate) async fn apply_phase6_search_indexes(
    object_store: Arc<dyn ObjectStore>,
    plan: RunQueryPlan,
    query: &RunQuery,
) -> Result<(RunQueryPlan, ObjectStoreReadSnapshot)> {
    let Some(predicate) = query
        .filter
        .as_ref()
        .map(|filter| filter.phase6_search_predicate())
        .transpose()
        .map_err(|err| anyhow!(err))?
        .flatten()
    else {
        return Ok((plan, ObjectStoreReadSnapshot::default()));
    };

    let mut stats = ObjectStoreReadSnapshot::default();
    let mut segments = Vec::new();
    let mut candidate_run_keys = HashSet::new();

    for mut segment in plan.segments {
        let matching_rows = load_matching_rows(&object_store, &segment, &predicate, &mut stats)
            .await
            .with_context(|| format!("evaluate search index for {}", segment.uri))?;
        segment.candidate_rows = filter_candidate_rows(segment.candidate_rows, &matching_rows);
        if segment.candidate_rows.is_empty() {
            continue;
        }
        candidate_run_keys.extend(segment.candidate_rows.iter().map(RunKey::from));
        segments.push(segment);
    }

    let candidate_runs = candidate_run_keys.len();
    if let Some(limit) = query.limits.max_candidate_runs
        && candidate_runs > limit
    {
        bail!("query rejected: candidate runs {candidate_runs} exceed limit {limit}");
    }
    let candidate_bytes = segments
        .iter()
        .map(|segment| segment.total_bytes + segment.search_index_bytes)
        .sum();
    let estimated_object_store_requests = super::planner::estimate_vortex_object_store_requests(
        segments.len(),
    )
    .saturating_add(
        super::planner::estimate_search_index_object_store_requests_for_segments(segments.len()),
    );

    Ok((
        RunQueryPlan {
            segments,
            candidate_run_keys,
            candidate_runs,
            candidate_bytes,
            estimated_object_store_requests,
        },
        stats,
    ))
}

async fn load_matching_rows(
    object_store: &Arc<dyn ObjectStore>,
    segment: &SegmentSource,
    predicate: &SearchPredicate,
    stats: &mut ObjectStoreReadSnapshot,
) -> Result<BTreeSet<u32>> {
    let Some(search_index_uri) = segment.search_index_uri.as_deref() else {
        bail!(
            "query rejected: segment {} is missing its Phase 6 search index; unsafe payload scan disabled",
            segment.uri
        );
    };
    if segment.search_index_schema_version != SEARCH_INDEX_SCHEMA_VERSION {
        bail!(
            "query rejected: segment {} has unsupported search index schema version {}",
            segment.uri,
            segment.search_index_schema_version
        );
    }

    let path = Path::from(search_index_uri);
    let directory = load_search_index_directory(object_store, &path, stats).await?;
    let selection = select_search_index_groups(&directory, predicate);
    let chunks = load_selected_chunks(
        object_store,
        &path,
        &directory,
        &selection,
        predicate,
        stats,
    )
    .await?;
    let index = decode_search_index_chunks(&directory, chunks)?;
    Ok(index.matching_rows(predicate))
}

async fn load_search_index_directory(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    stats: &mut ObjectStoreReadSnapshot,
) -> Result<SearchIndexDirectory> {
    let header_bytes =
        read_one_range(object_store, path, 0..SEARCH_INDEX_HEADER_LEN as u64, stats).await?;
    let header = decode_search_index_header(&header_bytes)?;
    let directory_start = SEARCH_INDEX_HEADER_LEN as u64;
    let directory_end = directory_start.saturating_add(u64::from(header.directory_len));
    let directory_bytes = if header.directory_len == 0 {
        Bytes::new()
    } else {
        read_one_range(object_store, path, directory_start..directory_end, stats).await?
    };
    decode_search_index_directory(header, &directory_bytes)
}

async fn load_selected_chunks(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    directory: &SearchIndexDirectory,
    selection: &crate::search::SearchIndexGroupSelection,
    predicate: &SearchPredicate,
    stats: &mut ObjectStoreReadSnapshot,
) -> Result<SearchIndexChunks> {
    let dictionaries = load_selected_dictionaries(object_store, path, directory, selection, stats)
        .await
        .context("load selected search dictionaries")?;
    let term_slices = select_search_index_terms(
        predicate,
        &dictionaries.term_key_groups,
        &dictionaries.term_value_groups,
    );
    load_term_slices(object_store, path, directory, term_slices, stats).await
}

async fn load_selected_dictionaries(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    directory: &SearchIndexDirectory,
    selection: &crate::search::SearchIndexGroupSelection,
    stats: &mut ObjectStoreReadSnapshot,
) -> Result<SearchIndexDictionaries> {
    let mut ranges = Vec::new();
    let term_key_plans = plan_dictionary_ranges(
        &selection.term_key_groups,
        &directory.term_key_groups,
        &mut ranges,
    );
    let term_value_plans = plan_dictionary_ranges(
        &selection.term_value_groups,
        &directory.term_value_groups,
        &mut ranges,
    );
    let chunks = read_ranges(object_store, path, &ranges, stats).await?;
    Ok(SearchIndexDictionaries {
        term_key_groups: materialize_dictionaries(
            &chunks,
            &directory.term_key_groups,
            term_key_plans,
        )?,
        term_value_groups: materialize_dictionaries(
            &chunks,
            &directory.term_value_groups,
            term_value_plans,
        )?,
    })
}

fn plan_dictionary_ranges(
    group_indexes: &BTreeSet<usize>,
    directories: &[SearchIndexGroupDirectory],
    ranges: &mut Vec<Range<u64>>,
) -> Vec<DictionaryRangePlan> {
    group_indexes
        .iter()
        .filter_map(|group_index| {
            let directory = directories.get(*group_index)?;
            Some(DictionaryRangePlan {
                group_index: *group_index,
                fst: push_range(ranges, directory.fst),
                term_infos: push_range(ranges, directory.term_infos),
            })
        })
        .collect()
}

fn push_range(ranges: &mut Vec<Range<u64>>, range: SearchIndexRange) -> usize {
    let index = ranges.len();
    ranges.push(range.as_range());
    index
}

fn materialize_dictionaries(
    chunks: &[Bytes],
    directories: &[SearchIndexGroupDirectory],
    plans: Vec<DictionaryRangePlan>,
) -> Result<Vec<SearchIndexGroupDictionary>> {
    plans
        .into_iter()
        .map(|plan| {
            let directory = directories
                .get(plan.group_index)
                .context("search dictionary plan group out of bounds")?;
            decode_search_group_dictionary(
                plan.group_index,
                chunks[plan.fst].clone(),
                &chunks[plan.term_infos],
                directory.term_info_count,
            )
        })
        .collect()
}

async fn load_term_slices(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    directory: &SearchIndexDirectory,
    term_slices: SearchIndexTermSlices,
    stats: &mut ObjectStoreReadSnapshot,
) -> Result<SearchIndexChunks> {
    let mut ranges = Vec::new();
    let term_key_plans = plan_term_data_ranges(
        term_slices.term_key_groups,
        &directory.term_key_groups,
        false,
        &mut ranges,
    )?;
    let term_value_plans = plan_term_data_ranges(
        term_slices.term_value_groups,
        &directory.term_value_groups,
        true,
        &mut ranges,
    )?;
    let chunks = read_ranges(object_store, path, &ranges, stats).await?;
    Ok(SearchIndexChunks {
        term_key_groups: materialize_term_data_groups(&chunks, term_key_plans)?,
        term_value_groups: materialize_term_data_groups(&chunks, term_value_plans)?,
    })
}

fn plan_term_data_ranges(
    groups: Vec<SearchIndexGroupTermSlices>,
    directories: &[SearchIndexGroupDirectory],
    allow_positions: bool,
    ranges: &mut Vec<Range<u64>>,
) -> Result<Vec<TermDataGroupPlan>> {
    groups
        .into_iter()
        .map(|group| {
            let directory = directories
                .get(group.group_index)
                .context("term data plan group out of bounds")?;
            let terms = group
                .terms
                .into_iter()
                .map(|term| {
                    Ok(TermDataPlan {
                        ordinal: term.ordinal,
                        info: term.info,
                        postings: push_range(
                            ranges,
                            subrange(
                                directory.postings,
                                term.info.postings_offset,
                                term.info.postings_len,
                            )?,
                        ),
                        positions: if allow_positions
                            && term.include_positions
                            && term.info.positions_len > 0
                        {
                            Some(push_range(
                                ranges,
                                subrange(
                                    directory.positions,
                                    term.info.positions_offset,
                                    term.info.positions_len,
                                )?,
                            ))
                        } else {
                            None
                        },
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(TermDataGroupPlan {
                group_index: group.group_index,
                fst_bytes: group.fst_bytes,
                term_info_count: group.term_info_count,
                terms,
            })
        })
        .collect()
}

fn materialize_term_data_groups(
    chunks: &[Bytes],
    plans: Vec<TermDataGroupPlan>,
) -> Result<Vec<SearchIndexGroupBytes>> {
    plans
        .into_iter()
        .map(|plan| {
            let mut infos = vec![SearchIndexTermInfo::default(); plan.term_info_count as usize];
            let mut postings = Vec::new();
            let mut positions = Vec::new();
            for term in plan.terms {
                let postings_offset = checked_u32(postings.len(), "local postings offset")?;
                let postings_chunk = chunks
                    .get(term.postings)
                    .context("missing postings chunk for selected term")?;
                postings.extend_from_slice(postings_chunk);
                let positions_offset = checked_u32(positions.len(), "local positions offset")?;
                let positions_len = if let Some(index) = term.positions {
                    let positions_chunk = chunks
                        .get(index)
                        .context("missing positions chunk for selected term")?;
                    positions.extend_from_slice(positions_chunk);
                    checked_u32(positions_chunk.len(), "local positions len")?
                } else {
                    0
                };
                if let Some(info) = infos.get_mut(term.ordinal) {
                    *info = SearchIndexTermInfo {
                        doc_count: term.info.doc_count,
                        postings_offset,
                        postings_len: checked_u32(postings_chunk.len(), "local postings len")?,
                        positions_offset,
                        positions_len,
                    };
                }
            }
            Ok(SearchIndexGroupBytes {
                group_index: plan.group_index,
                fst_bytes: plan.fst_bytes,
                term_info_bytes: Bytes::from(encode_search_term_infos(&infos)),
                postings: Bytes::from(postings),
                positions: Bytes::from(positions),
            })
        })
        .collect()
}

fn subrange(base: SearchIndexRange, offset: u32, len: u32) -> Result<SearchIndexRange> {
    let offset = base
        .offset
        .checked_add(u64::from(offset))
        .context("search term range offset overflow")?;
    let end = offset
        .checked_add(u64::from(len))
        .context("search term range end overflow")?;
    let base_end = base
        .offset
        .checked_add(base.len)
        .context("search group range end overflow")?;
    if end > base_end {
        bail!("search term range exceeds row group range");
    }
    Ok(SearchIndexRange {
        offset,
        len: u64::from(len),
    })
}

fn checked_u32(value: usize, label: &str) -> Result<u32> {
    u32::try_from(value).with_context(|| format!("{label} does not fit in u32"))
}

async fn read_one_range(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    range: Range<u64>,
    stats: &mut ObjectStoreReadSnapshot,
) -> Result<Bytes> {
    let mut chunks = read_ranges(object_store, path, &[range], stats).await?;
    chunks
        .pop()
        .context("object store returned no bytes for requested search index range")
}

async fn read_ranges(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    ranges: &[Range<u64>],
    stats: &mut ObjectStoreReadSnapshot,
) -> Result<Vec<Bytes>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let (coalesced_ranges, plans) = coalesce_ranges(ranges);
    stats.get_ranges_requests = stats.get_ranges_requests.saturating_add(1);
    let coalesced_chunks = object_store
        .get_ranges(path, &coalesced_ranges)
        .await
        .with_context(|| format!("read search index ranges from {path}"))?;
    stats.bytes_read = stats.bytes_read.saturating_add(
        coalesced_chunks
            .iter()
            .map(|chunk| chunk.len() as u64)
            .sum::<u64>(),
    );
    let mut chunks = vec![Bytes::new(); ranges.len()];
    for plan in plans {
        let parent = coalesced_chunks
            .get(plan.coalesced_index)
            .context("missing coalesced search range")?;
        let base = coalesced_ranges
            .get(plan.coalesced_index)
            .context("missing coalesced search range plan")?;
        let start = usize::try_from(plan.start.saturating_sub(base.start))
            .context("coalesced search range start does not fit usize")?;
        let len = usize::try_from(plan.end.saturating_sub(plan.start))
            .context("coalesced search range len does not fit usize")?;
        let end = start
            .checked_add(len)
            .context("coalesced search range slice overflow")?;
        chunks[plan.original_index] = parent.slice(start..end);
    }
    Ok(chunks)
}

fn coalesce_ranges(ranges: &[Range<u64>]) -> (Vec<Range<u64>>, Vec<CoalescedRangePlan>) {
    const COALESCE_GAP_BYTES: u64 = 1024 * 1024;
    const COALESCE_MAX_BYTES: u64 = 16 * 1024 * 1024;

    let mut indexed = ranges
        .iter()
        .enumerate()
        .map(|(index, range)| (index, range.clone()))
        .collect::<Vec<_>>();
    indexed.sort_by(|(_, left), (_, right)| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
    });

    let mut coalesced = Vec::<Range<u64>>::new();
    let mut plans = Vec::with_capacity(ranges.len());
    for (original_index, range) in indexed {
        let next_index = match coalesced.last_mut() {
            Some(last)
                if range.start <= last.end.saturating_add(COALESCE_GAP_BYTES)
                    && range.end.saturating_sub(last.start) <= COALESCE_MAX_BYTES =>
            {
                last.end = last.end.max(range.end);
                coalesced.len() - 1
            }
            _ => {
                coalesced.push(range.clone());
                coalesced.len() - 1
            }
        };
        plans.push(CoalescedRangePlan {
            original_index,
            coalesced_index: next_index,
            start: range.start,
            end: range.end,
        });
    }
    (coalesced, plans)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchIndexDictionaries {
    term_key_groups: Vec<SearchIndexGroupDictionary>,
    term_value_groups: Vec<SearchIndexGroupDictionary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DictionaryRangePlan {
    group_index: usize,
    fst: usize,
    term_infos: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TermDataGroupPlan {
    group_index: usize,
    fst_bytes: Bytes,
    term_info_count: u32,
    terms: Vec<TermDataPlan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TermDataPlan {
    ordinal: usize,
    info: SearchIndexTermInfo,
    postings: usize,
    positions: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CoalescedRangePlan {
    original_index: usize,
    coalesced_index: usize,
    start: u64,
    end: u64,
}

fn filter_candidate_rows(
    rows: Vec<SegmentCandidateRow>,
    matching_rows: &BTreeSet<u32>,
) -> Vec<SegmentCandidateRow> {
    rows.into_iter()
        .filter(|row| {
            u32::try_from(row.row_index)
                .ok()
                .is_some_and(|row_index| matching_rows.contains(&row_index))
        })
        .collect()
}

impl From<&SegmentCandidateRow> for RunKey {
    fn from(row: &SegmentCandidateRow) -> Self {
        Self {
            project_name: row.project_name.clone(),
            trace_id: row.trace_id.clone(),
            span_id: row.span_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{SearchField, SearchQuery, build_search_index, encode_search_index};
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    #[tokio::test]
    async fn rejects_segments_missing_search_indexes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let segment = SegmentSource {
            uri: "projects/demo/a.vortex".to_owned(),
            total_bytes: 10,
            schema_version: crate::segment::SPAN_SEGMENT_SCHEMA_VERSION,
            search_index_uri: None,
            search_index_bytes: 0,
            search_index_schema_version: 0,
            candidate_rows: vec![row(0)],
        };

        let error = load_matching_rows(
            &object_store,
            &segment,
            &SearchPredicate::JsonKey {
                pattern: "inputs.prompt".to_owned(),
            },
            &mut ObjectStoreReadSnapshot::default(),
        )
        .await
        .expect_err("missing index should reject");

        assert!(error.to_string().contains("unsafe payload scan disabled"));
    }

    #[tokio::test]
    async fn filters_candidate_rows_with_search_index() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let records = vec![
            record("first", r#"{"answer":"hello"}"#),
            record("second", r#"{"answer":"world"}"#),
        ];
        let index = build_search_index(&records).expect("build search index");
        let bytes = encode_search_index(&index).expect("encode");
        let full_index_len = bytes.len() as u64;
        object_store
            .put(
                &Path::from("projects/demo/a.search.fst"),
                PutPayload::from_bytes(bytes.clone()),
            )
            .await
            .expect("put search index");

        let segment = SegmentSource {
            uri: "projects/demo/a.vortex".to_owned(),
            total_bytes: 10,
            schema_version: crate::segment::SPAN_SEGMENT_SCHEMA_VERSION,
            search_index_uri: Some("projects/demo/a.search.fst".to_owned()),
            search_index_bytes: 10,
            search_index_schema_version: SEARCH_INDEX_SCHEMA_VERSION,
            candidate_rows: vec![row(0), row(1)],
        };
        let mut stats = ObjectStoreReadSnapshot::default();
        let rows = load_matching_rows(
            &object_store,
            &segment,
            &SearchPredicate::Text {
                field: SearchField::All,
                query: SearchQuery::parse("world"),
            },
            &mut stats,
        )
        .await
        .expect("load rows");

        assert_eq!(
            filter_candidate_rows(segment.candidate_rows, &rows),
            vec![row(1)]
        );
        assert_eq!(stats.get_requests, 0);
        assert_eq!(stats.get_ranges_requests, 4);
        assert!(stats.bytes_read < full_index_len);
    }

    fn row(row_index: i64) -> SegmentCandidateRow {
        SegmentCandidateRow {
            project_name: "demo".to_owned(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: format!("span-{row_index}"),
            row_index,
        }
    }

    fn record(span_id: &str, attributes_json: &str) -> crate::record::SpanRecord {
        crate::record::SpanRecord {
            project_name: "demo".to_owned(),
            run_id: String::new(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: span_id.to_owned(),
            parent_run_id: None,
            parent_span_id: None,
            name: span_id.to_owned(),
            run_type: "chain".to_owned(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            status_code: 1,
            event_kind: crate::record::RunEventKind::End,
            attributes_json: attributes_json.to_owned(),
            idempotency_key: None,
        }
    }
}
