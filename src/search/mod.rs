use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;

use anyhow::{Context, Result};
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Map, MapBuilder, Streamer};

use crate::record::SpanRecord;

mod codec;
mod indexer;
mod json_tape;
mod predicate;
mod selection;

pub use codec::{
    SEARCH_INDEX_HEADER_LEN, SearchIndexDirectory, SearchIndexGroupDirectory, SearchIndexHeader,
    SearchIndexRange, SearchIndexTermInfo, decode_search_index_directory,
    decode_search_index_header, decode_search_term_infos, encode_search_term_infos,
};
pub use predicate::{SearchField, SearchPredicate, SearchQuery, tokens_for_text};
pub use selection::{
    SearchIndexGroupDictionary, SearchIndexGroupSelection, SearchIndexGroupTermSlices,
    SearchIndexTermSlice, SearchIndexTermSlices, decode_search_group_dictionary,
    select_search_index_groups, select_search_index_terms,
};

use codec::{
    checked_range, checked_u32, decode_loaded_group, decode_positions, decode_u32_list,
    encode_positions, encode_u32_list,
};

pub const SEARCH_INDEX_SCHEMA_VERSION: i64 = 3;
pub const MIN_TOKEN_BYTES: usize = 2;
pub const MAX_TOKEN_BYTES: usize = 64;
pub const MAX_JSON_KEYS_PER_RUN: usize = 256;
pub const MAX_INDEXED_VALUE_BYTES: usize = 4096;
const BLOCK_LEN: usize = 128;
const KEY_VALUE_SEPARATOR: u8 = 0;
const ROW_GROUP_POSTINGS_BYTES: usize = 32 * 1024 * 1024;
const ROW_GROUP_RAW_TERM_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndex {
    row_count: u32,
    term_key_groups: Vec<RowGroup>,
    term_value_groups: Vec<RowGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutableSearchIndex {
    row_count: u32,
    term_keys: TermInterner,
    term_values: TermInterner,
    key_occurrences: Vec<KeyOccurrence>,
    value_occurrences: Vec<ValueOccurrence>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TermAccumulator {
    rows: BTreeSet<u32>,
    positions_by_row: BTreeMap<u32, BTreeSet<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowGroup {
    min_term: Vec<u8>,
    max_term: Vec<u8>,
    fst_bytes: Vec<u8>,
    term_infos: Vec<TermInfo>,
    postings: Vec<u8>,
    positions: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TermInfo {
    doc_count: u32,
    postings_offset: u32,
    postings_len: u32,
    positions_offset: u32,
    positions_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TermEntry {
    term: Vec<u8>,
    rows: Vec<u32>,
    positions_by_row: BTreeMap<u32, Vec<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedTerm {
    path: String,
    rows: BTreeSet<u32>,
    positions_by_row: BTreeMap<u32, BTreeSet<u32>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TermInterner {
    bytes: Vec<u8>,
    terms: Vec<Range<usize>>,
    lookup: HashMap<Vec<u8>, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyOccurrence {
    term_id: u32,
    row: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValueOccurrence {
    term_id: u32,
    row: u32,
    position: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexGroupBytes {
    pub group_index: usize,
    pub fst_bytes: bytes::Bytes,
    pub term_info_bytes: bytes::Bytes,
    pub postings: bytes::Bytes,
    pub positions: bytes::Bytes,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchIndexChunks {
    pub term_key_groups: Vec<SearchIndexGroupBytes>,
    pub term_value_groups: Vec<SearchIndexGroupBytes>,
}

impl MutableSearchIndex {
    pub(crate) fn new(row_count: usize) -> Self {
        Self {
            row_count: row_count.min(u32::MAX as usize) as u32,
            term_keys: TermInterner::default(),
            term_values: TermInterner::default(),
            key_occurrences: Vec::new(),
            value_occurrences: Vec::new(),
        }
    }

    pub(crate) fn add_path(&mut self, row: u32, path: &str) {
        let term_id = self.term_keys.intern(path.as_bytes());
        self.key_occurrences.push(KeyOccurrence { term_id, row });
    }

    pub(crate) fn add_value_position(&mut self, row: u32, path: &str, token: &str, position: u32) {
        let mut key = Vec::with_capacity(token.len() + 1 + path.len());
        key.extend_from_slice(token.as_bytes());
        key.push(KEY_VALUE_SEPARATOR);
        key.extend_from_slice(path.as_bytes());
        let term_id = self.term_values.intern(&key);
        self.value_occurrences.push(ValueOccurrence {
            term_id,
            row,
            position,
        });
    }

    fn finish(self) -> Result<SearchIndex> {
        let term_key_entries = build_key_entries(self.term_keys, self.key_occurrences);
        let term_value_entries = build_value_entries(self.term_values, self.value_occurrences);
        Ok(SearchIndex {
            row_count: self.row_count,
            term_key_groups: build_row_groups(term_key_entries, false)?,
            term_value_groups: build_row_groups(term_value_entries, true)?,
        })
    }
}

impl TermInterner {
    fn intern(&mut self, term: &[u8]) -> u32 {
        if let Some(term_id) = self.lookup.get(term) {
            return *term_id;
        }
        let start = self.bytes.len();
        self.bytes.extend_from_slice(term);
        let term_id = self.terms.len().min(u32::MAX as usize) as u32;
        self.terms.push(start..self.bytes.len());
        self.lookup.insert(term.to_vec(), term_id);
        term_id
    }

    fn term(&self, term_id: u32) -> &[u8] {
        let range = &self.terms[term_id as usize];
        &self.bytes[range.clone()]
    }
}

impl SearchIndex {
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    pub fn matching_rows(&self, predicate: &SearchPredicate) -> BTreeSet<u32> {
        match predicate {
            SearchPredicate::And(children) => {
                let mut children = children.iter();
                let Some(first) = children.next() else {
                    return self.all_rows();
                };
                let mut rows = self.matching_rows(first);
                for child in children {
                    let child_rows = self.matching_rows(child);
                    rows = rows.intersection(&child_rows).copied().collect();
                }
                rows
            }
            SearchPredicate::Or(children) => {
                children.iter().fold(BTreeSet::new(), |mut rows, child| {
                    rows.extend(self.matching_rows(child));
                    rows
                })
            }
            SearchPredicate::Not(child) => {
                let child_rows = self.matching_rows(child);
                self.all_rows().difference(&child_rows).copied().collect()
            }
            SearchPredicate::Text { field, query } => self.rows_for_query(field, query),
            SearchPredicate::ExactValue { field, value } => self.rows_for_exact_value(field, value),
            SearchPredicate::JsonKey { pattern } => self.rows_for_path_pattern(pattern),
        }
    }

    fn all_rows(&self) -> BTreeSet<u32> {
        (0..self.row_count).collect()
    }

    fn rows_for_query(&self, field: &SearchField, query: &SearchQuery) -> BTreeSet<u32> {
        if query.is_empty() {
            return BTreeSet::new();
        }

        let mut required = query
            .terms()
            .iter()
            .map(|term| self.rows_for_term(field, term))
            .chain(
                query
                    .phrases()
                    .iter()
                    .map(|phrase| self.rows_for_phrase(field, phrase)),
            );
        let Some(mut rows) = required.next() else {
            return BTreeSet::new();
        };
        for term_rows in required {
            rows = rows.intersection(&term_rows).copied().collect();
        }
        rows
    }

    fn rows_for_path_pattern(&self, pattern: &str) -> BTreeSet<u32> {
        let pattern = normalize_like_pattern(pattern);
        if !pattern.contains('*') {
            return self.rows_for_exact_key(&pattern);
        }

        if let Some(prefix) = simple_trailing_wildcard_prefix(&pattern) {
            return self.rows_for_key_prefix(prefix);
        }

        self.term_key_groups
            .iter()
            .flat_map(|group| self.group_path_pattern_rows(group, &pattern))
            .collect()
    }

    fn rows_for_exact_key(&self, key: &str) -> BTreeSet<u32> {
        self.term_key_groups
            .iter()
            .filter(|group| group_may_contain_exact(group, key.as_bytes()))
            .filter_map(|group| group_exact_term_rows(group, key.as_bytes(), false))
            .flat_map(|term| term.rows)
            .collect()
    }

    fn rows_for_key_prefix(&self, prefix: &str) -> BTreeSet<u32> {
        self.term_key_groups
            .iter()
            .filter(|group| group_overlaps_prefix(group, prefix.as_bytes()))
            .flat_map(|group| group_prefix_terms(group, prefix.as_bytes(), false))
            .flat_map(|term| term.rows)
            .collect()
    }

    fn group_path_pattern_rows(&self, group: &RowGroup, pattern: &str) -> BTreeSet<u32> {
        let mut rows = BTreeSet::new();
        let Ok(map) = Map::new(group.fst_bytes.as_slice()) else {
            return rows;
        };
        let mut stream = map.stream();
        while let Some((key, ordinal)) = stream.next() {
            let Ok(path) = std::str::from_utf8(key) else {
                continue;
            };
            if path_matches_pattern(path, pattern)
                && let Some(term) = group_term_by_ordinal(group, ordinal as usize, false)
            {
                rows.extend(term.rows);
            }
        }
        rows
    }

    fn rows_for_term(&self, field: &SearchField, term: &str) -> BTreeSet<u32> {
        self.value_terms_for_token(field, term, false)
            .into_iter()
            .flat_map(|term| term.rows)
            .collect()
    }

    fn rows_for_phrase(&self, field: &SearchField, phrase: &[String]) -> BTreeSet<u32> {
        if phrase.is_empty() {
            return BTreeSet::new();
        }
        if phrase.len() == 1 {
            return self.rows_for_term(field, &phrase[0]);
        }

        let mut per_term = Vec::new();
        for token in phrase {
            let terms = self.value_terms_for_token(field, token, true);
            if terms.is_empty() {
                return BTreeSet::new();
            }
            per_term.push(positions_by_row_path(terms));
        }

        let mut rows = BTreeSet::new();
        for ((row, path), first_positions) in &per_term[0] {
            for position in first_positions {
                let matches =
                    per_term
                        .iter()
                        .enumerate()
                        .skip(1)
                        .all(|(offset, term_positions)| {
                            term_positions
                                .get(&(*row, path.clone()))
                                .is_some_and(|positions| {
                                    positions.contains(&(position + offset as u32))
                                })
                        });
                if matches {
                    rows.insert(*row);
                    break;
                }
            }
        }
        rows
    }

    fn rows_for_exact_value(&self, field: &SearchField, value: &str) -> BTreeSet<u32> {
        self.value_terms_for_token(field, &exact_value_token(value), false)
            .into_iter()
            .flat_map(|term| term.rows)
            .collect()
    }

    fn value_terms_for_token(
        &self,
        field: &SearchField,
        token: &str,
        include_positions: bool,
    ) -> Vec<DecodedTerm> {
        match field {
            SearchField::ExactPath(path) => {
                let key = term_value_key(token, path);
                self.term_value_groups
                    .iter()
                    .filter(|group| group_may_contain_exact(group, &key))
                    .filter_map(|group| group_exact_term_rows(group, &key, include_positions))
                    .collect()
            }
            SearchField::All | SearchField::PathPrefix(_) => {
                let prefix = term_value_prefix(token);
                self.term_value_groups
                    .iter()
                    .filter(|group| group_overlaps_prefix(group, &prefix))
                    .flat_map(|group| group_prefix_terms(group, &prefix, include_positions))
                    .filter(|term| field.matches_path(&term.path))
                    .collect()
            }
        }
    }
}

pub fn build_search_index(records: &[SpanRecord]) -> Result<SearchIndex> {
    indexer::build_search_index(records)
}

pub fn encode_search_index(index: &SearchIndex) -> Result<bytes::Bytes> {
    codec::encode_search_index(index)
}

pub fn decode_search_index(bytes: &[u8]) -> Result<SearchIndex> {
    codec::decode_search_index(bytes)
}

pub fn search_index_uri_for_segment(segment_uri: &str) -> String {
    segment_uri
        .strip_suffix(".vortex")
        .map(|prefix| format!("{prefix}.search.fst"))
        .unwrap_or_else(|| format!("{segment_uri}.search.fst"))
}

pub fn decode_search_index_chunks(
    directory: &SearchIndexDirectory,
    chunks: SearchIndexChunks,
) -> Result<SearchIndex> {
    let term_key_groups = decode_group_chunks(&directory.term_key_groups, chunks.term_key_groups)?;
    let term_value_groups =
        decode_group_chunks(&directory.term_value_groups, chunks.term_value_groups)?;
    Ok(SearchIndex {
        row_count: directory.row_count,
        term_key_groups,
        term_value_groups,
    })
}

fn decode_group_chunks(
    directories: &[SearchIndexGroupDirectory],
    chunks: Vec<SearchIndexGroupBytes>,
) -> Result<Vec<RowGroup>> {
    chunks
        .into_iter()
        .map(|chunk| {
            let directory = directories
                .get(chunk.group_index)
                .context("search index group chunk index out of bounds")?;
            decode_loaded_group(
                directory,
                &chunk.fst_bytes,
                &chunk.term_info_bytes,
                &chunk.postings,
                &chunk.positions,
            )
        })
        .collect()
}

fn build_key_entries(
    interner: TermInterner,
    mut occurrences: Vec<KeyOccurrence>,
) -> Vec<(Vec<u8>, TermAccumulator)> {
    radix_sort_occurrences(&interner, &mut occurrences);
    let mut entries = Vec::new();
    let mut current_term_id = None;
    let mut current = TermAccumulator::default();
    for occurrence in occurrences {
        if Some(occurrence.term_id) != current_term_id {
            if let Some(term_id) = current_term_id {
                entries.push((interner.term(term_id).to_vec(), current));
                current = TermAccumulator::default();
            }
            current_term_id = Some(occurrence.term_id);
        }
        current.rows.insert(occurrence.row);
    }
    if let Some(term_id) = current_term_id {
        entries.push((interner.term(term_id).to_vec(), current));
    }
    entries
}

fn build_value_entries(
    interner: TermInterner,
    mut occurrences: Vec<ValueOccurrence>,
) -> Vec<(Vec<u8>, TermAccumulator)> {
    radix_sort_occurrences(&interner, &mut occurrences);
    let mut entries = Vec::new();
    let mut current_term_id = None;
    let mut current = TermAccumulator::default();
    for occurrence in occurrences {
        if Some(occurrence.term_id) != current_term_id {
            if let Some(term_id) = current_term_id {
                entries.push((interner.term(term_id).to_vec(), current));
                current = TermAccumulator::default();
            }
            current_term_id = Some(occurrence.term_id);
        }
        current.rows.insert(occurrence.row);
        current
            .positions_by_row
            .entry(occurrence.row)
            .or_default()
            .insert(occurrence.position);
    }
    if let Some(term_id) = current_term_id {
        entries.push((interner.term(term_id).to_vec(), current));
    }
    entries
}

fn radix_sort_occurrences<T>(interner: &TermInterner, occurrences: &mut Vec<T>)
where
    T: TermOccurrence + Clone,
{
    *occurrences = radix_sorted_occurrences(interner, occurrences, 0);
}

fn radix_sorted_occurrences<T>(interner: &TermInterner, occurrences: &[T], depth: usize) -> Vec<T>
where
    T: TermOccurrence + Clone,
{
    const SMALL_BUCKET: usize = 32;
    if occurrences.len() <= SMALL_BUCKET {
        let mut sorted = occurrences.to_vec();
        sorted.sort_by(|left, right| compare_occurrence_terms(interner, left, right));
        return sorted;
    }

    let mut buckets = (0..257).map(|_| Vec::new()).collect::<Vec<Vec<T>>>();
    for occurrence in occurrences {
        let bucket = interner
            .term(occurrence.term_id())
            .get(depth)
            .map(|byte| usize::from(*byte) + 1)
            .unwrap_or(0);
        buckets[bucket].push(occurrence.clone());
    }

    let mut sorted = Vec::with_capacity(occurrences.len());
    for (bucket_index, bucket) in buckets.into_iter().enumerate() {
        if bucket.is_empty() {
            continue;
        }
        if bucket_index == 0 || bucket.len() <= SMALL_BUCKET {
            let mut bucket = bucket;
            bucket.sort_by(|left, right| compare_occurrence_terms(interner, left, right));
            sorted.extend(bucket);
        } else {
            sorted.extend(radix_sorted_occurrences(interner, &bucket, depth + 1));
        }
    }
    sorted
}

fn compare_occurrence_terms<T: TermOccurrence>(
    interner: &TermInterner,
    left: &T,
    right: &T,
) -> std::cmp::Ordering {
    interner
        .term(left.term_id())
        .cmp(interner.term(right.term_id()))
}

trait TermOccurrence {
    fn term_id(&self) -> u32;
}

impl TermOccurrence for KeyOccurrence {
    fn term_id(&self) -> u32 {
        self.term_id
    }
}

impl TermOccurrence for ValueOccurrence {
    fn term_id(&self) -> u32 {
        self.term_id
    }
}

fn build_row_groups(
    terms: Vec<(Vec<u8>, TermAccumulator)>,
    include_positions: bool,
) -> Result<Vec<RowGroup>> {
    let entries = terms
        .into_iter()
        .map(|(term, accumulator)| TermEntry {
            term,
            rows: accumulator.rows.into_iter().collect(),
            positions_by_row: accumulator
                .positions_by_row
                .into_iter()
                .map(|(row, positions)| (row, positions.into_iter().collect()))
                .collect(),
        })
        .collect::<Vec<_>>();
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_postings_bytes = 0_usize;
    let mut current_raw_term_bytes = 0_usize;

    for entry in entries {
        let postings_bytes = estimated_term_bytes(&entry, include_positions);
        let raw_term_bytes = entry.term.len();
        if !current.is_empty()
            && (current_postings_bytes.saturating_add(postings_bytes) > ROW_GROUP_POSTINGS_BYTES
                || current_raw_term_bytes.saturating_add(raw_term_bytes) > ROW_GROUP_RAW_TERM_BYTES)
        {
            groups.push(encode_row_group(&current, include_positions)?);
            current.clear();
            current_postings_bytes = 0;
            current_raw_term_bytes = 0;
        }

        current_postings_bytes = current_postings_bytes.saturating_add(postings_bytes);
        current_raw_term_bytes = current_raw_term_bytes.saturating_add(raw_term_bytes);
        current.push(entry);
    }

    if !current.is_empty() {
        groups.push(encode_row_group(&current, include_positions)?);
    }

    Ok(groups)
}

fn estimated_term_bytes(entry: &TermEntry, include_positions: bool) -> usize {
    let postings = encode_u32_list(&entry.rows).len();
    if !include_positions {
        return postings;
    }
    postings + encode_positions(&entry.rows, &entry.positions_by_row).len()
}

fn encode_row_group(entries: &[TermEntry], include_positions: bool) -> Result<RowGroup> {
    let mut fst = MapBuilder::memory();
    let mut term_infos = Vec::with_capacity(entries.len());
    let mut postings = Vec::new();
    let mut positions = Vec::new();

    for (ordinal, entry) in entries.iter().enumerate() {
        fst.insert(&entry.term, ordinal as u64)
            .context("insert search FST term")?;
        let postings_offset = checked_u32(postings.len(), "postings offset")?;
        let encoded_postings = encode_u32_list(&entry.rows);
        postings.extend_from_slice(&encoded_postings);
        let postings_len = checked_u32(encoded_postings.len(), "postings len")?;

        let positions_offset = checked_u32(positions.len(), "positions offset")?;
        let positions_len = if include_positions {
            let encoded_positions = encode_positions(&entry.rows, &entry.positions_by_row);
            positions.extend_from_slice(&encoded_positions);
            checked_u32(encoded_positions.len(), "positions len")?
        } else {
            0
        };

        term_infos.push(TermInfo {
            doc_count: checked_u32(entry.rows.len(), "doc count")?,
            postings_offset,
            postings_len,
            positions_offset,
            positions_len,
        });
    }

    Ok(RowGroup {
        min_term: entries
            .first()
            .map(|entry| entry.term.clone())
            .unwrap_or_default(),
        max_term: entries
            .last()
            .map(|entry| entry.term.clone())
            .unwrap_or_default(),
        fst_bytes: fst.into_inner().context("finish search FST")?,
        term_infos,
        postings,
        positions,
    })
}

fn group_exact_term_rows(
    group: &RowGroup,
    key: &[u8],
    include_positions: bool,
) -> Option<DecodedTerm> {
    let map = Map::new(group.fst_bytes.as_slice()).ok()?;
    let ordinal = map.get(key)? as usize;
    group_term_by_ordinal(group, ordinal, include_positions)
}

fn group_prefix_terms(
    group: &RowGroup,
    prefix: &[u8],
    include_positions: bool,
) -> Vec<DecodedTerm> {
    let Ok(map) = Map::new(group.fst_bytes.as_slice()) else {
        return Vec::new();
    };
    let Ok(prefix) = std::str::from_utf8(prefix) else {
        return Vec::new();
    };
    let automaton = Str::new(prefix).starts_with();
    let mut stream = map.search(automaton).into_stream();
    let mut terms = Vec::new();
    while let Some((_key, ordinal)) = stream.next() {
        if let Some(term) = group_term_by_ordinal(group, ordinal as usize, include_positions) {
            terms.push(term);
        }
    }
    terms
}

fn group_term_by_ordinal(
    group: &RowGroup,
    ordinal: usize,
    include_positions: bool,
) -> Option<DecodedTerm> {
    let info = *group.term_infos.get(ordinal)?;
    let term = term_by_ordinal(group, ordinal)?;
    let path = term_path(&term);
    let postings_range = checked_range(info.postings_offset, info.postings_len, &group.postings)?;
    let rows = decode_u32_list(&group.postings[postings_range], info.doc_count as usize).ok()?;
    let positions_by_row = if include_positions && info.positions_len > 0 {
        let positions_range =
            checked_range(info.positions_offset, info.positions_len, &group.positions)?;
        decode_positions(&group.positions[positions_range], &rows).ok()?
    } else {
        BTreeMap::new()
    };

    Some(DecodedTerm {
        path,
        rows: rows.into_iter().collect(),
        positions_by_row,
    })
}

fn term_by_ordinal(group: &RowGroup, ordinal: usize) -> Option<Vec<u8>> {
    let map = Map::new(group.fst_bytes.as_slice()).ok()?;
    let mut stream = map.stream();
    while let Some((key, value)) = stream.next() {
        if value as usize == ordinal {
            return Some(key.to_vec());
        }
    }
    None
}

fn positions_by_row_path(terms: Vec<DecodedTerm>) -> BTreeMap<(u32, String), BTreeSet<u32>> {
    let mut positions = BTreeMap::new();
    for term in terms {
        for (row, row_positions) in term.positions_by_row {
            positions
                .entry((row, term.path.clone()))
                .or_insert_with(BTreeSet::new)
                .extend(row_positions);
        }
    }
    positions
}

pub(super) fn term_value_key(token: &str, path: &str) -> Vec<u8> {
    let mut key = term_value_prefix(token);
    key.extend_from_slice(path.as_bytes());
    key
}

pub(super) fn term_value_prefix(token: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(token.len() + 1);
    prefix.extend_from_slice(token.as_bytes());
    prefix.push(KEY_VALUE_SEPARATOR);
    prefix
}

pub(super) fn exact_value_token(value: &str) -> String {
    format!("={}", hex::encode(value.as_bytes()))
}

pub(super) fn term_path(term: &[u8]) -> String {
    let path = term
        .iter()
        .position(|byte| *byte == KEY_VALUE_SEPARATOR)
        .map(|separator| &term[separator + 1..])
        .unwrap_or(term);
    String::from_utf8_lossy(path).into_owned()
}

fn group_may_contain_exact(group: &RowGroup, key: &[u8]) -> bool {
    min_max_may_contain_exact(&group.min_term, &group.max_term, key)
}

fn group_overlaps_prefix(group: &RowGroup, prefix: &[u8]) -> bool {
    min_max_overlaps_prefix(&group.min_term, &group.max_term, prefix)
}

pub(super) fn min_max_may_contain_exact(min_term: &[u8], max_term: &[u8], key: &[u8]) -> bool {
    min_term <= key && key <= max_term
}

pub(super) fn min_max_overlaps_prefix(min_term: &[u8], max_term: &[u8], prefix: &[u8]) -> bool {
    if max_term < prefix {
        return false;
    }
    next_prefix(prefix)
        .map(|upper| min_term < upper.as_slice())
        .unwrap_or(true)
}

fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last != u8::MAX {
            upper.push(last + 1);
            return Some(upper);
        }
    }
    None
}

pub(super) fn normalize_like_pattern(pattern: &str) -> String {
    pattern.replace('%', "*")
}

pub(super) fn simple_trailing_wildcard_prefix(pattern: &str) -> Option<&str> {
    pattern
        .strip_suffix('*')
        .filter(|prefix| !prefix.contains('*'))
}

pub(super) fn path_matches_pattern(path: &str, pattern: &str) -> bool {
    if !pattern.contains('*') {
        return path == pattern;
    }

    let mut remaining = path;
    let mut first = true;
    for part in pattern.split('*') {
        if part.is_empty() {
            continue;
        }
        if first && !pattern.starts_with('*') {
            let Some(rest) = remaining.strip_prefix(part) else {
                return false;
            };
            remaining = rest;
            first = false;
            continue;
        }
        let Some(index) = remaining.find(part) else {
            return false;
        };
        remaining = &remaining[index + part.len()..];
        first = false;
    }

    pattern.ends_with('*') || remaining.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RunEventKind;
    use crate::search::codec::MAGIC;

    #[test]
    fn evaluates_full_text_path_and_phrase_queries() {
        let records = vec![record(
            "root",
            r#"{"langsmith.inputs":{"prompt":"hello brave world"},"metadata":{"thread_id":"abc"}}"#,
        )];
        let index = build_search_index(&records).expect("build");

        let search = SearchPredicate::Text {
            field: SearchField::All,
            query: SearchQuery::parse("hello world"),
        };
        assert_eq!(index.matching_rows(&search), [0].into_iter().collect());

        let phrase = SearchPredicate::Text {
            field: SearchField::ExactPath("langsmith.inputs.prompt".to_owned()),
            query: SearchQuery::parse("\"brave world\""),
        };
        assert_eq!(index.matching_rows(&phrase), [0].into_iter().collect());

        let path = SearchPredicate::JsonKey {
            pattern: "metadata.%".to_owned(),
        };
        assert_eq!(index.matching_rows(&path), [0].into_iter().collect());
    }

    #[test]
    fn round_trips_encoded_fst_index() {
        let records = vec![record("root", r#"{"answer":"world"}"#)];
        let index = build_search_index(&records).expect("build");
        let bytes = encode_search_index(&index).expect("encode");
        assert!(bytes.starts_with(MAGIC));
        let decoded = decode_search_index(&bytes).expect("decode");
        assert_eq!(decoded, index);
    }

    #[test]
    fn block_delta_codec_round_trips_full_blocks_and_tail() {
        let values = (0..300).map(|index| index * 3).collect::<Vec<_>>();
        let encoded = encode_u32_list(&values);
        assert!(encoded.len() < values.len() * std::mem::size_of::<u32>());
        assert_eq!(decode_u32_list(&encoded, values.len()).unwrap(), values);
    }

    fn record(name: &str, attributes_json: &str) -> SpanRecord {
        SpanRecord {
            project_name: "demo".to_owned(),
            run_id: String::new(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: name.to_owned(),
            parent_run_id: None,
            parent_span_id: None,
            name: name.to_owned(),
            run_type: "chain".to_owned(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            status_code: 1,
            event_kind: RunEventKind::End,
            attributes_json: attributes_json.to_owned(),
            idempotency_key: None,
        }
    }
}
