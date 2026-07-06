use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Map, MapBuilder, Streamer};

use crate::otlp::SpanRecord;

mod indexer;
mod predicate;

pub use predicate::{SearchField, SearchPredicate, SearchQuery, tokens_for_text};

pub const SEARCH_INDEX_SCHEMA_VERSION: i64 = 1;
pub const MIN_TOKEN_BYTES: usize = 2;
pub const MAX_TOKEN_BYTES: usize = 64;
pub const MAX_JSON_KEYS_PER_RUN: usize = 256;
pub const MAX_INDEXED_VALUE_BYTES: usize = 4096;
const MAGIC: &[u8; 8] = b"KDBFTS1\0";
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
    term_keys: BTreeMap<Vec<u8>, TermAccumulator>,
    term_values: BTreeMap<Vec<u8>, TermAccumulator>,
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

impl MutableSearchIndex {
    pub(crate) fn new(row_count: usize) -> Self {
        Self {
            row_count: row_count.min(u32::MAX as usize) as u32,
            term_keys: BTreeMap::new(),
            term_values: BTreeMap::new(),
        }
    }

    pub(crate) fn add_path(&mut self, row: u32, path: &str) {
        self.term_keys
            .entry(path.as_bytes().to_vec())
            .or_default()
            .rows
            .insert(row);
    }

    pub(crate) fn add_value_position(&mut self, row: u32, path: &str, token: &str, position: u32) {
        let mut key = Vec::with_capacity(token.len() + 1 + path.len());
        key.extend_from_slice(token.as_bytes());
        key.push(KEY_VALUE_SEPARATOR);
        key.extend_from_slice(path.as_bytes());
        let accumulator = self.term_values.entry(key).or_default();
        accumulator.rows.insert(row);
        accumulator
            .positions_by_row
            .entry(row)
            .or_default()
            .insert(position);
    }

    fn finish(self) -> Result<SearchIndex> {
        Ok(SearchIndex {
            row_count: self.row_count,
            term_key_groups: build_row_groups(self.term_keys, false)?,
            term_value_groups: build_row_groups(self.term_values, true)?,
        })
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

pub fn encode_search_index(index: &SearchIndex) -> Result<Bytes> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    put_u32(&mut out, index.row_count);
    encode_groups(&mut out, &index.term_key_groups)?;
    encode_groups(&mut out, &index.term_value_groups)?;
    Ok(Bytes::from(out))
}

pub fn decode_search_index(bytes: &[u8]) -> Result<SearchIndex> {
    let mut input = ByteReader::new(bytes);
    input.expect_bytes(MAGIC)?;
    let row_count = input.read_u32()?;
    let term_key_groups = input.read_groups()?;
    let term_value_groups = input.read_groups()?;
    input.expect_done()?;
    let index = SearchIndex {
        row_count,
        term_key_groups,
        term_value_groups,
    };
    validate_fsts(&index)?;
    Ok(index)
}

pub fn search_index_uri_for_segment(segment_uri: &str) -> String {
    segment_uri
        .strip_suffix(".vortex")
        .map(|prefix| format!("{prefix}.search.fst"))
        .unwrap_or_else(|| format!("{segment_uri}.search.fst"))
}

fn build_row_groups(
    terms: BTreeMap<Vec<u8>, TermAccumulator>,
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

fn term_value_key(token: &str, path: &str) -> Vec<u8> {
    let mut key = term_value_prefix(token);
    key.extend_from_slice(path.as_bytes());
    key
}

fn term_value_prefix(token: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(token.len() + 1);
    prefix.extend_from_slice(token.as_bytes());
    prefix.push(KEY_VALUE_SEPARATOR);
    prefix
}

fn term_path(term: &[u8]) -> String {
    let path = term
        .iter()
        .position(|byte| *byte == KEY_VALUE_SEPARATOR)
        .map(|separator| &term[separator + 1..])
        .unwrap_or(term);
    String::from_utf8_lossy(path).into_owned()
}

fn group_may_contain_exact(group: &RowGroup, key: &[u8]) -> bool {
    group.min_term.as_slice() <= key && key <= group.max_term.as_slice()
}

fn group_overlaps_prefix(group: &RowGroup, prefix: &[u8]) -> bool {
    if group.max_term.as_slice() < prefix {
        return false;
    }
    next_prefix(prefix)
        .map(|upper| group.min_term.as_slice() < upper.as_slice())
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

fn encode_u32_list(values: &[u32]) -> Vec<u8> {
    let mut deltas = Vec::with_capacity(values.len());
    let mut previous = 0;
    for value in values {
        deltas.push(value.saturating_sub(previous));
        previous = *value;
    }

    let mut out = Vec::new();
    let full_blocks = deltas.len() / BLOCK_LEN;
    for block_index in 0..full_blocks {
        let block = &deltas[block_index * BLOCK_LEN..(block_index + 1) * BLOCK_LEN];
        let width = bit_width(*block.iter().max().unwrap_or(&0));
        out.push(width);
        pack_bits(block, width, &mut out);
    }
    for delta in &deltas[full_blocks * BLOCK_LEN..] {
        put_vint(&mut out, *delta);
    }
    out
}

fn decode_u32_list(bytes: &[u8], count: usize) -> Result<Vec<u32>> {
    let mut input = ByteReader::new(bytes);
    let values = input.read_u32_list(count)?;
    input.expect_done()?;
    Ok(values)
}

fn encode_positions(rows: &[u32], positions_by_row: &BTreeMap<u32, Vec<u32>>) -> Vec<u8> {
    let mut out = Vec::new();
    for row in rows {
        let positions = positions_by_row.get(row).map(Vec::as_slice).unwrap_or(&[]);
        put_vint(&mut out, positions.len() as u32);
        out.extend_from_slice(&encode_u32_list(positions));
    }
    out
}

fn decode_positions(bytes: &[u8], rows: &[u32]) -> Result<BTreeMap<u32, BTreeSet<u32>>> {
    let mut input = ByteReader::new(bytes);
    let mut positions_by_row = BTreeMap::new();
    for row in rows {
        let count = input.read_vint()? as usize;
        let positions = input.read_u32_list(count)?;
        positions_by_row.insert(*row, positions.into_iter().collect());
    }
    input.expect_done()?;
    Ok(positions_by_row)
}

fn bit_width(max: u32) -> u8 {
    if max == 0 {
        0
    } else {
        (u32::BITS - max.leading_zeros()) as u8
    }
}

fn pack_bits(values: &[u32], width: u8, out: &mut Vec<u8>) {
    if width == 0 {
        return;
    }
    let mut accumulator = 0_u64;
    let mut bits = 0_u8;
    for value in values {
        accumulator |= u64::from(*value) << bits;
        bits += width;
        while bits >= 8 {
            out.push(accumulator as u8);
            accumulator >>= 8;
            bits -= 8;
        }
    }
    if bits > 0 {
        out.push(accumulator as u8);
    }
}

fn unpack_bits(bytes: &[u8], count: usize, width: u8) -> Result<Vec<u32>> {
    if width > 32 {
        bail!("invalid bit width {width}");
    }
    if width == 0 {
        return Ok(vec![0; count]);
    }
    let mask = if width == 32 {
        u64::from(u32::MAX)
    } else {
        (1_u64 << width) - 1
    };
    let mut values = Vec::with_capacity(count);
    let mut accumulator = 0_u64;
    let mut bits = 0_u8;
    let mut offset = 0;
    while values.len() < count {
        while bits < width {
            let Some(byte) = bytes.get(offset) else {
                bail!("truncated bitpacked block");
            };
            accumulator |= u64::from(*byte) << bits;
            bits += 8;
            offset += 1;
        }
        values.push((accumulator & mask) as u32);
        accumulator >>= width;
        bits -= width;
    }
    Ok(values)
}

fn packed_len(count: usize, width: u8) -> usize {
    count.saturating_mul(width as usize).div_ceil(8)
}

fn put_vint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn checked_u32(value: usize, label: &str) -> Result<u32> {
    u32::try_from(value).with_context(|| format!("{label} does not fit in u32"))
}

fn checked_range(offset: u32, len: u32, bytes: &[u8]) -> Option<std::ops::Range<usize>> {
    let start = offset as usize;
    let end = start.checked_add(len as usize)?;
    (end <= bytes.len()).then_some(start..end)
}

fn encode_groups(out: &mut Vec<u8>, groups: &[RowGroup]) -> Result<()> {
    put_u32(out, checked_u32(groups.len(), "row group count")?);
    for group in groups {
        put_bytes(out, &group.min_term)?;
        put_bytes(out, &group.max_term)?;
        put_bytes(out, &group.fst_bytes)?;
        put_u32(out, checked_u32(group.term_infos.len(), "term info count")?);
        for info in &group.term_infos {
            put_u32(out, info.doc_count);
            put_u32(out, info.postings_offset);
            put_u32(out, info.postings_len);
            put_u32(out, info.positions_offset);
            put_u32(out, info.positions_len);
        }
        put_bytes(out, &group.postings)?;
        put_bytes(out, &group.positions)?;
    }
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    put_u32(out, checked_u32(bytes.len(), "byte slice len")?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn validate_fsts(index: &SearchIndex) -> Result<()> {
    for group in index
        .term_key_groups
        .iter()
        .chain(index.term_value_groups.iter())
    {
        Map::new(group.fst_bytes.as_slice()).context("validate search FST")?;
    }
    Ok(())
}

fn normalize_like_pattern(pattern: &str) -> String {
    pattern.replace('%', "*")
}

fn simple_trailing_wildcard_prefix(pattern: &str) -> Option<&str> {
    pattern
        .strip_suffix('*')
        .filter(|prefix| !prefix.contains('*'))
}

fn path_matches_pattern(path: &str, pattern: &str) -> bool {
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

struct ByteReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_groups(&mut self) -> Result<Vec<RowGroup>> {
        let group_count = self.read_u32()? as usize;
        (0..group_count)
            .map(|_| {
                let min_term = self.read_vec()?;
                let max_term = self.read_vec()?;
                let fst_bytes = self.read_vec()?;
                let term_info_count = self.read_u32()? as usize;
                let term_infos = (0..term_info_count)
                    .map(|_| {
                        Ok(TermInfo {
                            doc_count: self.read_u32()?,
                            postings_offset: self.read_u32()?,
                            postings_len: self.read_u32()?,
                            positions_offset: self.read_u32()?,
                            positions_len: self.read_u32()?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let postings = self.read_vec()?;
                let positions = self.read_vec()?;
                Ok(RowGroup {
                    min_term,
                    max_term,
                    fst_bytes,
                    term_infos,
                    postings,
                    positions,
                })
            })
            .collect()
    }

    fn read_vec(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        Ok(self.read_slice(len)?.to_vec())
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .context("search index offset overflow")?;
        if end > self.bytes.len() {
            bail!("truncated search index");
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let byte = *self
            .bytes
            .get(self.offset)
            .context("truncated search index byte")?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_slice(4)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("slice len is 4"),
        ))
    }

    fn read_vint(&mut self) -> Result<u32> {
        let mut value = 0_u32;
        let mut shift = 0;
        loop {
            let byte = self.read_u8()?;
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift >= 32 {
                bail!("invalid vint in search index");
            }
        }
    }

    fn read_u32_list(&mut self, count: usize) -> Result<Vec<u32>> {
        let full_blocks = count / BLOCK_LEN;
        let mut values = Vec::with_capacity(count);
        let mut previous = 0_u32;
        for _ in 0..full_blocks {
            let width = self.read_u8()?;
            let packed_len = packed_len(BLOCK_LEN, width);
            let packed = self.read_slice(packed_len)?;
            for delta in unpack_bits(packed, BLOCK_LEN, width)? {
                previous = previous.saturating_add(delta);
                values.push(previous);
            }
        }
        for _ in 0..(count % BLOCK_LEN) {
            let delta = self.read_vint()?;
            previous = previous.saturating_add(delta);
            values.push(previous);
        }
        Ok(values)
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Result<()> {
        let actual = self.read_slice(expected.len())?;
        if actual != expected {
            bail!("invalid search index magic");
        }
        Ok(())
    }

    fn expect_done(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            bail!("search index has trailing bytes");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::RunEventKind;

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
