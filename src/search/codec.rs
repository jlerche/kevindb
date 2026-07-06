use std::ops::Range;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use fst::Map;

use super::{BLOCK_LEN, RowGroup, SearchIndex, TermInfo};

pub(super) const MAGIC: &[u8; 8] = b"KDBFTS2\0";
pub const SEARCH_INDEX_HEADER_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchIndexHeader {
    pub row_count: u32,
    pub term_key_group_count: u32,
    pub term_value_group_count: u32,
    pub directory_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexDirectory {
    pub row_count: u32,
    pub term_key_groups: Vec<SearchIndexGroupDirectory>,
    pub term_value_groups: Vec<SearchIndexGroupDirectory>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexGroupDirectory {
    pub min_term: Vec<u8>,
    pub max_term: Vec<u8>,
    pub term_info_count: u32,
    pub fst: SearchIndexRange,
    pub term_infos: SearchIndexRange,
    pub postings: SearchIndexRange,
    pub positions: SearchIndexRange,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchIndexRange {
    pub offset: u64,
    pub len: u64,
}

impl SearchIndexRange {
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn as_range(self) -> Range<u64> {
        self.offset..self.offset.saturating_add(self.len)
    }
}

pub(super) fn encode_search_index(index: &SearchIndex) -> Result<Bytes> {
    let mut key_groups = prepare_groups(&index.term_key_groups)?;
    let mut value_groups = prepare_groups(&index.term_value_groups)?;
    let directory_len = checked_u32(
        encoded_directory_len(&key_groups, &value_groups),
        "search directory len",
    )?;
    let mut offset = SEARCH_INDEX_HEADER_LEN as u64 + u64::from(directory_len);

    for group in key_groups.iter_mut().chain(value_groups.iter_mut()) {
        group.directory.fst = next_range(&mut offset, group.fst_bytes.len())?;
        group.directory.term_infos = next_range(&mut offset, group.term_infos.len())?;
        group.directory.postings = next_range(&mut offset, group.postings.len())?;
        group.directory.positions = next_range(&mut offset, group.positions.len())?;
    }

    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    put_u32(&mut out, index.row_count);
    put_u32(
        &mut out,
        checked_u32(key_groups.len(), "term_key group count")?,
    );
    put_u32(
        &mut out,
        checked_u32(value_groups.len(), "term_value group count")?,
    );
    put_u32(&mut out, directory_len);
    encode_directory(&mut out, &key_groups, &value_groups)?;

    for group in key_groups.iter().chain(value_groups.iter()) {
        out.extend_from_slice(group.fst_bytes);
        out.extend_from_slice(&group.term_infos);
        out.extend_from_slice(group.postings);
        out.extend_from_slice(group.positions);
    }
    Ok(Bytes::from(out))
}

pub(super) fn decode_search_index(bytes: &[u8]) -> Result<SearchIndex> {
    let header = decode_search_index_header(bytes)?;
    let directory_end = SEARCH_INDEX_HEADER_LEN
        .checked_add(header.directory_len as usize)
        .context("search directory offset overflow")?;
    if directory_end > bytes.len() {
        bail!("truncated search index directory");
    }
    let directory =
        decode_search_index_directory(header, &bytes[SEARCH_INDEX_HEADER_LEN..directory_end])?;
    let term_key_groups = decode_full_groups(bytes, &directory.term_key_groups)?;
    let term_value_groups = decode_full_groups(bytes, &directory.term_value_groups)?;
    Ok(SearchIndex {
        row_count: directory.row_count,
        term_key_groups,
        term_value_groups,
    })
}

pub fn decode_search_index_header(bytes: &[u8]) -> Result<SearchIndexHeader> {
    let mut input = ByteReader::new(bytes);
    input.expect_bytes(MAGIC)?;
    Ok(SearchIndexHeader {
        row_count: input.read_u32()?,
        term_key_group_count: input.read_u32()?,
        term_value_group_count: input.read_u32()?,
        directory_len: input.read_u32()?,
    })
}

pub fn decode_search_index_directory(
    header: SearchIndexHeader,
    bytes: &[u8],
) -> Result<SearchIndexDirectory> {
    if bytes.len() != header.directory_len as usize {
        bail!("search directory len mismatch");
    }
    let mut input = ByteReader::new(bytes);
    let term_key_groups = input.read_group_directories(header.term_key_group_count as usize)?;
    let term_value_groups = input.read_group_directories(header.term_value_group_count as usize)?;
    input.expect_done()?;
    Ok(SearchIndexDirectory {
        row_count: header.row_count,
        term_key_groups,
        term_value_groups,
    })
}

pub(super) fn decode_loaded_group(
    directory: &SearchIndexGroupDirectory,
    fst_bytes: &[u8],
    term_info_bytes: &[u8],
    postings: &[u8],
    positions: &[u8],
) -> Result<RowGroup> {
    Map::new(fst_bytes).context("validate search FST")?;
    Ok(RowGroup {
        min_term: directory.min_term.clone(),
        max_term: directory.max_term.clone(),
        fst_bytes: fst_bytes.to_vec(),
        term_infos: decode_term_infos(term_info_bytes, directory.term_info_count as usize)?,
        postings: postings.to_vec(),
        positions: positions.to_vec(),
    })
}

pub(super) fn encode_u32_list(values: &[u32]) -> Vec<u8> {
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

pub(super) fn decode_u32_list(bytes: &[u8], count: usize) -> Result<Vec<u32>> {
    let mut input = ByteReader::new(bytes);
    let values = input.read_u32_list(count)?;
    input.expect_done()?;
    Ok(values)
}

pub(super) fn encode_positions(
    rows: &[u32],
    positions_by_row: &std::collections::BTreeMap<u32, Vec<u32>>,
) -> Vec<u8> {
    let mut out = Vec::new();
    for row in rows {
        let positions = positions_by_row.get(row).map(Vec::as_slice).unwrap_or(&[]);
        put_vint(&mut out, positions.len() as u32);
        out.extend_from_slice(&encode_u32_list(positions));
    }
    out
}

pub(super) fn decode_positions(
    bytes: &[u8],
    rows: &[u32],
) -> Result<std::collections::BTreeMap<u32, std::collections::BTreeSet<u32>>> {
    let mut input = ByteReader::new(bytes);
    let mut positions_by_row = std::collections::BTreeMap::new();
    for row in rows {
        let count = input.read_vint()? as usize;
        let positions = input.read_u32_list(count)?;
        positions_by_row.insert(*row, positions.into_iter().collect());
    }
    input.expect_done()?;
    Ok(positions_by_row)
}

pub(super) fn checked_u32(value: usize, label: &str) -> Result<u32> {
    u32::try_from(value).with_context(|| format!("{label} does not fit in u32"))
}

pub(super) fn checked_range(offset: u32, len: u32, bytes: &[u8]) -> Option<Range<usize>> {
    let start = offset as usize;
    let end = start.checked_add(len as usize)?;
    (end <= bytes.len()).then_some(start..end)
}

fn prepare_groups(groups: &[RowGroup]) -> Result<Vec<EncodedGroup<'_>>> {
    groups.iter().map(EncodedGroup::new).collect()
}

fn encoded_directory_len(
    key_groups: &[EncodedGroup<'_>],
    value_groups: &[EncodedGroup<'_>],
) -> usize {
    key_groups
        .iter()
        .chain(value_groups.iter())
        .map(|group| {
            4 + group.directory.min_term.len() + 4 + group.directory.max_term.len() + 4 + 64
        })
        .sum()
}

fn encode_directory(
    out: &mut Vec<u8>,
    key_groups: &[EncodedGroup<'_>],
    value_groups: &[EncodedGroup<'_>],
) -> Result<()> {
    for group in key_groups.iter().chain(value_groups.iter()) {
        put_bytes(out, &group.directory.min_term)?;
        put_bytes(out, &group.directory.max_term)?;
        put_u32(out, group.directory.term_info_count);
        put_range(out, group.directory.fst);
        put_range(out, group.directory.term_infos);
        put_range(out, group.directory.postings);
        put_range(out, group.directory.positions);
    }
    Ok(())
}

fn decode_full_groups(bytes: &[u8], groups: &[SearchIndexGroupDirectory]) -> Result<Vec<RowGroup>> {
    groups
        .iter()
        .map(|group| {
            let fst = slice_range(bytes, group.fst)?;
            let term_infos = slice_range(bytes, group.term_infos)?;
            let postings = slice_range(bytes, group.postings)?;
            let positions = slice_range(bytes, group.positions)?;
            decode_loaded_group(group, fst, term_infos, postings, positions)
        })
        .collect()
}

fn encode_term_infos(term_infos: &[TermInfo]) -> Vec<u8> {
    let mut out = Vec::with_capacity(term_infos.len() * 20);
    for info in term_infos {
        put_u32(&mut out, info.doc_count);
        put_u32(&mut out, info.postings_offset);
        put_u32(&mut out, info.postings_len);
        put_u32(&mut out, info.positions_offset);
        put_u32(&mut out, info.positions_len);
    }
    out
}

fn decode_term_infos(bytes: &[u8], expected_count: usize) -> Result<Vec<TermInfo>> {
    let expected_len = expected_count
        .checked_mul(20)
        .context("term_info len overflow")?;
    if bytes.len() != expected_len {
        bail!("term_info len mismatch");
    }
    let mut input = ByteReader::new(bytes);
    let term_infos = (0..expected_count)
        .map(|_| {
            Ok(TermInfo {
                doc_count: input.read_u32()?,
                postings_offset: input.read_u32()?,
                postings_len: input.read_u32()?,
                positions_offset: input.read_u32()?,
                positions_len: input.read_u32()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    input.expect_done()?;
    Ok(term_infos)
}

fn next_range(offset: &mut u64, len: usize) -> Result<SearchIndexRange> {
    let len = u64::try_from(len).context("search range len does not fit in u64")?;
    let range = SearchIndexRange {
        offset: *offset,
        len,
    };
    *offset = offset
        .checked_add(len)
        .context("search range offset overflow")?;
    Ok(range)
}

fn slice_range(bytes: &[u8], range: SearchIndexRange) -> Result<&[u8]> {
    let start = usize::try_from(range.offset).context("search range offset does not fit usize")?;
    let len = usize::try_from(range.len).context("search range len does not fit usize")?;
    let end = start
        .checked_add(len)
        .context("search range offset overflow")?;
    if end > bytes.len() {
        bail!("truncated search index range");
    }
    Ok(&bytes[start..end])
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

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    put_u32(out, checked_u32(bytes.len(), "byte slice len")?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn put_range(out: &mut Vec<u8>, range: SearchIndexRange) {
    put_u64(out, range.offset);
    put_u64(out, range.len);
}

struct EncodedGroup<'a> {
    directory: SearchIndexGroupDirectory,
    fst_bytes: &'a [u8],
    term_infos: Vec<u8>,
    postings: &'a [u8],
    positions: &'a [u8],
}

impl<'a> EncodedGroup<'a> {
    fn new(group: &'a RowGroup) -> Result<Self> {
        Ok(Self {
            directory: SearchIndexGroupDirectory {
                min_term: group.min_term.clone(),
                max_term: group.max_term.clone(),
                term_info_count: checked_u32(group.term_infos.len(), "term info count")?,
                fst: SearchIndexRange::default(),
                term_infos: SearchIndexRange::default(),
                postings: SearchIndexRange::default(),
                positions: SearchIndexRange::default(),
            },
            fst_bytes: &group.fst_bytes,
            term_infos: encode_term_infos(&group.term_infos),
            postings: &group.postings,
            positions: &group.positions,
        })
    }
}

struct ByteReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_group_directories(
        &mut self,
        group_count: usize,
    ) -> Result<Vec<SearchIndexGroupDirectory>> {
        (0..group_count)
            .map(|_| {
                Ok(SearchIndexGroupDirectory {
                    min_term: self.read_vec()?,
                    max_term: self.read_vec()?,
                    term_info_count: self.read_u32()?,
                    fst: self.read_range()?,
                    term_infos: self.read_range()?,
                    postings: self.read_range()?,
                    positions: self.read_range()?,
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

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_slice(8)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().expect("slice len is 8"),
        ))
    }

    fn read_range(&mut self) -> Result<SearchIndexRange> {
        Ok(SearchIndexRange {
            offset: self.read_u64()?,
            len: self.read_u64()?,
        })
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

    #[test]
    fn block_delta_codec_round_trips_full_blocks_and_tail() {
        let values = (0..300).map(|index| index * 3).collect::<Vec<_>>();
        let encoded = encode_u32_list(&values);
        assert!(encoded.len() < values.len() * std::mem::size_of::<u32>());
        assert_eq!(decode_u32_list(&encoded, values.len()).unwrap(), values);
    }
}
