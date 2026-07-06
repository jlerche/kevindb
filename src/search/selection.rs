use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bytes::Bytes;
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Map, Streamer};

use super::{
    SearchField, SearchIndexDirectory, SearchIndexTermInfo, SearchPredicate,
    decode_search_term_infos, exact_value_token, min_max_may_contain_exact,
    min_max_overlaps_prefix, normalize_like_pattern, path_matches_pattern,
    simple_trailing_wildcard_prefix, term_path, term_value_key, term_value_prefix,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchIndexGroupSelection {
    pub term_key_groups: BTreeSet<usize>,
    pub term_value_groups: BTreeSet<usize>,
    pub include_positions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexGroupDictionary {
    pub group_index: usize,
    pub fst_bytes: Bytes,
    pub term_infos: Vec<SearchIndexTermInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchIndexTermSlice {
    pub ordinal: usize,
    pub info: SearchIndexTermInfo,
    pub include_positions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexGroupTermSlices {
    pub group_index: usize,
    pub fst_bytes: Bytes,
    pub term_info_count: u32,
    pub terms: Vec<SearchIndexTermSlice>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchIndexTermSlices {
    pub term_key_groups: Vec<SearchIndexGroupTermSlices>,
    pub term_value_groups: Vec<SearchIndexGroupTermSlices>,
}

pub fn select_search_index_groups(
    directory: &SearchIndexDirectory,
    predicate: &SearchPredicate,
) -> SearchIndexGroupSelection {
    let mut selection = SearchIndexGroupSelection::default();
    select_groups_for_predicate(directory, predicate, &mut selection);
    selection
}

pub fn decode_search_group_dictionary(
    group_index: usize,
    fst_bytes: Bytes,
    term_info_bytes: &[u8],
    term_info_count: u32,
) -> Result<SearchIndexGroupDictionary> {
    Map::new(fst_bytes.as_ref()).context("validate search FST")?;
    Ok(SearchIndexGroupDictionary {
        group_index,
        fst_bytes,
        term_infos: decode_search_term_infos(term_info_bytes, term_info_count as usize)?,
    })
}

pub fn select_search_index_terms(
    predicate: &SearchPredicate,
    term_key_groups: &[SearchIndexGroupDictionary],
    term_value_groups: &[SearchIndexGroupDictionary],
) -> SearchIndexTermSlices {
    let mut key_terms = BTreeMap::<usize, BTreeMap<usize, bool>>::new();
    let mut value_terms = BTreeMap::<usize, BTreeMap<usize, bool>>::new();
    select_terms_for_predicate(
        predicate,
        term_key_groups,
        term_value_groups,
        &mut key_terms,
        &mut value_terms,
    );
    SearchIndexTermSlices {
        term_key_groups: materialize_term_slices(term_key_groups, key_terms),
        term_value_groups: materialize_term_slices(term_value_groups, value_terms),
    }
}

fn select_groups_for_predicate(
    directory: &SearchIndexDirectory,
    predicate: &SearchPredicate,
    selection: &mut SearchIndexGroupSelection,
) {
    match predicate {
        SearchPredicate::And(children) | SearchPredicate::Or(children) => {
            for child in children {
                select_groups_for_predicate(directory, child, selection);
            }
        }
        SearchPredicate::Not(child) => select_groups_for_predicate(directory, child, selection),
        SearchPredicate::Text { field, query } => {
            select_value_groups_for_query(directory, field, query, selection);
        }
        SearchPredicate::ExactValue { field, value } => {
            select_value_groups_for_token(
                directory,
                field,
                &exact_value_token(value),
                false,
                selection,
            );
        }
        SearchPredicate::JsonKey { pattern } => {
            select_key_groups_for_pattern(directory, pattern, selection);
        }
    }
}

fn select_terms_for_predicate(
    predicate: &SearchPredicate,
    term_key_groups: &[SearchIndexGroupDictionary],
    term_value_groups: &[SearchIndexGroupDictionary],
    key_terms: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
    value_terms: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
) {
    match predicate {
        SearchPredicate::And(children) | SearchPredicate::Or(children) => {
            for child in children {
                select_terms_for_predicate(
                    child,
                    term_key_groups,
                    term_value_groups,
                    key_terms,
                    value_terms,
                );
            }
        }
        SearchPredicate::Not(child) => select_terms_for_predicate(
            child,
            term_key_groups,
            term_value_groups,
            key_terms,
            value_terms,
        ),
        SearchPredicate::Text { field, query } => {
            select_value_terms_for_query(term_value_groups, field, query, value_terms);
        }
        SearchPredicate::ExactValue { field, value } => {
            select_value_terms_for_token(
                term_value_groups,
                field,
                &exact_value_token(value),
                false,
                value_terms,
            );
        }
        SearchPredicate::JsonKey { pattern } => {
            select_key_terms_for_pattern(term_key_groups, pattern, key_terms);
        }
    }
}

fn select_key_groups_for_pattern(
    directory: &SearchIndexDirectory,
    pattern: &str,
    selection: &mut SearchIndexGroupSelection,
) {
    let pattern = normalize_like_pattern(pattern);
    if !pattern.contains('*') {
        for (index, group) in directory.term_key_groups.iter().enumerate() {
            if min_max_may_contain_exact(&group.min_term, &group.max_term, pattern.as_bytes()) {
                selection.term_key_groups.insert(index);
            }
        }
        return;
    }

    if let Some(prefix) = simple_trailing_wildcard_prefix(&pattern) {
        for (index, group) in directory.term_key_groups.iter().enumerate() {
            if min_max_overlaps_prefix(&group.min_term, &group.max_term, prefix.as_bytes()) {
                selection.term_key_groups.insert(index);
            }
        }
        return;
    }

    selection
        .term_key_groups
        .extend(0..directory.term_key_groups.len());
}

fn select_key_terms_for_pattern(
    groups: &[SearchIndexGroupDictionary],
    pattern: &str,
    selected: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
) {
    let pattern = normalize_like_pattern(pattern);
    if !pattern.contains('*') {
        select_exact_terms(groups, pattern.as_bytes(), false, selected);
        return;
    }

    if let Some(prefix) = simple_trailing_wildcard_prefix(&pattern) {
        select_prefix_terms(groups, prefix.as_bytes(), SearchField::All, false, selected);
        return;
    }

    for group in groups {
        let Ok(map) = Map::new(group.fst_bytes.as_ref()) else {
            continue;
        };
        let mut stream = map.stream();
        while let Some((key, ordinal)) = stream.next() {
            let Ok(path) = std::str::from_utf8(key) else {
                continue;
            };
            if path_matches_pattern(path, &pattern) {
                merge_selected_term(selected, group.group_index, ordinal as usize, false);
            }
        }
    }
}

fn select_value_groups_for_query(
    directory: &SearchIndexDirectory,
    field: &SearchField,
    query: &super::SearchQuery,
    selection: &mut SearchIndexGroupSelection,
) {
    for term in query.terms() {
        select_value_groups_for_token(directory, field, term, false, selection);
    }
    for phrase in query.phrases() {
        let needs_positions = phrase.len() > 1;
        for term in phrase {
            select_value_groups_for_token(directory, field, term, needs_positions, selection);
        }
    }
}

fn select_value_groups_for_token(
    directory: &SearchIndexDirectory,
    field: &SearchField,
    token: &str,
    include_positions: bool,
    selection: &mut SearchIndexGroupSelection,
) {
    if include_positions {
        selection.include_positions = true;
    }
    match field {
        SearchField::ExactPath(path) => {
            let key = term_value_key(token, path);
            for (index, group) in directory.term_value_groups.iter().enumerate() {
                if min_max_may_contain_exact(&group.min_term, &group.max_term, &key) {
                    selection.term_value_groups.insert(index);
                }
            }
        }
        SearchField::All | SearchField::PathPrefix(_) => {
            let prefix = term_value_prefix(token);
            for (index, group) in directory.term_value_groups.iter().enumerate() {
                if min_max_overlaps_prefix(&group.min_term, &group.max_term, &prefix) {
                    selection.term_value_groups.insert(index);
                }
            }
        }
    }
}

fn select_value_terms_for_query(
    groups: &[SearchIndexGroupDictionary],
    field: &SearchField,
    query: &super::SearchQuery,
    selected: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
) {
    for term in query.terms() {
        select_value_terms_for_token(groups, field, term, false, selected);
    }
    for phrase in query.phrases() {
        let include_positions = phrase.len() > 1;
        for term in phrase {
            select_value_terms_for_token(groups, field, term, include_positions, selected);
        }
    }
}

fn select_value_terms_for_token(
    groups: &[SearchIndexGroupDictionary],
    field: &SearchField,
    token: &str,
    include_positions: bool,
    selected: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
) {
    match field {
        SearchField::ExactPath(path) => {
            select_exact_terms(
                groups,
                &term_value_key(token, path),
                include_positions,
                selected,
            );
        }
        SearchField::All | SearchField::PathPrefix(_) => {
            select_prefix_terms(
                groups,
                &term_value_prefix(token),
                field.clone(),
                include_positions,
                selected,
            );
        }
    }
}

fn select_exact_terms(
    groups: &[SearchIndexGroupDictionary],
    key: &[u8],
    include_positions: bool,
    selected: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
) {
    for group in groups {
        let Ok(map) = Map::new(group.fst_bytes.as_ref()) else {
            continue;
        };
        if let Some(ordinal) = map.get(key) {
            merge_selected_term(
                selected,
                group.group_index,
                ordinal as usize,
                include_positions,
            );
        }
    }
}

fn select_prefix_terms(
    groups: &[SearchIndexGroupDictionary],
    prefix: &[u8],
    field: SearchField,
    include_positions: bool,
    selected: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
) {
    let Ok(prefix) = std::str::from_utf8(prefix) else {
        return;
    };
    let automaton = Str::new(prefix).starts_with();
    for group in groups {
        let Ok(map) = Map::new(group.fst_bytes.as_ref()) else {
            continue;
        };
        let mut stream = map.search(automaton.clone()).into_stream();
        while let Some((key, ordinal)) = stream.next() {
            if field.matches_path(&term_path(key)) {
                merge_selected_term(
                    selected,
                    group.group_index,
                    ordinal as usize,
                    include_positions,
                );
            }
        }
    }
}

fn merge_selected_term(
    selected: &mut BTreeMap<usize, BTreeMap<usize, bool>>,
    group_index: usize,
    ordinal: usize,
    include_positions: bool,
) {
    let entry = selected
        .entry(group_index)
        .or_default()
        .entry(ordinal)
        .or_insert(false);
    *entry |= include_positions;
}

fn materialize_term_slices(
    groups: &[SearchIndexGroupDictionary],
    selected: BTreeMap<usize, BTreeMap<usize, bool>>,
) -> Vec<SearchIndexGroupTermSlices> {
    selected
        .into_iter()
        .filter_map(|(group_index, ordinals)| {
            let group = groups
                .iter()
                .find(|group| group.group_index == group_index)?;
            let terms = ordinals
                .into_iter()
                .filter_map(|(ordinal, include_positions)| {
                    group
                        .term_infos
                        .get(ordinal)
                        .copied()
                        .map(|info| SearchIndexTermSlice {
                            ordinal,
                            info,
                            include_positions,
                        })
                })
                .collect::<Vec<_>>();
            (!terms.is_empty()).then_some(SearchIndexGroupTermSlices {
                group_index,
                fst_bytes: group.fst_bytes.clone(),
                term_info_count: group.term_infos.len() as u32,
                terms,
            })
        })
        .collect()
}
