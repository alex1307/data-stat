use std::{collections::HashMap, fmt::Debug};

use polars::{
    lazy::dsl::{col, lit, Expr},
    prelude::Literal,
    series::Series,
};

pub enum PredicateFilter<T: ToOwned + ToString + Debug + Into<Expr> + Clone + Literal> {
    Like(HashMap<String, T>, bool, bool),
    In(String, Vec<T>),
    Eq(HashMap<String, T>, bool),
    Gt(HashMap<String, T>, bool),
    Lt(HashMap<String, T>, bool),
    Gte(HashMap<String, T>, bool),
    Lte(HashMap<String, T>, bool),
}

struct PriceFilter {
    source: Option<Vec<String>>,
    make: Option<Vec<String>>,
    model: Option<Vec<String>>,
    model_like: Option<String>,
    engine: Option<Vec<String>>,
    gearbox: Option<Vec<String>>,

    year_gt: Option<i32>,
    year_lt: Option<i32>,
    year_eq: Option<i32>,
    year_gte: Option<i32>,
    year_lte: Option<i32>,

    price_eq: Option<i32>,
    price_lt: Option<i32>,
    price_gt: Option<i32>,
    price_lte: Option<i32>,
    price_gte: Option<i32>,

    estimated_price_eq: Option<i32>,
    estimated_price_lt: Option<i32>,
    estimated_price_lte: Option<i32>,
    estimated_price_gt: Option<i32>,
    estimated_price_gte: Option<i32>,

    mileage_eq: Option<i32>,
    mileage_lt: Option<i32>,
    mileage_gt: Option<i32>,
    mileage_lte: Option<i32>,
    mileage_gte: Option<i32>,

    power_eq: Option<i32>,
    power_lt: Option<i32>,
    power_gt: Option<i32>,
    power_lte: Option<i32>,
    power_gte: Option<i32>,

    cc_eq: Option<i32>,
    cc_lt: Option<i32>,
    cc_gt: Option<i32>,
    cc_lte: Option<i32>,
    cc_gte: Option<i32>,

    created_on_before: Option<String>,
    created_on_after: Option<String>,
    created_on_eq: Option<String>,

    last_updated_on_before: Option<String>,
    last_updated_on_after: Option<String>,
    last_updated_on_eq: Option<String>,
}

fn to_like_predicate<T: ToString + ToOwned + Debug + Literal>(
    filter: HashMap<String, T>,
    strict: bool,
    join_and: bool,
) -> Option<polars::lazy::dsl::Expr> {
    let mut predicates = vec![];
    for (c, v) in filter.iter() {
        let p = col(c).str().contains(lit(v.to_string()), false);
        predicates.push(p);
    }
    let mut predicate = predicates[0].clone();
    for p in predicates.iter().skip(1) {
        if join_and {
            predicate = predicate.and(p.clone());
        } else {
            predicate = predicate.or(p.clone());
        }
    }
    Some(predicate)
}

fn to_in_predicate<T: ToOwned + ToString + Debug + Into<Expr> + Clone + Literal>(
    column: &str,
    values: Vec<T>,
) -> Option<polars::lazy::dsl::Expr> {
    if values.is_empty() || column.is_empty() {
        return None;
    }
    let mut predicates = vec![];
    for v in values.iter() {
        let p = col(column).eq(v.clone().into());
        predicates.push(p);
    }
    let mut predicate = predicates[0].clone();
    for p in predicates.iter().skip(1) {
        predicate = predicate.or(p.clone());
    }
    Some(predicate)
}

pub fn to_predicate<T: ToOwned + ToString + Debug + Into<Expr> + Clone + Literal>(
    filter: PredicateFilter<T>,
) -> Option<polars::lazy::dsl::Expr> {
    match filter {
        PredicateFilter::Like(filter, strict, join_and) => {
            to_like_predicate(filter, strict, join_and)
        }
        PredicateFilter::In(column, values) => to_in_predicate(&column, values),
        PredicateFilter::Eq(key_values, join_and) => to_eq_predicate(key_values, join_and),
        _ => None,
    }
}

fn to_eq_predicate<T: Debug + ToOwned + ToString + Into<Expr> + Clone + Literal>(
    key_values: HashMap<String, T>,
    join_and: bool,
) -> Option<polars::lazy::dsl::Expr> {
    if key_values.is_empty() {
        return None;
    }
    let mut predicates = vec![];
    for (c, v) in key_values.iter() {
        println!("column {}: value {}", c, v.to_string());
        let p = col(c).eq(lit(v.clone()));
        predicates.push(p);
    }
    println!("predicates {:?}", predicates);
    let mut predicate = predicates[0].clone();
    for p in predicates.iter().skip(1) {
        if join_and {
            predicate = predicate.and(p.clone());
        } else {
            predicate = predicate.or(p.clone());
        }
    }
    Some(predicate)
}
