use std::collections::HashMap;

use chrono::{NaiveDate, TimeDelta, Utc};
use log::info;
use polars::prelude::{col, lit, Expr, Literal};

use crate::model::AxumAPIModel::StatisticSearchPayload;

pub fn to_aggregator(aggregators: Vec<String>, column: &str) -> Vec<Expr> {
    let mut agg = vec![];
    for aggregator in aggregators {
        let func = get_aggregator(column, &aggregator);
        if let Some(agg_func) = func {
            agg.push(agg_func.alias(aggregator));
        }
    }
    agg
}
pub fn generate_colors(count: usize) -> Vec<String> {
    let palette = vec![
        "rgba(75, 192, 192, 0.8)",  // Teal
        "rgba(255, 99, 132, 0.8)",  // Pink
        "rgba(54, 162, 235, 0.8)",  // Blue
        "rgba(255, 206, 86, 0.8)",  // Yellow
        "rgba(153, 102, 255, 0.8)", // Purple
        "rgba(255, 159, 64, 0.8)",  // Orange
    ];

    (0..count)
        .map(|i| palette[i % palette.len()].to_string())
        .collect()
}

pub fn get_aggregator(column: &str, aggregator: &str) -> Option<Expr> {
    match aggregator {
        "count" => Some(col(column).count().alias(aggregator)),
        "min" => Some(col(column).min().alias(aggregator)),
        "max" => Some(col(column).max().alias(aggregator)),
        "mean" => Some(col(column).mean().alias(aggregator)),
        "median" => Some(col(column).median().alias(aggregator)),
        "sum" => Some(col(column).sum().alias(aggregator)),
        "avg" => Some(col(column).sum() / col(column).count().alias(aggregator)),
        "std" => Some(col(column).std(1).alias(aggregator)),
        "rsd" => Some(col(column).std(1) / col(column).mean().alias(aggregator)),
        "quantile_60" => Some(
            col(column)
                .quantile(0.60.into(), polars::prelude::QuantileMethod::Nearest)
                .alias(aggregator),
        ),
        "quantile_66" => Some(
            col(column)
                .quantile(0.66.into(), polars::prelude::QuantileMethod::Nearest)
                .alias(aggregator),
        ),
        "quantile_70" => Some(
            col(column)
                .quantile(0.70.into(), polars::prelude::QuantileMethod::Nearest)
                .alias(aggregator),
        ),
        "quantile_75" => Some(
            col(column)
                .quantile(0.75.into(), polars::prelude::QuantileMethod::Nearest)
                .alias(aggregator),
        ),
        "quantile_80" => Some(
            col(column)
                .quantile(0.80.into(), polars::prelude::QuantileMethod::Nearest)
                .alias(aggregator),
        ),
        "quantile_90" => Some(
            col(column)
                .quantile(0.90.into(), polars::prelude::QuantileMethod::Nearest)
                .alias(aggregator),
        ),
        _ => None, // Return None for unsupported aggregators
    }
}

pub fn to_predicate(search: StatisticSearchPayload) -> Expr {
    let mut predicates = vec![];

    if let Some(search) = search.search {
        info!("search: {:?}", search);
        let mut search_filter = HashMap::new();
        search_filter.insert(
            "title".to_string().to_lowercase(),
            search.clone().to_lowercase(),
        );
        search_filter.insert(
            "equipment".to_string().to_lowercase(),
            search.clone().to_lowercase(),
        );
        let predicate = to_like_predicate(search_filter, true);
        if let Some(p) = predicate {
            predicates.push(p);
        }
    }

    if let Some(make) = search.make {
        predicates.push(col("make").eq(lit(make)));
    }

    if let Some(model) = search.model {
        predicates.push(col("model").eq(lit(model)));
    }

    if let Some(engine) = search.engine {
        let mut engine_predicates = vec![];
        for v in engine.iter() {
            let p = col("engine").eq(lit(v.clone()));
            engine_predicates.push(p);
        }
        if !engine_predicates.is_empty() {
            let mut predicate = engine_predicates[0].clone();
            for p in engine_predicates.iter().skip(1) {
                predicate = predicate.or(p.clone());
            }

            predicates.push(predicate);
        }
    }

    if let Some(gearbox) = search.gearbox {
        predicates.push(col("gearbox").eq(lit(gearbox)));
    }

    if let Some(estimated_price) = search.estimated_price {
        predicates.push(col("estimated_price_in_eur").gt_eq(lit(estimated_price)));
    }

    if let Some(price) = search.price {
        predicates.push(col("price_in_eur").gt_eq(lit(price)));
    }

    if let Some(yearFrom) = search.year {
        predicates.push(col("year").eq(lit(yearFrom)));
    } else {
        if let Some(yearFrom) = search.yearFrom {
            predicates.push(col("year").gt_eq(lit(yearFrom)));
        }
        if let Some(yearTo) = search.yearTo {
            predicates.push(col("year").lt_eq(lit(yearTo)));
        }
    }
    if let Some(discount) = search.discountFrom {
        predicates.push(col("discount").gt_eq(lit(discount)));
    }
    if let Some(discount) = search.discountTo {
        predicates.push(col("discount").lt_eq(lit(discount)));
    }

    if let Some(saveDifference) = search.saveDiffFrom {
        predicates.push(col("save_diff_in_eur").gt_eq(lit(saveDifference)));
    }
    if let Some(saveDifference) = search.saveDiffTo {
        predicates.push(col("save_diff_in_eur").lt_eq(lit(saveDifference)));
    }

    if let Some(power) = search.power {
        predicates.push(col("power").eq(lit(power)));
    } else {
        if let Some(powerFrom) = search.powerFrom {
            predicates.push(col("power").gt_eq(lit(powerFrom)));
        }
        if let Some(powerTo) = search.powerTo {
            predicates.push(col("power").lt_eq(lit(powerTo)));
        }
    }
    if let Some(mileage) = search.mileage {
        predicates.push(col("mileage").eq(lit(mileage)));
    } else {
        if let Some(mileageFrom) = search.mileageFrom {
            predicates.push(col("mileage").gt_eq(lit(mileageFrom)));
        }
        if let Some(mileageTo) = search.mileageTo {
            predicates.push(col("mileage").lt_eq(lit(mileageTo)));
        }
    }

    if let Some(cc) = search.cc {
        predicates.push(col("cc").eq(lit(cc)));
    } else {
        if let Some(ccFrom) = search.ccFrom {
            predicates.push(col("cc").gt_eq(lit(ccFrom)));
        }
        if let Some(ccTo) = search.ccTo {
            predicates.push(col("cc").lt_eq(lit(ccTo)));
        }
    }

    if let Some(createdOnFrom) = search.createdOnFrom {
        let date = convert_days_to_date(createdOnFrom as i64);
        predicates.push(col("created_on").gt_eq(lit(date)));
    }

    if let Some(createdOnTo) = search.createdOnTo {
        let date = convert_days_to_date(createdOnTo as i64);
        predicates.push(col("created_on").lt_eq(lit(date)));
    }
    if predicates.is_empty() {
        return col("make").neq(lit("x"));
    }
    let combined_predicates = predicates
        .into_iter()
        .reduce(|acc, pred| acc.and(pred))
        .unwrap();
    info!("Predicates: {:?}", combined_predicates);
    combined_predicates
}

pub fn to_like_predicate<T: ToString + ToOwned + std::fmt::Debug + Literal>(
    filter: HashMap<String, T>,
    join_and: bool,
) -> Option<polars::lazy::dsl::Expr> {
    let mut predicates = vec![];
    let mut column_predicates = vec![];
    for (c, v) in filter.iter() {
        let p = if v.to_string().starts_with('*') {
            col(c).str().ends_with(lit(v.to_string().replace('*', "")))
        } else if v.to_string().ends_with('*') {
            col(c)
                .str()
                .starts_with(lit(v.to_string().replace('*', "")))
        } else {
            col(c).str().contains(lit(v.to_string()), false)
        };
        column_predicates.push(p);
    }

    let column_predicate = column_predicates
        .iter()
        .cloned()
        .reduce(|acc, b| acc.or(b))
        .unwrap();

    predicates.push(column_predicate);

    if predicates.is_empty() {
        return None;
    }
    if predicates.len() == 1 {
        return Some(predicates[0].clone());
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
fn convert_days_to_date(days_ago: i64) -> NaiveDate {
    let today = Utc::now().naive_utc().date();
    today.checked_sub_signed(TimeDelta::days(days_ago)).unwrap()
}
