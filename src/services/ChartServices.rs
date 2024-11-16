use std::{collections::HashMap, fmt::Debug, vec};

use log::info;
use polars::{
    error::PolarsError,
    frame::DataFrame,
    prelude::{col, lit, when, Expr, IntoLazy, SortMultipleOptions},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    model::{
        DistributionType,
        Intervals::{Interval, SortedIntervals, StatInterval},
        Quantiles::{generate_quantiles, Quantile},
    },
    services::{PriceService::to_aggregator, VehicleService::to_generic_json},
    VEHICLE_STATIC_DATA,
};

use super::PriceService::{to_predicate, StatisticSearchPayload};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Statistics {
    pub count: u32,
    pub min: i32,
    pub max: i32,
    pub mean: f64,
    pub median: f64,
    pub rsd: f64,
    pub quantiles: Vec<Quantile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DistributionChartData<T: Default> {
    pub axisLabel: String,
    pub dataLabel: String,
    pub axisValues: Vec<T>,
    pub min: T,
    pub max: T,
    pub median: T,
    pub count: i32,
    pub mean: f64,
    pub rsd: f64,
    pub data: Vec<IntervalData<T>>,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IntervalData<T> {
    pub column: String,
    pub category: String,
    pub min: T,
    pub max: T,
    pub median: T,
    pub count: i32,
    pub mean: f64,
    pub rsd: f64,
}

pub fn chartData(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df = VEHICLE_STATIC_DATA.clone();
    if search.group.is_empty() {
        HashMap::new()
    } else {
        let filterConditions = to_predicate(search.clone());
        let stat_column = search.stat_column.unwrap_or("advert_id".to_string());
        let aggregators = if stat_column == "advert_id" {
            to_aggregator(vec!["count".to_string()], &stat_column)
        } else {
            to_aggregator(search.aggregators.clone(), &stat_column)
        };

        let by = search.group.iter().map(col).collect::<Vec<_>>();
        let df = df
            .with_columns(&by)
            .filter(filterConditions)
            .group_by(by.as_slice())
            .agg(&aggregators);
        let result = if !search.order.is_empty() {
            let mut columns = Vec::new();
            let mut orders = Vec::new();

            for sort in search.order.iter() {
                columns.push(sort.column.clone());
                orders.push(!sort.asc);
            }
            let sort = SortMultipleOptions::new()
                .with_order_descending_multi(orders)
                .with_nulls_last(true);
            df.sort(&columns, sort).collect().unwrap()
        } else {
            let sort = SortMultipleOptions::new()
                .with_order_descending_multi(vec![false])
                .with_nulls_last(true);
            df.sort(vec!["count".to_string()], sort).collect().unwrap()
        };
        to_generic_json(&result)
    }
}

pub fn get_statistic_data(
    column: &str,
    search: &StatisticSearchPayload,
    interval: Option<Interval<i32>>,
    bins: usize, // Number of bins
) -> Result<Statistics, PolarsError> {
    let df = VEHICLE_STATIC_DATA.clone();

    let filter_conditions = to_predicate(search.clone());
    let price_series = df
        .with_column(col(column))
        .with_column(lit(1).alias("tmp_col"));

    // Apply the base filter conditions
    let mut qr = price_series.filter(filter_conditions);
    let quantiles = generate_quantiles(column, bins);

    // Apply the interval filter only if `interval` is `Some`
    if let Some(interval) = interval {
        qr = qr.filter(
            col(column)
                .gt_eq(lit(interval.start))
                .and(col(column).lt_eq(lit(interval.end))),
        );
    }

    // Dynamically generate quantile expressions based on bins
    let mut quantile_expressions = Vec::new();
    for q in quantiles.iter() {
        info!("{}: {}", q.alias, q.quantile);
        quantile_expressions.push(
            col(column)
                .quantile(q.quantile.into(), polars::prelude::QuantileMethod::Nearest)
                .alias(q.alias.clone()),
        );
    }

    // Group by and aggregate statistics
    let qr = qr
        .group_by(vec![col("tmp_col")])
        .agg(
            [
                col(column).count().alias("count"),
                col(column).min().alias("min"),
                col(column).mean().alias("mean"),
                col(column).median().alias("median"),
                col(column).max().alias("max"),
                (col(column).std(1) * lit(100)
                    / col(column).mean().cast(polars::prelude::DataType::Int32))
                .alias("rsd"),
            ]
            .iter()
            .cloned()
            .chain(quantile_expressions) // Add dynamic quantile expressions
            .collect::<Vec<_>>(),
        )
        .collect()
        .unwrap();

    load_statistics(&qr, quantiles)
}

fn load_statistics(df: &DataFrame, source: Vec<Quantile>) -> Result<Statistics, PolarsError> {
    let mut quantiles: Vec<Quantile> = Vec::new();
    for q in source.iter() {
        let qname = q.alias.clone();
        let qvalue = df.column(&qname)?.f64()?.get(0).unwrap_or_default();
        let calculated = Quantile {
            value: qvalue,
            ..q.clone()
        };
        quantiles.push(calculated);
    }

    Ok(Statistics {
        count: df.column("count")?.u32()?.get(0).unwrap_or_default(),
        min: df.column("min")?.i32()?.get(0).unwrap_or_default(),
        max: df.column("max")?.i32()?.get(0).unwrap_or_default(),
        mean: df.column("mean")?.f64()?.get(0).unwrap_or_default(),
        median: df.column("median")?.f64()?.get(0).unwrap_or_default(),
        rsd: df.column("rsd")?.f64()?.get(0).unwrap_or_default(),
        quantiles,
    })
}

pub fn clean_data(column: &str, search: StatisticSearchPayload) -> StatInterval {
    let df = VEHICLE_STATIC_DATA.clone();

    let filterConditions = to_predicate(search.clone());
    let price_series = df
        .with_column(col(column))
        .with_column(lit(1).alias("tmp_col"));

    let filtered = price_series
        .clone()
        .filter(filterConditions.clone())
        .group_by(vec![col("tmp_col")])
        .agg(&[
            col(column).min().alias("min"),
            col(column).max().alias("max"),
            col(column).median().alias("median"),
            col(column).count().alias("count"),
            col(column)
                .quantile(0.0005.into(), polars::prelude::QuantileMethod::Nearest)
                .alias("q1"),
            col(column)
                .quantile(0.03.into(), polars::prelude::QuantileMethod::Nearest)
                .alias("q3"),
            col(column)
                .quantile(0.05.into(), polars::prelude::QuantileMethod::Nearest)
                .alias("q5"),
            col(column)
                .quantile(0.95.into(), polars::prelude::QuantileMethod::Nearest)
                .alias("q95"),
            col(column)
                .quantile(0.97.into(), polars::prelude::QuantileMethod::Nearest)
                .alias("q97"),
            col(column)
                .quantile(0.995.into(), polars::prelude::QuantileMethod::Nearest)
                .alias("q99"),
            (col(column).std(1) * lit(100)
                / col(column).mean().cast(polars::prelude::DataType::Int32))
            .alias("rsd"),
        ])
        .collect()
        .unwrap();
    let count = filtered
        .column("count")
        .unwrap()
        .u32()
        .unwrap()
        .get(0)
        .unwrap_or_default();
    let rsd = filtered
        .column("rsd")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();
    let start = filtered
        .column("min")
        .unwrap()
        .i32()
        .unwrap()
        .get(0)
        .unwrap_or_default();
    let end = filtered
        .column("max")
        .unwrap()
        .i32()
        .unwrap()
        .get(0)
        .unwrap_or_default();
    let median = filtered
        .column("median")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();

    let q1 = filtered
        .column("q1")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();

    let q3 = filtered
        .column("q3")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();

    let q5 = filtered
        .column("q5")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();

    let q95 = filtered
        .column("q95")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();

    let q97 = filtered
        .column("q97")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();

    let q99 = filtered
        .column("q99")
        .unwrap()
        .f64()
        .unwrap()
        .get(0)
        .unwrap_or_default();

    info!("Count: {}", count);
    info!("RSD: {}", rsd);
    info!("Start: {}", start);
    info!("End: {}", end);
    info!("Median: {}", median);
    info!("Q1: {}", q1);
    info!("Q3: {}", q3);
    info!("Q5: {}", q5);
    info!("Q95: {}", q95);
    info!("Q97: {}", q97);
    info!("Q99: {}", q99);

    if count > 1000 {
        StatInterval {
            column: column.to_string(),
            orig_start: start,
            orig_end: end,
            rsd,
            count: count as i32,
            start: q1 as i32,
            end: q99 as i32,
        }
    } else if count > 500 {
        StatInterval {
            column: column.to_string(),
            orig_start: start,
            orig_end: end,
            rsd,
            count: count as i32,
            start: q3 as i32,
            end: q97 as i32,
        }
    } else if count > 50 {
        StatInterval {
            column: column.to_string(),
            orig_start: start,
            orig_end: end,
            rsd,
            count: count as i32,
            start: q5 as i32,
            end: q95 as i32,
        }
    } else {
        StatInterval {
            column: column.to_string(),
            orig_start: start,
            orig_end: end,
            rsd,
            count: count as i32,
            start,
            end,
        }
    }
}

pub fn calculate(
    column_name: &str,
    intervals: SortedIntervals<i32>,
    search: &StatisticSearchPayload,
) -> Result<DistributionChartData<i32>, String> {
    if intervals.is_empty() {
        info!("Intervals are empty");
        return Err("Intervals are empty".to_string());
    }
    let min = intervals.min().unwrap().start;
    let max = intervals.max().unwrap().end;
    let df = VEHICLE_STATIC_DATA.clone();
    let filterConditions = to_predicate(search.clone());
    let df = df
        .filter(filterConditions)
        .filter((col(column_name).gt_eq(lit(min))).and(col(column_name).lt_eq(lit(max))));
    let case_column: Expr;
    if intervals.len() == 1 {
        info!("Intervals has only one element");
        if let Some(interval) = intervals.get_interval(0) {
            let expr = when(
                col(column_name)
                    .gt_eq(lit(interval.start))
                    .and(col(column_name).lt(lit(interval.end))),
            )
            .then(lit(interval.category.clone()));
            case_column = expr.otherwise(lit(1)).alias("stat_category");
        } else {
            case_column = lit(1).alias("stat_category");
        }
    } else {
        info!("Intervals has more than one element");
        let mut expr = when(
            col(column_name)
                .gt_eq(lit(intervals.get_interval(0).unwrap().start))
                .and(col(column_name).lt(lit(intervals.get_interval(0).unwrap().end))),
        )
        .then(lit(intervals.get_interval(0).unwrap().category.clone()))
        .when(
            col(column_name)
                .gt_eq(lit(intervals.get_interval(1).unwrap().start))
                .and(col(column_name).lt(lit(intervals.get_interval(1).unwrap().end))),
        )
        .then(lit(intervals.get_interval(1).unwrap().category.clone()));
        for interval in &intervals.intervals[2..] {
            expr = expr
                .when(
                    col(column_name)
                        .gt_eq(lit(interval.start))
                        .and(col(column_name).lt(lit(interval.end))),
                )
                .then(lit(interval.category.clone()));
        }
        case_column = expr.otherwise(lit(10)).alias("stat_category");
    }
    let data = df
        .clone()
        .with_column(case_column)
        .group_by(vec![col("stat_category")])
        .agg(&[
            col(column_name).min().alias("min"),
            col(column_name).max().alias("max"),
            col(column_name).count().alias("count"),
            col(column_name).mean().alias("mean"),
            col(column_name).median().alias("median"),
            (col(column_name).std(1) * lit(100)
                / col(column_name)
                    .mean()
                    .cast(polars::prelude::DataType::Int32))
            .alias("rsd"),
        ])
        .collect()
        .unwrap();

    let result = if !search.order.is_empty() {
        let mut columns = Vec::new();
        let mut orders = Vec::new();

        for sort in search.order.iter() {
            columns.push(sort.column.clone());
            orders.push(!sort.asc);
        }
        let sort = SortMultipleOptions::new()
            .with_order_descending_multi(orders)
            .with_nulls_last(true);
        data.lazy().sort(&columns, sort).collect().unwrap()
    } else {
        data.lazy().collect().unwrap()
    };
    let count = result.height();
    info!("Count: {}", count);
    let mut intervalData = Vec::new();
    for i in 0..count {
        let min = result
            .column("min")
            .unwrap()
            .i32()
            .unwrap()
            .get(i)
            .unwrap_or_default();
        let max = result
            .column("max")
            .unwrap()
            .i32()
            .unwrap()
            .get(i)
            .unwrap_or_default();
        let count = result
            .column("count")
            .unwrap()
            .u32()
            .unwrap()
            .get(i)
            .unwrap_or_default() as i32;
        let mean = result
            .column("mean")
            .unwrap()
            .f64()
            .unwrap()
            .get(i)
            .unwrap_or_default();
        let median = result
            .column("median")
            .unwrap()
            .f64()
            .unwrap()
            .get(i)
            .unwrap_or_default() as i32;
        let rsd = result
            .column("rsd")
            .unwrap()
            .f64()
            .unwrap()
            .get(i)
            .unwrap_or_default();
        let category = result
            .column("stat_category")
            .unwrap()
            .str()
            .unwrap()
            .get(i)
            .unwrap_or_default();
        let IntervalData = IntervalData {
            column: column_name.to_string(),
            category: category.to_lowercase().trim().to_string(),
            min,
            max,
            median,
            count,
            mean,
            rsd,
        };
        intervalData.push(IntervalData);
    }
    let mut sorted_data = vec![];
    for i in intervals.intervals.iter() {
        for j in intervalData.iter() {
            if i.category == j.category {
                sorted_data.push(j.clone());
                break;
            }
        }
    }

    let data = DistributionChartData::<i32> {
        axisLabel: column_name.to_string(),
        dataLabel: "stat_category".to_string(),
        axisValues: intervals.values().to_vec(),
        count: count as i32,
        mean: 0.0,
        rsd: 0.0,
        data: sorted_data,
        ..Default::default()
    };

    Ok(data)
}

pub fn data_to_bins(
    column: &str,
    filter: StatisticSearchPayload,
    all: bool,
    distribution_type: DistributionType,
    number_of_bins: usize,
) -> Result<DistributionChartData<i32>, String> {
    if number_of_bins < 2 {
        return Err("Number of bins must be greater than 2".to_string());
    }

    let stat_interval = clean_data(column, filter.clone());

    let interval_start = if all {
        stat_interval.start
    } else {
        stat_interval.orig_start
    };

    let interval_end = if all {
        stat_interval.end
    } else {
        stat_interval.orig_end
    };

    let mut intervals = vec![];
    if distribution_type == DistributionType::ByInterval {
        let step = (interval_end - interval_start) / (number_of_bins as i32);
        for i in 0..number_of_bins as i32 {
            let interval: Interval<i32> = Interval {
                column: column.to_string(),
                start: interval_start + i * step,
                end: interval_start + (i + 1) * step,
                category: i.to_string(),
                index: 0,
            };
            intervals.push(interval);
        }
    } else {
        let interval: Interval<i32> = Interval {
            column: column.to_string(),
            start: interval_start,
            end: interval_end,
            category: "all".to_string(),
            index: 0,
        };
        let statistics: Statistics =
            get_statistic_data(column, &filter, Some(interval), number_of_bins).unwrap();
        let quantiles = statistics.quantiles;

        let start = Interval {
            column: column.to_string(),
            start: statistics.min,
            end: quantiles[0].value as i32,
            category: "1".to_string(),
            index: 0,
        };
        intervals.push(start);
        for i in 1..quantiles.len() {
            let interval = Interval {
                column: column.to_string(),
                start: quantiles[i - 1].value as i32,
                end: quantiles[i].value as i32,
                category: (i + 1).to_string(),
                index: i,
            };
            intervals.push(interval);
        }
    }

    calculate(column, SortedIntervals::from(intervals), &filter)
}
