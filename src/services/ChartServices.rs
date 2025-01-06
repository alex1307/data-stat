use std::vec;

use log::info;
use polars::{
    error::PolarsError,
    frame::DataFrame,
    prelude::{col, lit, when, Expr, IntoLazy, SortMultipleOptions},
};

use crate::{
    model::{
        AxumAPIModel::{
            DimensionData, Metadata, StatisticData, StatisticResponse, StatisticSearchPayload,
        },
        DistributionChartData, DistributionType, IntervalData,
        Intervals::{Interval, SortedIntervals, StatInterval},
        Quantiles::{generate_quantiles, Quantile},
        Statistics,
    },
    services::Utils::to_predicate,
    VEHICLE_STATIC_DATA,
};

use super::Utils::to_aggregator;

pub fn chartData(search: StatisticSearchPayload) -> StatisticResponse {
    let df = VEHICLE_STATIC_DATA.clone();
    let group = if search.group.is_none() {
        vec![]
    } else {
        search.group.clone().unwrap()
    };
    if group.is_empty() {
        return StatisticResponse {
            metadata: vec![],
            dimensions: vec![],
            data: vec![],
            total_count: 0,
        };
    }

    let filterConditions = to_predicate(search.clone());
    let stat_column = search.stat_column.unwrap_or("advert_id".to_string());
    let aggregators = if stat_column == "advert_id" || search.aggregators.is_none() {
        to_aggregator(vec!["count".to_string()], &stat_column)
    } else {
        let aggregators = search.aggregators.unwrap();
        to_aggregator(aggregators, &stat_column)
    };

    let by = group.iter().map(col).collect::<Vec<_>>();
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
    to_static_response(&result, group.clone())
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
    let interval: Interval<i32> = Interval {
        column: column.to_string(),
        start: interval_start,
        end: interval_end,
        category: "all".to_string(),
        index: 0,
    };
    let statistics: Statistics =
        get_statistic_data(column, &filter, Some(interval), number_of_bins).unwrap();

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

    let data_by_bins = calculate(column, SortedIntervals::from(intervals), &filter);
    if let Ok(mut data) = data_by_bins {
        data.count = statistics.count as i32;
        data.mean = statistics.mean;
        data.rsd = statistics.rsd;
        data.median = statistics.median as i32;
        data.max = statistics.max;
        data.min = statistics.min;
        Ok(data)
    } else {
        Err("Error calculating data".to_string())
    }
}

pub fn to_static_response(data: &DataFrame, group_by: Vec<String>) -> StatisticResponse {
    let column_values = data.get_columns();
    info!("Found results: {}", data.height());
    info!("Column count: {}", column_values.len());

    let mut metadata = vec![];
    for (idx, column) in data.get_columns().iter().enumerate() {
        metadata.push(Metadata {
            column_index: idx as u8,
            column: column.name().to_string(),
        });
    }
    let mut dimensions = vec![];
    for (idx, cv) in column_values
        .iter()
        .filter(|cv| group_by.contains(&cv.name().to_string()))
        .enumerate()
    {
        let dtype = cv.dtype().clone();
        let data = if dtype == polars::prelude::DataType::Int32
            || dtype == polars::prelude::DataType::UInt32
            || dtype == polars::prelude::DataType::Int64
            || dtype == polars::prelude::DataType::UInt64
        {
            cv.i32()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or_default().to_string())
                .collect::<Vec<_>>()
        } else {
            cv.str()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or_default().to_string())
                .collect::<Vec<_>>()
        };
        let name = cv.name().to_string();
        dimensions.push(DimensionData {
            column_index: idx as u8,
            column_name: name.clone(),
            label: name.clone(),
            data: data.clone(),
            distinct_count: data
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
                .len() as u32,
        });
    }
    let count = data.height();
    let mut chart_data = vec![];
    for i in 0..count {
        let count = if let Ok(count) = data.column("count") {
            Some(count.u32().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };
        let sum = if let Ok(sum) = data.column("sum") {
            Some(sum.i32().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };
        let avg = if let Ok(avg) = data.column("avg") {
            Some(avg.i64().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };

        let min = if let Ok(min) = data.column("min") {
            Some(min.i32().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };

        let max = if let Ok(max) = data.column("max") {
            Some(max.i32().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };

        let median = if let Ok(median) = data.column("median") {
            Some(median.i32().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };

        let rsd = if let Ok(rsd) = data.column("rsd") {
            Some(rsd.f64().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };
        let quantile = if let Ok(quantile) = data.column("quantile") {
            Some(quantile.f64().unwrap().get(i).unwrap_or_default())
        } else {
            None
        };
        let data = StatisticData {
            count,
            sum,
            avg,
            min,
            max,
            median,
            rsd,
            quantile,
        };
        chart_data.push(data);
    }

    StatisticResponse {
        metadata,
        dimensions,
        data: chart_data,
        total_count: count as u32,
    }
}
