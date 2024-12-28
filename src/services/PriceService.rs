use std::{
    collections::{BTreeMap, HashMap as StdHashMap, HashMap, HashSet},
    vec,
};

use chrono::{NaiveDate, TimeDelta, Utc};
use log::{error, info};

use polars::{
    error::PolarsError,
    frame::DataFrame,
    lazy::dsl::{col, lit, Expr},
    prelude::{pivot::pivot, DataType, IntoLazy, LazyFrame, SortMultipleOptions},
    series::Series,
};

use serde_json::{json, Value};

use crate::{model::AxumAPIModel::StatisticSearchPayload, PRICE_DATA};

use super::VehicleService::{to_generic_json, to_like_predicate};

pub struct StatisticService {
    pub price_data: LazyFrame,
}

pub fn stat_distribution(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df: LazyFrame = PRICE_DATA.clone();

    // Group by the required columns and calculate the required statistics
    info!("Payload: {:?}", search);
    let filterConditions = to_predicate(search.clone());
    let group: Vec<String>;
    let aggregators: Vec<String>;
    if search.group.is_none() {
        let mut error_map = HashMap::new();
        error_map.insert("error".to_string(), "Group is required".into());
        return error_map;
    } else {
        group = search.group.clone().unwrap();
        if group.is_empty() {
            let mut error_map = HashMap::new();
            error_map.insert("error".to_string(), "Group is required".into());
            return error_map;
        }
    }
    if search.aggregators.is_none() {
        let mut error_map = HashMap::new();
        error_map.insert("error".to_string(), "Aggregators are required".into());
        return error_map;
    } else {
        aggregators = search.aggregators.clone().unwrap();
        if aggregators.is_empty() {
            let mut error_map = HashMap::new();
            error_map.insert("error".to_string(), "Aggregators are required".into());
            return error_map;
        }
    }

    let by = group.iter().map(col).collect::<Vec<_>>();
    let stat_column = search
        .stat_column
        .clone()
        .unwrap_or("price_in_eur".to_string());
    let aggregators = to_aggregator(aggregators.clone(), &stat_column);

    let filtered = df
        .with_columns(&[
            col("make"),
            col("model"),
            col("year"),
            col("engine"),
            col("gearbox"),
            col("power"),
            col("mileage"),
            col("cc"),
            col("mileage_breakdown"),
            col("power_breakdown"),
            col("price_in_eur"),
            col("estimated_price_in_eur"),
            col("save_diff_in_eur"),
            col("extra_charge_in_eur"),
            col("discount"),
            col("increase"),
        ])
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
        info!("Columns: {:?}", columns);
        info!("Orders: {:?}", orders);
        let sort = SortMultipleOptions::new()
            .with_order_descending_multi(orders)
            .with_nulls_last(true);
        filtered.sort(&columns, sort).collect().unwrap()
    } else {
        filtered.collect().unwrap()
    };

    to_generic_json(&result)
}

pub fn chart_data(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df: LazyFrame = PRICE_DATA.clone();

    // Group by the required columns and calculate the required statistics
    info!("Payload: {:?}", search);
    let filterConditions = to_predicate(search.clone());
    let group: Vec<String>;
    let aggregators: Vec<String>;
    if search.group.is_none() {
        let mut error_map = HashMap::new();
        error_map.insert("error".to_string(), "Group is required".into());
        return error_map;
    } else {
        group = search.group.clone().unwrap();
        if group.is_empty() {
            let mut error_map = HashMap::new();
            error_map.insert("error".to_string(), "Group is required".into());
            return error_map;
        }
    }
    if search.aggregators.is_none() {
        let mut error_map = HashMap::new();
        error_map.insert("error".to_string(), "Aggregators are required".into());
        return error_map;
    } else {
        aggregators = search.aggregators.clone().unwrap();
        if aggregators.is_empty() {
            let mut error_map = HashMap::new();
            error_map.insert("error".to_string(), "Aggregators are required".into());
            return error_map;
        }
    }

    let by = group.iter().map(col).collect::<Vec<_>>();
    let stat_column = search
        .stat_column
        .clone()
        .unwrap_or("price_in_eur".to_string());
    let aggregators = to_aggregator(aggregators.clone(), &stat_column);
    let filtered = df
        .with_columns(&[
            col("make"),
            col("model"),
            col("year"),
            col("engine"),
            col("gearbox"),
            col("power"),
            col("mileage"),
            col("cc"),
            col("mileage_breakdown"),
            col("power_breakdown"),
            col("price_in_eur"),
            col("estimated_price_in_eur"),
            col("save_diff_in_eur"),
            col("extra_charge_in_eur"),
            col("discount"),
            col("increase"),
        ])
        .filter(filterConditions);

    let data_aggregated = filtered.clone().group_by(by.as_slice()).agg(&aggregators);
    let result = if !search.order.is_empty() {
        let mut columns = Vec::new();
        let mut orders = Vec::new();

        for sort in search.order.iter() {
            columns.push(sort.column.clone());
            orders.push(!sort.asc);
        }
        info!("Columns: {:?}", columns);
        info!("Orders: {:?}", orders);
        let sort = SortMultipleOptions::new()
            .with_order_descending_multi(orders)
            .with_nulls_last(true);
        data_aggregated.sort(&columns, sort).collect().unwrap()
    } else {
        data_aggregated.collect().unwrap()
    };
    let groups = search.group.clone().unwrap();
    let aggregator = search.aggregators.clone().unwrap()[0].clone();
    if groups.len() <= 2 {
        to_stacked_bars_json(&result, group, &aggregator)
    } else {
        let pivot_col = search.group.clone().unwrap()[1].clone();
        let x_col = search.group.clone().unwrap()[0].clone();
        let stacked_col = search.group.clone().unwrap()[2].clone();
        info!("Pivot: {:?}", pivot_col);
        info!("X: {:?}", x_col);
        info!("Stacked: {:?}", stacked_col);
        info!("Aggregator: {:?}", aggregator);
        to_pivot_json(
            &filtered.collect().unwrap(),
            group,
            pivot_col,
            aggregator,
            true, // Sort pivoted columns
        )
        .unwrap()
    }
}

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

fn convert_days_to_date(days_ago: i64) -> NaiveDate {
    let today = Utc::now().naive_utc().date();
    today.checked_sub_signed(TimeDelta::days(days_ago)).unwrap()
}

pub fn to_stacked_bars_json(
    data: &DataFrame,
    group_cols: Vec<String>, // e.g. ["year", "gearbox", "engine"]
    aggregator_col: &str,    // e.g. "count"
) -> HashMap<String, Value> {
    let mut json_map = HashMap::new();

    // Bootstrap dark mode colors
    let dark_mode_colors = vec![
        "rgba(75, 192, 192, 0.8)",  // Teal
        "rgba(255, 99, 132, 0.8)",  // Pink
        "rgba(54, 162, 235, 0.8)",  // Blue
        "rgba(255, 206, 86, 0.8)",  // Yellow
        "rgba(153, 102, 255, 0.8)", // Purple
        "rgba(255, 159, 64, 0.8)",  // Orange
        "rgba(201, 203, 207, 0.8)", // Grey
    ];

    // ------------------------------------------------------------------
    // 1) Basic info: itemsCount, columns, metadata
    // ------------------------------------------------------------------
    json_map.insert("itemsCount".to_owned(), data.height().into());
    let column_values = data.get_columns();
    info!("Found results: {}", data.height());
    info!("Column count: {}", column_values.len());

    // ------------------------------------------------------------------
    // 2) Identify group & aggregator columns
    // ------------------------------------------------------------------
    let mut group_series = Vec::new();
    for col_name in group_cols {
        match data.column(&col_name) {
            Ok(s) => group_series.push(s),
            Err(_) => {
                json_map.insert(
                    "error".to_string(),
                    json!(format!(
                        "Group column '{}' not found in DataFrame",
                        col_name
                    )),
                );
                return json_map;
            }
        }
    }

    let agg_series = match data.column(aggregator_col) {
        Ok(s) => s,
        Err(_) => {
            json_map.insert(
                "error".to_string(),
                json!(format!("Aggregator column '{}' not found", aggregator_col)),
            );
            return json_map;
        }
    };

    // ------------------------------------------------------------------
    // 3) Collect X-axis labels
    // ------------------------------------------------------------------
    let x_col = group_series[0];
    let x_values = series_to_string_vec(x_col.as_series().unwrap());

    let unique_x_set: HashSet<String> = x_values.iter().cloned().collect();
    let mut sorted_x: Vec<String> = unique_x_set.into_iter().collect();
    sorted_x.sort_by_key(|lbl| lbl.parse::<i32>().unwrap_or(i32::MIN));

    // ------------------------------------------------------------------
    // 4) Build data map
    // ------------------------------------------------------------------
    let second_val_series_opt = if group_series.len() > 1 {
        Some(group_series[1])
    } else {
        None
    };
    let third_val_series_opt = if group_series.len() > 2 {
        Some(group_series[2])
    } else {
        None
    };

    let second_vals = second_val_series_opt.map(|s| series_to_string_vec(s.as_series().unwrap()));
    let third_vals = third_val_series_opt.map(|s| series_to_string_vec(s.as_series().unwrap()));
    let agg_values = series_to_f64_vec(agg_series.as_series().unwrap());

    let mut data_map: StdHashMap<(String, String), BTreeMap<String, f64>> = StdHashMap::new();

    for i in 0..data.height() {
        let x_val = &x_values[i];
        let s_val = match &second_vals {
            Some(vec) => &vec[i],
            None => "",
        };
        let t_val = match &third_vals {
            Some(vec) => &vec[i],
            None => "",
        };
        let a_val = agg_values[i];
        let key = (s_val.to_string(), t_val.to_string());
        data_map
            .entry(key)
            .or_insert_with(BTreeMap::new)
            .insert(x_val.clone(), a_val);
    }

    // ------------------------------------------------------------------
    // 5) Create datasets with colors
    // ------------------------------------------------------------------
    let mut dataset_entries = Vec::new();
    let mut all_keys: Vec<_> = data_map.keys().cloned().collect();
    all_keys.sort();

    for (i, (s_val, t_val)) in all_keys.iter().enumerate() {
        let label = match group_series.len() {
            1 => "Count".to_string(),
            2 => s_val.clone(),
            3 => format!("{} - {}", s_val, t_val),
            _ => "N/A".to_string(),
        };

        let counts_map = &data_map[&(s_val.clone(), t_val.clone())];
        let data_array: Vec<f64> = sorted_x
            .iter()
            .map(|xv| *counts_map.get(xv).unwrap_or(&0.0))
            .collect();

        // Assign colors using the dark_mode_colors palette, cycling if necessary
        let color = dark_mode_colors[i % dark_mode_colors.len()];

        let dataset = json!({
            "label": label,
            "data": data_array,
            "backgroundColor": color
        });
        dataset_entries.push(dataset);
    }

    // ------------------------------------------------------------------
    // 6) Final JSON structure
    // ------------------------------------------------------------------
    json_map.insert("labels".to_string(), json!(sorted_x));
    json_map.insert("datasets".to_string(), Value::Array(dataset_entries));

    json_map
}

/// Utility: Convert a Polars Series to a Vec<String>, handling common types
/// (e.g., Int32, Int64, Float64, LargeUtf8, etc.).
fn series_to_string_vec(series: &Series) -> Vec<String> {
    // You can expand this to handle more data types as needed.
    match series.dtype() {
        DataType::Int32 => series
            .i32()
            .unwrap()
            .into_iter()
            .map(|opt| opt.map(|v| v.to_string()).unwrap_or_default())
            .collect(),
        DataType::Int64 => series
            .i64()
            .unwrap()
            .into_iter()
            .map(|opt| opt.map(|v| v.to_string()).unwrap_or_default())
            .collect(),
        DataType::Float64 => series
            .f64()
            .unwrap()
            .into_iter()
            .map(|opt| opt.map(|v| v.to_string()).unwrap_or_default())
            .collect(),
        DataType::String => series
            .str()
            .unwrap()
            .into_iter()
            .map(|opt| opt.unwrap_or("").to_string())
            .collect(),
        // fallback
        _ => series
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .map(|opt| opt.unwrap_or("").to_string())
            .collect(),
    }
}

pub fn to_pivot_json(
    data: &DataFrame,
    group_cols: Vec<String>, // Columns to group by
    pivot_col: String,       // Column to pivot
    agg_func: String,        // Aggregation function (e.g., Expr::sum())
    sort_columns: bool,      // Whether to sort pivoted columns
) -> Result<HashMap<String, Value>, PolarsError> {
    let mut json_map = HashMap::new();

    // ------------------------------------------------------------------
    // 1) Validate Inputs
    // ------------------------------------------------------------------
    if group_cols.is_empty() {
        json_map.insert(
            "error".to_string(),
            json!("At least one group column is required."),
        );
        return Ok(json_map);
    }
    info!("Group columns: {:?}", group_cols);

    // Ensure all required columns exist
    let required_cols = [&group_cols[..], &[pivot_col.as_str().to_string()]].concat();
    for col in &required_cols {
        if data.column(col).is_err() {
            json_map.insert(
                "error".to_string(),
                json!(format!("Column '{}' not found in DataFrame", col)),
            );
            error!("Column '{}' not found in DataFrame", col);
            return Ok(json_map);
        }
    }

    // ------------------------------------------------------------------
    // 2) Group and Aggregate Data
    // ------------------------------------------------------------------
    let aggregator = get_aggregator("price_in_eur", &agg_func);
    let aggs = match &aggregator {
        Some(agg) => vec![agg.clone()],
        None => {
            json_map.insert(
                "error".to_string(),
                json!(format!("Unsupported aggregator: '{}'", agg_func)),
            );
            error!("Unsupported aggregator: '{}'", agg_func);
            return Ok(json_map);
        }
    };
    let sort = SortMultipleOptions::new()
        .with_order_descending_multi(vec![true])
        .with_nulls_last(true);
    let grouped_ok = data
        .clone()
        .lazy()
        .group_by(group_cols.iter().map(|c| col(c)).collect::<Vec<_>>())
        .agg(&aggs)
        .sort(vec!["year"], sort)
        .collect();
    let grouped = match grouped_ok {
        Ok(grouped) => {
            info!("Grouped data: {:?}", grouped);
            grouped
        }
        Err(e) => {
            error!("Error grouping data: {}", e);
            json_map.insert("error".to_string(), json!(format!("{}", e)));
            return Ok(json_map);
        }
    };

    // ------------------------------------------------------------------
    // 3) Perform Pivot
    // ------------------------------------------------------------------

    let base_cols = group_cols
        .iter()
        .filter(|c| *c != &pivot_col)
        .cloned()
        .collect::<Vec<_>>();
    let pivoted = pivot(
        &grouped,
        [pivot_col],             // Pivot column (e.g., engine)
        Some(base_cols.clone()), // Index columns
        Some([agg_func]),        // Values column
        sort_columns,            // Whether to sort pivoted columns
        None,                    // No additional aggregation needed
        None,                    // No separator
    )?;

    let filled_pivoted = pivoted.lazy().fill_null(lit(0)).collect()?;
    info!("Pivoted data: {:?}", &filled_pivoted);
    // ------------------------------------------------------------------
    // 4) Extract Labels and Datasets
    // ------------------------------------------------------------------
    let year_col = filled_pivoted.column("year")?.i32()?;
    let gearbox_col = filled_pivoted.column("gearbox")?.str()?;

    let labels = year_col
        .into_iter()
        .zip(gearbox_col.into_iter())
        .map(|(year, gearbox)| {
            match (year, gearbox) {
                (Some(y), Some(g)) => format!("{} ({})", y, g), // Combine year and gearbox
                (Some(y), None) => y.to_string(),
                _ => "Unknown".to_string(),
            }
        })
        .collect::<Vec<_>>();

    // Generate distinct colors for each pivoted column
    let unique_values = filled_pivoted
        .get_columns()
        .iter()
        .filter(|col| !base_cols.contains(&col.name().to_string())) // Exclude the labels column
        .map(|col| col.name().to_string())
        .collect::<Vec<_>>();

    let colors = generate_colors(unique_values.len());
    let mut datasets = vec![];
    info!("Unique values: {:?}", unique_values);
    for (idx, value) in unique_values.iter().enumerate() {
        if let Ok(data_series) = filled_pivoted.column(value) {
            let data = match data_series.dtype() {
                DataType::String => data_series
                    .str()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or("").parse::<f64>().unwrap_or(0.0))
                    .collect::<Vec<_>>(),
                DataType::Float64 => data_series
                    .f64()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0.0))
                    .collect::<Vec<_>>(),
                DataType::UInt32 => data_series
                    .u32()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64) // Convert to f64
                    .collect::<Vec<_>>(),
                DataType::UInt64 => data_series
                    .u64()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64) // Convert to f64
                    .collect::<Vec<_>>(),
                DataType::Int32 => data_series
                    .i32()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64) // Convert to f64
                    .collect::<Vec<_>>(),
                DataType::Int64 => data_series
                    .i64()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64) // Convert to f64
                    .collect::<Vec<_>>(),
                _ => {
                    return Err(PolarsError::ComputeError(
                        format!("Unsupported data type: {:?}", data_series.dtype()).into(),
                    ))
                }
            };

            datasets.push(json!({
                "label": value,
                "data": data,
                "backgroundColor": colors[idx]
            }));
        }
    }

    // ------------------------------------------------------------------
    // 5) Build Final JSON
    // ------------------------------------------------------------------
    json_map.insert("labels".to_string(), json!(labels));
    json_map.insert("datasets".to_string(), Value::Array(datasets));

    Ok(json_map)
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

fn generate_colors(count: usize) -> Vec<String> {
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

/// Utility: Convert a Polars Series to a Vec<f64>, for aggregator columns
/// (like "count", "sum", "mean", etc.).
fn series_to_f64_vec(series: &Series) -> Vec<f64> {
    match series.dtype() {
        DataType::Int32 => series
            .i32()
            .unwrap()
            .into_iter()
            .map(|opt| opt.map(|v| v as f64).unwrap_or(0.0))
            .collect(),
        DataType::Int64 => series
            .i64()
            .unwrap()
            .into_iter()
            .map(|opt| opt.map(|v| v as f64).unwrap_or(0.0))
            .collect(),
        DataType::Float64 => series
            .f64()
            .unwrap()
            .into_iter()
            .map(|opt| opt.unwrap_or(0.0))
            .collect(),
        DataType::Float32 => series
            .f32()
            .unwrap()
            .into_iter()
            .map(|opt| opt.map(|v| v as f64).unwrap_or(0.0))
            .collect(),
        // fallback: try casting to Float64
        _ => series
            .cast(&DataType::Float64)
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .map(|opt| opt.unwrap_or(0.0))
            .collect(),
    }
}

#[cfg(test)]
mod test_stat {
    use std::{
        fs::{self, File},
        io::Write,
    };

    use log::info;
    use polars::prelude::{all, col, lit, when, IntoLazy, SortMultipleOptions};

    use crate::{
        configure_log4rs,
        model::AxumAPIModel::Order,
        services::{
            PriceService::{stat_distribution, StatisticSearchPayload},
            VehicleService::to_generic_json,
        },
    };

    #[test]
    fn test_load_data() {
        configure_log4rs("resources/log4rs.yml");
        let df = super::PRICE_DATA.clone();
        let result = df
            .clone()
            .select(&[
                col("make"),
                col("model"),
                col("year"),
                col("engine"),
                col("gearbox"),
                col("power"),
                col("mileage"),
                col("cc"),
                col("mileage_breakdown"),
                col("power_breakdown"),
                col("price_in_eur"),
                col("estimated_price_in_eur"),
                col("save_diff_in_eur"),
                col("extra_charge_in_eur"),
                col("discount"),
                col("increase"),
            ])
            .collect()
            .unwrap();

        assert_eq!(result.height(), 222441);

        // let search = StatisticSearchPayload {
        //     make: Some("Audi".to_string()),
        //     group: vec![],
        //     aggregators: vec![],
        //     order: vec![],
        //     ..Default::default()
        // };
        // let filter = super::to_predicate(search);
        let filtered = df
            .select([all()])
            .filter(col("make").eq(lit("Audi")))
            .collect()
            .unwrap();
        info!("{:?}", filtered.head(Some(10)));
        assert_eq!(filtered.height(), 22197);
    }

    #[test]
    fn test_stat_distribution() {
        configure_log4rs("resources/log4rs.yml");
        let functions = vec![
            "count".to_string(),
            "mean".to_string(),
            "median".to_string(),
            "std".to_string(),
            "avg".to_string(),
            "rsd".to_string(),
            "quantile_60".to_string(),
            "quantile_66".to_string(),
            "quantile_75".to_string(),
            "quantile_80".to_string(),
            "quantile_90".to_string(),
            "min".to_string(),
            "max".to_string(),
        ];
        let column = "price_in_eur".to_string();
        let columns = functions
            .iter()
            .map(|f| format!("{}_{}", column, f))
            .collect::<Vec<_>>();

        let search = StatisticSearchPayload {
            make: Some("BMW".to_string()),
            model: Some("320".to_string()),
            year: Some(2018),
            engine: Some(vec!["Petrol".to_string()]),
            powerFrom: Some(150),
            powerTo: Some(200),
            mileageFrom: Some(50000),
            mileageTo: Some(80000),
            group: Some(vec!["make".to_string(), "model".to_string()]),
            aggregators: Some(functions),
            order: vec![Order {
                column: "make".to_string(),
                asc: true,
            }],
            estimated_price: Some(0),
            stat_column: Some(column),
            ..Default::default()
        };

        let json = serde_json::to_string_pretty(&search).unwrap();
        let mut file = File::create("resources/payload.json").unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let result = stat_distribution(search);
        let json = serde_json::to_string_pretty(&result).unwrap();
        info!("Result: {:?}", json);
        let found = result.get("count").unwrap().as_i64().unwrap();

        for c in columns.iter() {
            assert!(result.contains_key(c));
            let value = result.get(c).unwrap().as_array().unwrap()[0]
                .as_f64()
                .unwrap();
            info!("Value: {} = {:?}", c, value);
        }
        assert_eq!(found, 1);
    }

    #[test]
    fn test_estimate_price() {
        configure_log4rs("resources/log4rs.yml");
        let mean: f64 = 30358.79301745636;
        let median: f64 = 28950.0;
        let std: f64 = 8244.372185288039;
        let rsd: f64 = 0.2715645572782657;
        let quantile_60: f64 = 29990.0;
        let quantile_66: f64 = 31300.0;
        let quantile_75: f64 = 32900.0;
        let quantile_80: f64 = 33990.0;
        let quantile_90: f64 = 37180.0;

        // Calculate an estimated price based on the statistics
        let estimated_price = suggest_estimated_price(
            mean,
            median,
            std,
            rsd,
            quantile_60,
            quantile_66,
            quantile_75,
            quantile_80,
            quantile_90,
        );

        // Print the estimated price
        info!("Estimated Price: {}", estimated_price);
    }
    fn suggest_estimated_price(
        mean: f64,
        median: f64,
        std: f64,
        rsd: f64,
        quantile_60: f64,
        quantile_66: f64,
        quantile_75: f64,
        quantile_80: f64,
        quantile_90: f64,
    ) -> f64 {
        // Define weights for the statistics
        let weight_mean = 0.25;
        let weight_median = 0.25;
        let weight_quantile_60 = 0.10;
        let weight_quantile_66 = 0.10;
        let weight_quantile_75 = 0.10;
        let weight_quantile_80 = 0.10;
        let weight_quantile_90 = 0.10;

        // Calculate the weighted average
        let weighted_average = weight_mean * mean
            + weight_median * median
            + weight_quantile_60 * quantile_60
            + weight_quantile_66 * quantile_66
            + weight_quantile_75 * quantile_75
            + weight_quantile_80 * quantile_80
            + weight_quantile_90 * quantile_90;

        // Adjust the estimate based on RSD and Std
        let adjustment_factor = if rsd > 0.3 {
            1.1 // if RSD is high, increase the estimate slightly
        } else if rsd < 0.1 {
            0.95 // if RSD is low, decrease the estimate slightly
        } else {
            1.0 // no adjustment
        };

        let adjusted_estimate = weighted_average * adjustment_factor;

        // Optionally, we could add a factor based on standard deviation

        if std > 10000.0 {
            adjusted_estimate * 1.05 // if std is very high, slightly increase the estimate
        } else if std < 5000.0 {
            adjusted_estimate * 0.95 // if std is very low, slightly decrease the estimate
        } else {
            adjusted_estimate
        }
    }

    #[test]
    fn test_with_json() {
        configure_log4rs("resources/log4rs.yml");

        let message: String = fs::read_to_string("resources/payload.json").unwrap();
        let payload = serde_json::from_str::<StatisticSearchPayload>(&message).unwrap();
        let functions = payload.aggregators.clone();
        let column = "price_in_eur".to_string();
        let columns = functions
            .unwrap()
            .iter()
            .map(|f| format!("{}_{}", column, f))
            .collect::<Vec<_>>();
        let result = stat_distribution(payload);
        let json = serde_json::to_string_pretty(&result).unwrap();
        info!("Result: {:?}", json);
        let found = result.get("count").unwrap().as_i64().unwrap();

        for c in columns.iter() {
            assert!(result.contains_key(c));
            let value = result.get(c).unwrap().as_array().unwrap()[0]
                .as_f64()
                .unwrap();
            info!("Value: {} = {:?}", c, value);
        }
        assert_eq!(found, 1);
    }

    #[test]
    fn test_PriceDistribution() {
        configure_log4rs("resources/log4rs.yml");
        let column = "price_in_eur".to_string();
        let df = super::PRICE_DATA.clone();
        let df = df
            .with_columns(&[
                col("make"),
                col("model"),
                col("year"),
                col("engine"),
                col("gearbox"),
                col("mileage_breakdown"),
                col("power_breakdown"),
                col(&column),
                when(col(&column).lt(5_000.0))
                    .then(lit("5K"))
                    .when(col(&column).lt(10_000.0))
                    .then(lit("10K"))
                    .when(col(&column).lt(15_000.0))
                    .then(lit("15K"))
                    .when(col(&column).lt(20_000.0))
                    .then(lit("20K"))
                    .when(col(&column).lt(25_000.0))
                    .then(lit("25K"))
                    .when(col(&column).lt(30_000.0))
                    .then(lit("30K"))
                    .when(col(&column).lt(40_000.0))
                    .then(lit("40K"))
                    .when(col(&column).lt(50_000.0))
                    .then(lit("50K"))
                    .when(col(&column).lt(60_000.0))
                    .then(lit("60K"))
                    .when(col(&column).lt(70_000.0))
                    .then(lit("70K"))
                    .when(col(&column).lt(80_000.0))
                    .then(lit("80K"))
                    .when(col(&column).lt(90_000.0))
                    .then(lit("90K"))
                    .when(col(&column).lt(100_000.0))
                    .then(lit("100K"))
                    .otherwise(lit("100K+"))
                    .alias("price_breakdown"),
                when(col(&column).lt(5_000.0))
                    .then(lit(1))
                    .when(col(&column).lt(10_000.0))
                    .then(lit(2))
                    .when(col(&column).lt(15_000.0))
                    .then(lit(3))
                    .when(col(&column).lt(20_000.0))
                    .then(lit(5))
                    .when(col(&column).lt(25_000.0))
                    .then(lit(6))
                    .when(col(&column).lt(30_000.0))
                    .then(lit(7))
                    .when(col(&column).lt(40_000.0))
                    .then(lit(8))
                    .when(col(&column).lt(50_000.0))
                    .then(lit(9))
                    .when(col(&column).lt(60_000.0))
                    .then(lit(10))
                    .when(col(&column).lt(70_000.0))
                    .then(lit(11))
                    .when(col(&column).lt(80_000.0))
                    .then(lit(12))
                    .when(col(&column).lt(90_000.0))
                    .then(lit(13))
                    .when(col(&column).lt(100_000.0))
                    .then(lit(14))
                    .otherwise(lit(15))
                    .alias("price_breakdown_order"),
            ])
            .collect()
            .unwrap();
        let sort = SortMultipleOptions::new()
            .with_order_descending_multi(vec![false])
            .with_nulls_last(true);
        let grouped_df = df
            .lazy()
            .group_by(vec![col("price_breakdown"), col("price_breakdown_order")])
            .agg(&[
                col(&column).count().alias("count"),
                col(&column)
                    .mean()
                    .cast(polars::prelude::DataType::Int32)
                    .alias("mean"),
                col(&column)
                    .median()
                    .cast(polars::prelude::DataType::Int32)
                    .alias("median"),
                (col(&column).std(1) * lit(100) / col(&column).mean())
                    .cast(polars::prelude::DataType::Int32)
                    .alias("rsd"),
            ])
            .sort(vec!["price_breakdown_order".to_string()], sort)
            .collect()
            .unwrap();
        log::info!("{:?}", grouped_df);
        let json = to_generic_json(&grouped_df);
        log::info!("{:?}", json);
    }
}
