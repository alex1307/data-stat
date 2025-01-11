use std::{
    collections::{HashMap, HashSet},
    vec,
};

use log::{error, info};
use polars::{
    error::PolarsError,
    frame::DataFrame,
    prelude::{col, lit, pivot::pivot, AnyValue, IntoLazy, LazyFrame, SortMultipleOptions},
};
use serde_json::{json, Value};

use crate::{
    model::AxumAPIModel::PivotData,
    services::{
        extract_labels, process_datasets,
        Utils::{generate_colors, to_aggregator, to_predicate},
    },
    PRICE_DATA,
};

pub fn pivot_chart(pivot_request: PivotData) -> HashMap<String, Value> {
    let df: LazyFrame = PRICE_DATA.clone();
    let search = pivot_request.filter.clone();
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

    to_pivot_json(&result, &pivot_request).unwrap()
}

pub fn to_pivot_json(
    data: &DataFrame,
    pivot_data: &PivotData, // Whether to sort pivoted columns
) -> Result<HashMap<String, Value>, PolarsError> {
    let mut json_map = HashMap::new();
    let mut group_cols = vec![pivot_data.x_column.clone()];
    info!("Pivot data: {:?}", pivot_data);
    if let Some(pivot_column) = &pivot_data.pivot_column {
        group_cols.push(pivot_column.clone());
    }

    if let Some(second_x_column) = &pivot_data.second_x_column {
        group_cols.push(second_x_column.clone());
    }

    info!("Group columns: {:?}", group_cols);

    // Ensure all required columns exist

    for col in &group_cols {
        if data.column(col).is_err() {
            json_map.insert(
                "error".to_string(),
                json!(format!("Column '{}' not found in DataFrame", col)),
            );
            error!("Column '{}' not found in DataFrame", col);
            return Ok(json_map);
        }
    }

    let grouped_ok = data.clone().lazy().collect();
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

    let base_cols = if let Some(second_x_col) = &pivot_data.second_x_column {
        vec![pivot_data.x_column.clone(), second_x_col.clone()]
    } else {
        vec![pivot_data.x_column.clone()]
    };

    let pivoted = if let Some(pivot_column) = &pivot_data.pivot_column {
        info!("Pivoting data...");
        info!("Grouped data: {:?}", &grouped);
        info!("Base columns: {:?}", base_cols);
        info!("Pivot column: {:?}", &pivot_column);
        info!("Y function: {:?}", &pivot_data.y_function);
        pivot(
            &grouped,
            vec![pivot_column],                    // Pivot column
            Some(base_cols.clone()),               // Index columns
            Some([pivot_data.y_function.clone()]), // Values column
            true,                                  // Sort pivoted columns
            Some(polars::prelude::Expr::sum(col(&pivot_data.y_function))), // No additional aggregation needed
            None,                                                          // No separator
        )?
    } else {
        info!("No pivot column specified. Data set: {:?}", &grouped);
        grouped
    };
    info!("Pivoted data: {:?}", &pivoted);
    let filled_pivoted = pivoted.lazy().fill_null(lit(0)).collect()?;
    info!("Pivoted data: {:?}", &filled_pivoted);
    // ------------------------------------------------------------------
    // 4) Extract Labels and Datasets
    // ------------------------------------------------------------------

    let labels = extract_labels(&filled_pivoted, &pivot_data.x_column)?;

    let datasets = if pivot_data.pivot_column.is_none() {
        let columns = if let Some(groups) = pivot_data.filter.group.clone() {
            groups
                .iter()
                .filter(|col| col.as_str() != pivot_data.x_column)
                .cloned()
                .collect::<Vec<String>>()
        } else {
            vec![]
        };

        process_datasets_non_pivoted(
            &filled_pivoted,
            &pivot_data.x_column,
            &pivot_data.y_function,
            columns.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
            generate_colors,
        )?
    } else {
        process_datasets(&filled_pivoted, &base_cols, generate_colors)?
    };
    info!("Datasets: {:?}", datasets);
    info!("Labels: {:?}", labels);
    // ------------------------------------------------------------------
    // 5) Build Final JSON
    // ------------------------------------------------------------------
    json_map.insert("labels".to_string(), json!(labels));
    json_map.insert("datasets".to_string(), Value::Array(datasets));

    Ok(json_map)
}

pub fn process_datasets_non_pivoted(
    df: &DataFrame,
    label_col: &str,          // Column for labels (e.g., years)
    value_col: &str,          // Column for count/values
    grouping_cols: Vec<&str>, // Grouping columns (can be empty for no grouping)
    generate_colors: impl Fn(usize) -> Vec<String>,
) -> Result<Vec<Value>, PolarsError> {
    let unique_groups: HashSet<String> = (0..df.height())
        .map(|row_idx| {
            grouping_cols
                .iter()
                .map(|col| {
                    df.column(col).map(|series| {
                        series
                            .get(row_idx)
                            .unwrap_or(polars::prelude::AnyValue::String("Unknown"))
                            .to_string()
                    })
                })
                .collect::<Result<Vec<_>, PolarsError>>()
                .map(|vec| vec.join(", "))
        })
        .collect::<Result<HashSet<_>, PolarsError>>()?;

    // Generate colors based on the number of unique groups
    let colors = generate_colors(unique_groups.len());

    let mut datasets_map: HashMap<String, Vec<u32>> = HashMap::new();
    let mut labels = HashSet::new();

    // Iterate through the rows of the DataFrame
    info!("Grouping columns: {:?}", grouping_cols);
    info!("Unique groups: {:?}", unique_groups);
    info!("Number of rows: {}", df.height());
    for row_idx in 0..df.height() {
        // Get the label (x-axis value)
        let label = df
            .column(label_col)?
            .get(row_idx)
            .unwrap_or(AnyValue::String("Unknown"))
            .to_string();
        if label == "Unknown" {
            error!("Unknown label found in row: {}", row_idx);
        } else {
            labels.insert(label.clone());
        }

        // Get the grouping key by concatenating all grouping column values
        let group_key = grouping_cols
            .iter()
            .map(|col| {
                df.column(col)
                    .map(|series| {
                        series
                            .get(row_idx)
                            .unwrap_or(polars::prelude::AnyValue::String("Unknown"))
                            .to_string()
                    })
                    .unwrap_or_else(|_| "Unknown".to_string())
            })
            .collect::<Vec<_>>()
            .join(", ");
        info!("Group key: {:?}", group_key);
        // Get the value for this row
        let value = match df.column(value_col)?.get(row_idx) {
            Ok(AnyValue::Float64(val)) => val as u32,
            Ok(AnyValue::Float32(val)) => val as u32,
            Ok(AnyValue::Int64(val)) => val as u32,
            Ok(AnyValue::Int32(val)) => val as u32,
            Ok(AnyValue::UInt64(val)) => val as u32,
            Ok(AnyValue::UInt32(val)) => val,
            _ => 0,
        };
        if value == 0 {
            error!("Zero value found in row: {}", row_idx);
        }
        // Add the label to the label set

        // Update the dataset for the group
        datasets_map.entry(group_key).or_default().push(value);
    }

    info!("Labels: {:?}, count: {}", labels, labels.len());

    // Build the datasets
    let datasets: Vec<Value> = datasets_map
        .into_iter()
        .enumerate()
        .map(|(idx, (label, data))| {
            json!({
                "label": label,
                "data": data,
                "backgroundColor": colors[idx]
            })
        })
        .collect();

    // Return the result
    Ok(datasets)
}
