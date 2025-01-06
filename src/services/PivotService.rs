use std::{collections::HashMap, vec};

use log::{error, info};
use polars::{
    error::PolarsError,
    frame::DataFrame,
    prelude::{
        col, lit, pivot::pivot, ChunkUnique, DataType, IntoLazy, LazyFrame, SortMultipleOptions,
    },
};
use serde_json::{json, Value};

use crate::{
    model::AxumAPIModel::PivotData,
    services::{
        process_datasets, process_datasets_non_pivoted,
        Utils::{generate_colors, get_aggregator, to_aggregator, to_predicate},
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
        pivot(
            &grouped,
            vec![pivot_column],                    // Pivot column
            Some(base_cols.clone()),               // Index columns
            Some([pivot_data.y_function.clone()]), // Values column
            true,                                  // Sort pivoted columns
            None,                                  // No additional aggregation needed
            None,                                  // No separator
        )?
    } else {
        info!("No pivot column specified. Data set: {:?}", &grouped);
        grouped
    };

    let filled_pivoted = pivoted.lazy().fill_null(lit(0)).collect()?;
    info!("Pivoted data: {:?}", &filled_pivoted);
    // ------------------------------------------------------------------
    // 4) Extract Labels and Datasets
    // ------------------------------------------------------------------
    let x_col_as_str = filled_pivoted
        .column(&pivot_data.x_column)?
        .cast(&DataType::String)?;
    let x_col = x_col_as_str.str()?;
    let labels = if pivot_data.pivot_column.is_none() {
        x_col
            .unique()?
            .into_iter()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|opt| opt.unwrap_or_else(|| "Unknown").to_string())
            .collect::<Vec<_>>()
    } else {
        if let Some(second_column) = &pivot_data.second_x_column {
            let second_x_col_as_str = filled_pivoted
                .column(second_column)?
                .cast(&DataType::String)?;
            let second_x_col = second_x_col_as_str.str()?;
            x_col
                .into_iter()
                .zip(second_x_col.into_iter())
                .map(|(x, y)| {
                    match (x, y) {
                        (Some(x), Some(y)) => format!("{} ({})", x, y), // Combine x and y columns
                        (Some(x), None) => x.to_string(),
                        _ => "Unknown".to_string(),
                    }
                })
                .collect::<Vec<_>>()
        } else {
            x_col
                .into_iter()
                .map(|opt| opt.unwrap_or_else(|| "Unknown").to_string())
                .collect::<Vec<_>>()
        }
    };

    let datasets = if pivot_data.pivot_column.is_none() {
        info!("Processing non-pivoted data");
        process_datasets_non_pivoted(
            &filled_pivoted,
            &pivot_data.x_column,
            &pivot_data.y_function,
            "engine",
            generate_colors,
        )
        .unwrap()
    } else {
        process_datasets(&filled_pivoted, &base_cols, generate_colors).unwrap()
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
