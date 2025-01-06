pub mod AnalysisService;
pub mod ChartServices;
pub mod EnumService;
pub mod PivotService;
pub mod PriceCalculatorService;
pub mod Utils;
pub mod VehicleService;

use std::collections::HashMap;

use log::info;
use polars::error::PolarsResult;
use polars::prelude::*;
use serde_json::{json, Value};

pub fn process_datasets(
    filled_pivoted: &DataFrame,
    base_cols: &[String],
    generate_colors: impl Fn(usize) -> Vec<String>,
) -> Result<Vec<Value>, PolarsError> {
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
            let data = extract_column_values(data_series.as_series().unwrap())?;
            datasets.push(json!({
                "label": value,
                "data": data,
                "backgroundColor": colors[idx]
            }));
        }
    }
    Ok(datasets)
}

pub fn process_datasets_non_pivoted(
    df: &DataFrame,
    label_col: &str,  // Column for labels (e.g., years)
    value_col: &str,  // Column for count/values
    engine_col: &str, // Column for unique values (e.g., engines)
    generate_colors: impl Fn(usize) -> Vec<String>,
) -> Result<Vec<Value>, PolarsError> {
    let axis_values = df
        .column(label_col)?
        .cast(&DataType::String)? // Ensure label column is cast to String for processing
        .str()?
        .unique()?
        .into_iter()
        .filter_map(|opt| opt.map(String::from)) // Convert Option<&str> to String
        .collect::<Vec<String>>();

    let unique_engines = df
        .column(engine_col)?
        .cast(&DataType::String)?
        .str()?
        .unique()?
        .into_iter()
        .filter_map(|opt| opt.map(String::from))
        .collect::<Vec<String>>();

    let mut datasets = vec![];
    let colors = generate_colors(unique_engines.len());

    for (idx, engine) in unique_engines.iter().enumerate() {
        let mut data = vec![0; axis_values.len()]; // Initialize with 0 for all years

        for (axis_idx, axis_value) in axis_values.iter().enumerate() {
            let engine_mask = df
                .column(engine_col)?
                .cast(&DataType::String)?
                .str()?
                .into_iter()
                .map(|opt| opt == Some(engine.as_str()))
                .collect::<BooleanChunked>();

            let year_mask = df
                .column(label_col)?
                .cast(&DataType::String)?
                .str()?
                .into_iter()
                .map(|opt| opt == Some(axis_value.as_str()))
                .collect::<BooleanChunked>();

            let combined_mask = &engine_mask & &year_mask; // Combine masks to filter by engine and year
            let filtered_df = df.filter(&combined_mask)?;
            let value = match filtered_df.column(value_col)?.dtype() {
                DataType::String => filtered_df
                    .column(value_col)?
                    .str()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or("").parse::<f64>().unwrap_or(0.0))
                    .collect::<Vec<f64>>()
                    .pipe(Ok),
                DataType::Float64 => filtered_df
                    .column(value_col)?
                    .f64()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0.0))
                    .collect::<Vec<f64>>()
                    .pipe(Ok),
                DataType::UInt32 => filtered_df
                    .column(value_col)?
                    .u32()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64)
                    .collect::<Vec<f64>>()
                    .pipe(Ok),
                DataType::UInt64 => filtered_df
                    .column(value_col)?
                    .u64()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64)
                    .collect::<Vec<f64>>()
                    .pipe(Ok),
                DataType::Int32 => filtered_df
                    .column(value_col)?
                    .i32()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64)
                    .collect::<Vec<f64>>()
                    .pipe(Ok),
                DataType::Int64 => filtered_df
                    .column(value_col)?
                    .i64()?
                    .into_iter()
                    .map(|opt| opt.unwrap_or(0) as f64)
                    .collect::<Vec<f64>>()
                    .pipe(Ok),
                _ => Err(PolarsError::ComputeError(
                    format!(
                        "Unsupported data type: {:?}",
                        filtered_df.column(value_col)?.dtype()
                    )
                    .into(),
                )),
            };
            if let Ok(nvalue) = value {
                data[axis_idx] = nvalue.iter().sum::<f64>() as i32;
            }
        }

        datasets.push(json!({
            "label": engine,
            "data": data,
            "backgroundColor": colors[idx]
        }));
    }

    Ok(datasets)
}

pub fn extract_generic_column_values<T>(data_series: &Series) -> PolarsResult<Vec<T>>
where
    T: From<String> + From<i32> + From<f32> + From<u32> + From<f64> + Default,
{
    match data_series.dtype() {
        DataType::String => data_series
            .str()?
            .into_iter()
            .map(|opt| {
                opt.map_or_else(
                    || T::default(),
                    |val| T::try_from(val.to_string()).unwrap_or_else(|_| T::default()),
                )
            })
            .collect::<Vec<T>>()
            .pipe(Ok),
        DataType::Float32 => data_series
            .f32()?
            .into_iter()
            .map(|opt| opt.map_or_else(|| T::default(), |val| T::from(val)))
            .collect::<Vec<T>>()
            .pipe(Ok),
        DataType::Int32 => data_series
            .i32()?
            .into_iter()
            .map(|opt| opt.map_or_else(|| T::default(), |val| T::from(val)))
            .collect::<Vec<T>>()
            .pipe(Ok),
        DataType::UInt32 => data_series
            .u32()?
            .into_iter()
            .map(|opt| opt.map_or_else(|| T::default(), |val| T::from(val)))
            .collect::<Vec<T>>()
            .pipe(Ok),
        DataType::Float64 => data_series
            .f64()?
            .into_iter()
            .map(|opt| opt.map_or_else(|| T::default(), |val| T::from(val)))
            .collect::<Vec<T>>()
            .pipe(Ok),
        _ => Err(PolarsError::ComputeError(
            format!("Unsupported data type: {:?}", data_series.dtype()).into(),
        )),
    }
}

/// Extracts and converts column values to a `Vec<f64>` from a given `Series`.
/// Supports various data types and provides a unified result.
///
/// # Arguments
/// * `data_series` - A reference to the `Series` from which to extract values.
///
/// # Returns
/// * A `Result<Vec<f64>>` containing the converted values or an error if the type is unsupported.
pub fn extract_column_values(data_series: &Series) -> PolarsResult<Vec<f64>> {
    match data_series.dtype() {
        DataType::String => data_series
            .str()?
            .into_iter()
            .map(|opt| opt.unwrap_or("").parse::<f64>().unwrap_or(0.0))
            .collect::<Vec<f64>>()
            .pipe(Ok),
        DataType::Float64 => data_series
            .f64()?
            .into_iter()
            .map(|opt| opt.unwrap_or(0.0))
            .collect::<Vec<f64>>()
            .pipe(Ok),
        DataType::UInt32 => data_series
            .u32()?
            .into_iter()
            .map(|opt| opt.unwrap_or(0) as f64)
            .collect::<Vec<f64>>()
            .pipe(Ok),
        DataType::UInt64 => data_series
            .u64()?
            .into_iter()
            .map(|opt| opt.unwrap_or(0) as f64)
            .collect::<Vec<f64>>()
            .pipe(Ok),
        DataType::Int32 => data_series
            .i32()?
            .into_iter()
            .map(|opt| opt.unwrap_or(0) as f64)
            .collect::<Vec<f64>>()
            .pipe(Ok),
        DataType::Int64 => data_series
            .i64()?
            .into_iter()
            .map(|opt| opt.unwrap_or(0) as f64)
            .collect::<Vec<f64>>()
            .pipe(Ok),
        _ => Err(PolarsError::ComputeError(
            format!("Unsupported data type: {:?}", data_series.dtype()).into(),
        )),
    }
}

/// A helper trait to make piping syntax more readable.
trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
        f(self)
    }
}

impl<T> Pipe for T {}
