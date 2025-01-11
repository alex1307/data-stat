pub mod AnalysisService;
pub mod ChartServices;
pub mod EnumService;
pub mod PivotService;
pub mod PriceCalculatorService;
pub mod Utils;
pub mod VehicleService;

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

pub fn extract_generic_column_values<T>(data_series: &Series) -> PolarsResult<Vec<T>>
where
    T: From<String> + From<i32> + From<f32> + From<u32> + From<f64> + Default,
{
    match data_series.dtype() {
        DataType::String => data_series
            .str()?
            .into_iter()
            .map(|opt| opt.map_or_else(|| T::default(), |val| T::from(val.to_string())))
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

pub fn extract_labels(
    filled_pivoted: &DataFrame,
    x_column: &str,
) -> Result<Vec<String>, PolarsError> {
    let binding = filled_pivoted.column(x_column)?.cast(&DataType::String)?;
    let x_col = binding.str()?;

    // Convert the column to a vector of strings, handling missing values
    Ok(x_col
        .unique()?
        .into_iter()
        .map(|opt| opt.unwrap_or("Unknown").to_string())
        .collect())
}

/// A helper trait to make piping syntax more readable.
trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
        f(self)
    }
}

impl<T> Pipe for T {}
