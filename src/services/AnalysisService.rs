use std::{
    collections::{BTreeMap, HashMap as StdHashMap, HashMap, HashSet},
    vec,
};

use log::info;

use polars::{
    frame::DataFrame,
    lazy::dsl::col,
    prelude::{DataType, LazyFrame, SortMultipleOptions},
    series::Series,
};

use serde_json::{json, Value};

use crate::{
    model::AxumAPIModel::{PivotData, StatisticSearchPayload},
    services::{
        PivotService::to_pivot_json,
        Utils::{to_aggregator, to_predicate},
    },
    PRICE_DATA,
};

use super::VehicleService::to_generic_json;

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

//pub fn chart_data(search: StatisticSearchPayload) -> HashMap<String, Value> {
pub fn chart_data(payload: PivotData) -> HashMap<String, Value> {
    let search = payload.filter.clone();
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
            &payload, // Sort pivoted columns
        )
        .unwrap()
    }
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
            AnalysisService::{stat_distribution, StatisticSearchPayload},
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
