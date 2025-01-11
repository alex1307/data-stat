use std::collections::HashMap;

use log::info;

use polars::{
    error::PolarsError,
    lazy::dsl::col,
    prelude::{lit, pivot::pivot, IntoLazy, LazyFrame, SortMultipleOptions},
};

use serde_json::Value;

use crate::{
    model::AxumAPIModel::{PivotData, StatisticSearchPayload},
    services::Utils::{to_aggregator, to_predicate},
    PRICE_DATA,
};

use super::VehicleService::to_generic_json;

pub struct StatisticService {
    pub price_data: LazyFrame,
}

pub fn stat_distribution(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let filtered = filterAndAggregateData(&search).unwrap();
    process_results(filtered, search).unwrap()
}

pub fn pivot_distribution(payload: PivotData) -> Result<HashMap<String, Value>, PolarsError> {
    let filter = payload.filter.clone();
    let group = match &filter.group {
        Some(group) if !group.is_empty() => group,
        _ => return Err(PolarsError::ComputeError("Group is required".into()))?,
    };
    if group.is_empty() {
        return Err(PolarsError::ComputeError(
            "At least one group by column is required".into(),
        ));
    }

    // Group by the required columns and calculate the required statistics

    let data = filterAndAggregateData(&filter)?;
    if let Some(pivot_column) = payload.pivot_column {
        let index = group
            .into_iter()
            .filter(|c| c.as_str() != pivot_column.as_str())
            .map(|c| c.clone())
            .collect::<Vec<String>>();
        let pivoted = pivot(
            &data.collect().unwrap(),
            vec![pivot_column],                 // Pivot column
            Some(index),                        // Index columns
            Some([payload.y_function.clone()]), // Values column
            true,                               // Sort pivoted columns
            Some(polars::prelude::Expr::sum(col(&payload.y_function))), // No additional aggregation needed
            None,                                                       // No separator
        )
        .unwrap();
        info!("{:?}", pivoted);
        process_results(
            pivoted.lazy().fill_null(lit(0)).collect()?.lazy(),
            payload.filter,
        )
    } else {
        Ok(stat_distribution(payload.filter))
    }
}

pub fn filterAndAggregateData(search: &StatisticSearchPayload) -> Result<LazyFrame, PolarsError> {
    use polars::prelude::*;

    // Clone the PRICE_DATA LazyFrame
    let df: LazyFrame = PRICE_DATA.clone();

    // Log the incoming search payload
    info!("Payload: {:?}", search);

    // Validate the group fields in the search payload
    let group = match search.group.clone() {
        Some(group) if !group.is_empty() => group,
        _ => return Err(PolarsError::ComputeError("Group is required".into())),
    };

    // Validate the aggregators in the search payload
    let aggregators = match search.aggregators.clone() {
        Some(aggregators) if !aggregators.is_empty() => aggregators,
        _ => return Err(PolarsError::ComputeError("Aggregators are required".into())),
    };

    // Prepare the group-by columns
    let by = group.iter().map(col).collect::<Vec<_>>();

    // Determine the stat column or use a default value
    let stat_column = search
        .clone()
        .stat_column
        .unwrap_or_else(|| "price_in_eur".to_string());

    // Convert aggregators into their corresponding Polars aggregation expressions
    let aggregators = to_aggregator(aggregators.clone(), &stat_column);

    // Build filter conditions from the search payload
    let filter_conditions = to_predicate(search.clone());

    // Select relevant columns
    let selected_columns = vec![
        "make",
        "model",
        "year",
        "engine",
        "gearbox",
        "power",
        "mileage",
        "cc",
        "mileage_breakdown",
        "power_breakdown",
        "price_in_eur",
        "estimated_price_in_eur",
        "save_diff_in_eur",
        "extra_charge_in_eur",
        "discount",
        "increase",
    ]
    .into_iter()
    .map(col)
    .collect::<Vec<_>>();

    // Return the LazyFrame with transformations applied
    Ok(df
        .with_columns(&selected_columns)
        .filter(filter_conditions)
        .group_by(by.as_slice())
        .agg(&aggregators))
}

pub fn process_results(
    filtered: LazyFrame,
    search: StatisticSearchPayload,
) -> Result<HashMap<String, Value>, PolarsError> {
    // Check if sorting is required
    let result_df = if !search.order.is_empty() {
        // Prepare columns and sorting orders
        let (columns, orders): (Vec<_>, Vec<_>) = search
            .order
            .iter()
            .map(|sort| (sort.column.clone(), !sort.asc)) // Convert asc to desc logic
            .unzip();

        // Log sorting details
        info!("Columns: {:?}", columns);
        info!("Orders: {:?}", orders);

        // Configure sorting options
        let sort_options = SortMultipleOptions::new()
            .with_order_descending_multi(orders)
            .with_nulls_last(true);

        // Apply sorting to the LazyFrame
        filtered.sort(&columns, sort_options).collect()?
    } else {
        // No sorting, materialize the LazyFrame
        filtered.collect()?
    };

    // Convert the resulting DataFrame to a JSON-compatible structure
    Ok(to_generic_json(&result_df))
}

//pub fn chart_data(search: StatisticSearchPayload) -> HashMap<String, Value> {

/// Utility: Convert a Polars Series to a Vec<String>, handling common types
/// (e.g., Int32, Int64, Float64, LargeUtf8, etc.).

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
