use std::collections::HashMap;

use chrono::{NaiveDate, TimeDelta, Utc};
use log::info;
use polars::{
    lazy::dsl::{col, lit, Expr},
    prelude::{LazyFrame, SortMultipleOptions},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::PRICE_DATA;

use super::VehicleService::{to_generic_json, to_like_predicate};

pub struct StatisticService {
    pub price_data: LazyFrame,
}
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Order {
    pub column: String,
    pub asc: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct StatisticSearchPayload {
    pub search: Option<String>,
    pub(crate) make: Option<String>,
    pub(crate) model: Option<String>,

    pub(crate) engine: Option<Vec<String>>,
    pub(crate) gearbox: Option<String>,

    pub(crate) yearFrom: Option<i32>,
    pub(crate) yearTo: Option<i32>,
    pub(crate) year: Option<i32>,

    pub powerFrom: Option<i32>,
    pub powerTo: Option<i32>,
    pub power: Option<i32>,

    pub mileageFrom: Option<i32>,
    pub mileageTo: Option<i32>,
    pub mileage: Option<i32>,

    pub ccFrom: Option<i32>,
    pub ccTo: Option<i32>,
    pub cc: Option<i32>,

    pub(crate) saveDiffFrom: Option<i32>,
    pub(crate) saveDiffTo: Option<i32>,

    pub(crate) discountFrom: Option<i32>,
    pub(crate) discountTo: Option<i32>,

    pub(crate) createdOnFrom: Option<i32>,
    pub(crate) createdOnTo: Option<i32>,

    pub(crate) group: Vec<String>,
    pub(crate) aggregators: Vec<String>,
    pub(crate) order: Vec<Order>,
    pub(crate) stat_column: Option<String>,
    pub(crate) estimated_price: Option<i32>,
    pub(crate) price: Option<i32>,
}

pub fn stat_distribution(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df: LazyFrame = PRICE_DATA.clone();

    // Group by the required columns and calculate the required statistics
    info!("Payload: {:?}", search);
    let filterConditions = to_predicate(search.clone());
    let by = search.group.iter().map(|k| col(k)).collect::<Vec<_>>();
    let stat_column = search
        .stat_column
        .clone()
        .unwrap_or("price_in_eur".to_string());
    let aggregators = to_aggregator(search.aggregators.clone(), &stat_column);

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

pub fn to_aggregator(aggregators: Vec<String>, column: &str) -> Vec<Expr> {
    let mut agg = vec![];
    for aggregator in aggregators {
        let func = match aggregator.as_str() {
            "count" => col(column).count(),
            "min" => col(column).min(),
            "max" => col(column).max(),
            "mean" => col(column).mean(),
            "median" => col(column).median(),
            "sum" => col(column).sum(),
            "avg" => col(column).sum() / col(column).count(),
            "std" => col(column).std(1),
            "rsd" => col(column).std(1) / col(column).mean(),
            "quantile_60" => {
                col(column).quantile(0.60.into(), polars::prelude::QuantileMethod::Nearest)
            }
            "quantile_66" => {
                col(column).quantile(0.66.into(), polars::prelude::QuantileMethod::Nearest)
            }
            "quantile_70" => {
                col(column).quantile(0.70.into(), polars::prelude::QuantileMethod::Nearest)
            }
            "quantile_75" => {
                col(column).quantile(0.75.into(), polars::prelude::QuantileMethod::Nearest)
            }
            "quantile_80" => {
                col(column).quantile(0.8.into(), polars::prelude::QuantileMethod::Nearest)
            }
            "quantile_90" => {
                col(column).quantile(0.90.into(), polars::prelude::QuantileMethod::Nearest)
            }

            _ => continue,
        };
        agg.push(func.alias(&aggregator));
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
        services::{
            PriceService::{stat_distribution, Order, StatisticSearchPayload},
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
            group: vec!["make".to_string(), "model".to_string()],
            aggregators: functions,
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
        let final_estimate = if std > 10000.0 {
            adjusted_estimate * 1.05 // if std is very high, slightly increase the estimate
        } else if std < 5000.0 {
            adjusted_estimate * 0.95 // if std is very low, slightly decrease the estimate
        } else {
            adjusted_estimate
        };

        final_estimate
    }

    #[test]
    fn test_with_json() {
        configure_log4rs("resources/log4rs.yml");

        let message: String = fs::read_to_string("resources/payload.json").unwrap();
        let payload = serde_json::from_str::<StatisticSearchPayload>(&message).unwrap();
        let functions = payload.aggregators.clone();
        let column = "price_in_eur".to_string();
        let columns = functions
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
            .sort(&vec!["price_breakdown_order".to_string()], sort)
            .collect()
            .unwrap();
        log::info!("{:?}", grouped_df);
        let json = to_generic_json(&grouped_df);
        log::info!("{:?}", json);
    }
}
