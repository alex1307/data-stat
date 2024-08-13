use std::collections::HashMap;

use log::info;
use polars::{
    lazy::dsl::{col, lit, Expr},
    prelude::{LazyFrame, SortMultipleOptions},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{STAT_DATA, VEHICLES_DATA};

use super::PriceStatistic::{to_generic_json, to_like_predicate};

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
    search: Option<String>,
    make: Option<String>,
    model: Option<String>,

    engine: Option<Vec<String>>,
    gearbox: Option<String>,

    yearFrom: Option<i32>,
    yearTo: Option<i32>,
    year: Option<i32>,

    powerFrom: Option<i32>,
    powerTo: Option<i32>,
    power: Option<i32>,

    mileageFrom: Option<i32>,
    mileageTo: Option<i32>,
    mileage: Option<i32>,

    ccFrom: Option<i32>,
    ccTo: Option<i32>,
    cc: Option<i32>,

    saveDifference: Option<i32>,
    discount: Option<i32>,

    group: Vec<String>,
    aggregators: Vec<String>,
    order: Vec<Order>,
    stat_column: Option<String>,
    estimated_price: Option<i32>,
}

pub fn search(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df = VEHICLES_DATA.clone();

    // Group by the required columns and calculate the required statistics

    let filterConditions = to_predicate(search.clone());
    //save_diff;extra_charge;discount;increase;price_in_eur;estimated_price_in_eur;safe_diff_in_eur;extra_charge_in_eur;equipment;url;created_on;updated_on

    let filtered = df
        .with_columns(&[
            col("source"),
            col("title"),
            col("make"),
            col("model"),
            col("year"),
            col("engine"),
            col("gearbox"),
            col("power"),
            col("mileage"),
            col("cc"),
            col("currency"),
            col("price"),
            col("estimated_price"),
            col("discount"),
            col("increase"),
            col("save_diff"),
            col("extra_charge"),
            col("price_in_eur"),
            col("estimated_price_in_eur"),
            col("safe_diff_in_eur"),
            col("extra_charge_in_eur"),
            col("equipment"),
            col("url"),
            col("created_on"),
            col("updated_on"),
        ])
        .filter(filterConditions);
    let result = if !search.order.is_empty() {
        let mut columns = Vec::new();
        let mut orders = Vec::new();

        for sort in search.order.iter() {
            columns.push(sort.column.clone());
            orders.push(!sort.asc);
        }
        info!("* Columns: {:?}", columns);
        info!("* Orders: {:?}", orders);
        filtered
            .sort(
                columns,
                SortMultipleOptions::new()
                    .with_order_descending_multi(orders)
                    .with_nulls_last(true),
            )
            .collect()
            .unwrap()
    } else {
        filtered.collect().unwrap()
    };

    to_generic_json(&result)
}

pub fn stat_distribution(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df: LazyFrame = STAT_DATA.clone();

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

fn to_aggregator(aggregators: Vec<String>, column: &str) -> Vec<Expr> {
    let mut agg = vec![];
    for aggregator in aggregators {
        let func = match aggregator.as_str() {
            "count" => col(column).count(),
            "min" => col(column).min(),
            "max" => col(column).max(),
            "mean" => col(column).mean(),
            "median" => col(column).median(),
            "avg" => col(column).sum() / col(column).count(),
            "sum" => col(column).sum(),
            "std" => col(column).std(1),
            "rsd" => col(column).std(1) / col(column).mean(),
            "quantile_60" => col(column).quantile(
                0.60.into(),
                polars::prelude::QuantileInterpolOptions::Nearest,
            ),
            "quantile_66" => col(column).quantile(
                0.66.into(),
                polars::prelude::QuantileInterpolOptions::Nearest,
            ),
            "quantile_70" => col(column).quantile(
                0.70.into(),
                polars::prelude::QuantileInterpolOptions::Nearest,
            ),
            "quantile_75" => col(column).quantile(
                0.75.into(),
                polars::prelude::QuantileInterpolOptions::Nearest,
            ),
            "quantile_80" => col(column).quantile(
                0.8.into(),
                polars::prelude::QuantileInterpolOptions::Nearest,
            ),
            "quantile_90" => col(column).quantile(
                0.90.into(),
                polars::prelude::QuantileInterpolOptions::Nearest,
            ),

            _ => continue,
        };
        agg.push(func.alias(&format!("{}_{}", column, aggregator)));
    }
    agg
}

pub fn to_predicate(search: StatisticSearchPayload) -> Expr {
    let mut predicates = vec![];

    if let Some(search) = search.search {
        info!("search: {:?}", search);
        let mut search_filter = HashMap::new();
        search_filter.insert("title".to_string(), search.clone());
        search_filter.insert("equipment".to_string(), search.clone());
        search_filter.insert("dealer".to_string(), search.clone());
        search_filter.insert("model".to_string(), search.clone());
        let predicate = to_like_predicate(search_filter, true);
        if let Some(p) = predicate {
            predicates.push(p);
        }
    }

    if let Some(make) = search.make {
        info!("Makes: {:?}", make);
        predicates.push(col("make").eq(lit(make)));
    }

    if let Some(model) = search.model {
        predicates.push(col("model").eq(lit(model)));
    }

    if let Some(engine) = search.engine {
        info!("Engines: {:?}", engine);
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
        info!("gearbox: {:?}", gearbox);
        predicates.push(col("gearbox").eq(lit(gearbox)));
    }

    if let Some(estimated_price) = search.estimated_price {
        predicates.push(col("estimated_price_in_eur").gt(lit(estimated_price)));
    }

    if let Some(yearFrom) = search.year {
        info!("Year: {:?}", yearFrom);
        predicates.push(col("year").gt_eq(lit(yearFrom)));
    } else {
        if let Some(yearFrom) = search.yearFrom {
            info!("Year from: {:?}", yearFrom);
            predicates.push(col("year").gt_eq(lit(yearFrom)));
        }
        if let Some(yearTo) = search.yearTo {
            info!("Year To: {:?}", yearTo);
            predicates.push(col("year").lt_eq(lit(yearTo)));
        }
    }
    if let Some(discount) = search.discount {
        info!("Discount: {:?}", discount);
        predicates.push(col("discount").gt_eq(lit(discount)));
    }

    if let Some(saveDifference) = search.saveDifference {
        info!("Save Difference: {:?}", saveDifference);
        predicates.push(col("save_diff_in_eur").gt_eq(lit(saveDifference)));
    }

    if let Some(power) = search.power {
        info!("Power: {:?}", power);
        predicates.push(col("power").eq(lit(power)));
    } else {
        info!("Power from: {:?}", search.powerFrom);
        info!("Power to: {:?}", search.powerTo);
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
        info!("Mileage from: {:?}", search.mileageFrom);
        info!("Mileage to: {:?}", search.mileageTo);
        if let Some(mileageFrom) = search.mileageFrom {
            predicates.push(col("mileage").gt_eq(lit(mileageFrom)));
        }
        if let Some(mileageTo) = search.mileageTo {
            predicates.push(col("mileage").lt_eq(lit(mileageTo)));
        }
    }

    if let Some(cc) = search.cc {
        info!("CC: {:?}", cc);
        predicates.push(col("cc").eq(lit(cc)));
    } else {
        if let Some(ccFrom) = search.ccFrom {
            info!("CC from: {:?}", search.ccFrom);
            predicates.push(col("cc").gt_eq(lit(ccFrom)));
        }
        if let Some(ccTo) = search.ccTo {
            info!("CC to: {:?}", search.ccTo);
            predicates.push(col("cc").lt_eq(lit(ccTo)));
        }
    }

    let combined_predicates = predicates
        .into_iter()
        .reduce(|acc, pred| acc.and(pred))
        .unwrap();
    info!("Predicates: {:?}", combined_predicates);
    combined_predicates
}

#[cfg(test)]
mod test_stat {
    use std::{
        fs::{self, File},
        io::Write,
    };

    use log::info;
    use polars::prelude::{all, col, lit};

    use crate::{
        configure_log4rs,
        services::Statistic::{stat_distribution, Order, StatisticSearchPayload},
    };

    #[test]
    fn test_load_data() {
        configure_log4rs("resources/log4rs.yml");
        let df = super::STAT_DATA.clone();
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
}
