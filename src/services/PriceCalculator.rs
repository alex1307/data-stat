use std::collections::HashMap;

use log::info;
use polars::prelude::{col, lit};
use serde_json::{json, Value};

use crate::ESTIMATED_PRICES_DATA;

use super::{
    PriceStatistic::to_generic_json,
    Statistic::{to_predicate, StatisticSearchPayload},
};

const MILEAGE: [(i32, i32); 8] = [
    (0, 20000),
    (20001, 40000),
    (40001, 60000),
    (60001, 80000),
    (80001, 100000),
    (100001, 120000),
    (120001, 150000),
    (150001, 999999),
];

const POWER: [(i32, i32); 10] = [
    (0, 90),
    (91, 130),
    (131, 150),
    (151, 200),
    (201, 252),
    (253, 303),
    (304, 358),
    (359, 404),
    (405, 454),
    (455, 9999),
];

pub fn calculateStatistic(filter: StatisticSearchPayload) -> HashMap<String, Value> {
    let result = calculate(filter);
    let rsd = result.get("rsd").unwrap().as_array().unwrap();
    let mean = result.get("mean").unwrap().as_array().unwrap();
    let median = result.get("median").unwrap().as_array().unwrap();
    let quantile_66 = result.get("quantile_66").unwrap().as_array().unwrap();
    let quantile_75 = result.get("quantile_75").unwrap().as_array().unwrap();
    let quantile_80 = result.get("quantile_80").unwrap().as_array().unwrap();
    let quantile_85 = result.get("quantile_85").unwrap().as_array().unwrap();
    let max = result.get("max").unwrap().as_array().unwrap();
    let count = result.get("count").unwrap().as_array().unwrap();

    // Converting to f64
    let count = count[0].as_i64().unwrap();
    let rsd = rsd[0].as_f64().unwrap();
    let mean = mean[0].as_f64().unwrap();
    let median = median[0].as_f64().unwrap();
    let q66 = quantile_66[0].as_f64().unwrap();
    let q75 = quantile_75[0].as_f64().unwrap();
    let q80 = quantile_80[0].as_f64().unwrap();
    let q85 = quantile_85[0].as_f64().unwrap();
    let max = max[0].as_f64().unwrap();
    let estimation = calculate_estimated_value(&result);
    let mut response = HashMap::new();
    response.insert("rsd".to_string(), json!((rsd * 100.0).round() as i32));
    response.insert("count".to_string(), json!(count));
    response.insert("mean".to_string(), json!(mean.round() as i32));
    response.insert("median".to_string(), json!(median.round() as i32));
    response.insert("quantile_66".to_string(), json!(q66.round() as i32));
    response.insert("quantile_75".to_string(), json!(q75.round() as i32));
    response.insert("quantile_80".to_string(), json!(q80.round() as i32));
    response.insert("quantile_85".to_string(), json!(q85.round() as i32));
    response.insert("max".to_string(), json!(max.round() as i32));
    response.insert("estimation".to_string(), json!(estimation.round() as i32));
    response
}

fn calculate(filter: StatisticSearchPayload) -> HashMap<String, Value> {
    let column = match filter.stat_column.clone() {
        Some(v) => v.clone(),
        None => "estimated_price_in_eur".to_string(),
    };
    let vehicles = ESTIMATED_PRICES_DATA.clone();

    // Group by the required columns and calculate the required statistics
    let mut reduced = filter;

    let (filterConditions, count) = getCount(&mut reduced, &vehicles);
    if count == 0 {
        let mut error = HashMap::new();
        error.insert("error".to_string(), json!("No data found"));
        error
    } else {
        let data = vehicles
            .with_columns(vec![
                col("make"),
                col("model"),
                col("year"),
                col("engine"),
                col("gearbox"),
                col("power"),
                col("mileage"),
                col("cc"),
                col("equipment"),
                col("title"),
                col("price_in_eur"),
                col("estimated_price_in_eur"),
                lit(1).alias("GroupBy"),
            ])
            .filter(filterConditions)
            .group_by(vec![col("GroupBy")])
            .agg(vec![
                col(&column).count().alias("count"),
                col(&column).min().alias("min"),
                col(&column).mean().alias("mean"),
                col(&column).median().alias("median"),
                col(&column)
                    .quantile(
                        0.66.into(),
                        polars::prelude::QuantileInterpolOptions::Nearest,
                    )
                    .alias("quantile_66"),
                col(&column)
                    .quantile(
                        0.75.into(),
                        polars::prelude::QuantileInterpolOptions::Nearest,
                    )
                    .alias("quantile_75"),
                col(&column)
                    .quantile(
                        0.8.into(),
                        polars::prelude::QuantileInterpolOptions::Nearest,
                    )
                    .alias("quantile_80"),
                col(&column)
                    .quantile(
                        0.85.into(),
                        polars::prelude::QuantileInterpolOptions::Nearest,
                    )
                    .alias("quantile_85"),
                col(&column).max().alias("max"),
                col(&column).std(1).alias("std"),
                (col(&column).std(1) / col(&column).mean()).alias("rsd"),
            ])
            .collect()
            .unwrap();
        to_generic_json(&data)
    }
}

fn getCount(
    reduced: &mut StatisticSearchPayload,
    vehicles: &polars::prelude::LazyFrame,
) -> (polars::prelude::Expr, usize) {
    for (from, to) in MILEAGE.iter() {
        if let Some(mileage) = reduced.mileage {
            if mileage >= *from && mileage <= *to {
                reduced.mileageFrom = Some(*from);
                reduced.mileageTo = Some(*to);
                break;
            }
        }
    }
    reduced.mileage = None;
    for (from, to) in POWER.iter() {
        if let Some(power) = reduced.power {
            if power >= *from && power <= *to {
                reduced.powerFrom = Some(*from);
                reduced.powerTo = Some(*to);
                break;
            }
        }
    }
    reduced.power = None;

    let filterConditions = to_predicate(reduced.clone());
    //save_diff;extra_charge;discount;increase;price_in_eur;estimated_price_in_eur;safe_diff_in_eur;extra_charge_in_eur;equipment;url;created_on;updated_on
    let filtered = vehicles
        .clone()
        .filter(filterConditions.clone())
        .collect()
        .unwrap();
    let count = filtered.height();
    if count == 0 {
        info!("No data found for the given search criteria");
        reduced.search = None;
        reduced.cc = None;
    }

    let filterConditions = to_predicate(reduced.clone());
    let result = vehicles
        .clone()
        .filter(filterConditions.clone())
        .collect()
        .unwrap();
    let count = result.height();
    (filterConditions, count)
}

fn calculate_estimated_value(result: &HashMap<String, Value>) -> f64 {
    // Extracting values from the HashMap
    let rsd = result.get("rsd").unwrap().as_array().unwrap();
    let mean = result.get("mean").unwrap().as_array().unwrap();
    let median = result.get("median").unwrap().as_array().unwrap();
    let quantile_66 = result.get("quantile_66").unwrap().as_array().unwrap();
    let quantile_75 = result.get("quantile_75").unwrap().as_array().unwrap();
    let quantile_80 = result.get("quantile_80").unwrap().as_array().unwrap();
    let quantile_85 = result.get("quantile_85").unwrap().as_array().unwrap();
    let max = result.get("max").unwrap().as_array().unwrap();

    // Converting to f64
    let rsd = rsd[0].as_f64().unwrap();
    let mean = mean[0].as_f64().unwrap();
    let median = median[0].as_f64().unwrap();
    let q66 = quantile_66[0].as_f64().unwrap();
    let q75 = quantile_75[0].as_f64().unwrap();
    let q80 = quantile_80[0].as_f64().unwrap();
    let q85 = quantile_85[0].as_f64().unwrap();
    let max = max[0].as_f64().unwrap();

    info!(
        "RSD: {}, Mean: {}, Median: {}, Q66: {}, Q75: {}, Q80: {}, Q85: {}, Max: {}",
        rsd, mean, median, q66, q75, q80, q85, max
    );

    let mut estimation;
    if rsd <= 0.1 {
        let w_mean = (1.0 - rsd) * 0.4;
        let w_median = (1.0 - rsd) * 0.4;
        let w_q66 = (1.0 - rsd) * 0.12;
        let w_q80 = (1.0 - rsd) * 0.1;
        let w_max = rsd;

        // Logging weights (optional)
        println!(
            "Weights: {}, {}, {}, {}, {}",
            w_mean, w_median, w_q66, w_q80, w_max
        );

        // Calculating estimation
        estimation = w_mean * mean + w_median * median + w_q66 * q66 + w_q80 * q80 + w_max * max;
    } else if rsd > 0.1 && rsd <= 0.3 {
        let w_mean = (1.0 - rsd) * 0.2;
        let w_median = (1.0 - rsd) * 0.2;
        let w_66 = (1.0 - rsd) * 0.3;
        let w_75 = 0.15;
        let w_80 = (1.0 - rsd) * 0.3;

        // Logging weights (optional)
        println!(
            "Weights: {}, {}, {}, {}, {}",
            w_mean, w_median, w_75, w_66, w_80
        );

        let w_total = w_mean + w_median + w_66 + w_75 + w_80;

        // Calculating estimation
        estimation = w_mean * mean + w_median * median + w_66 * q66 + w_75 * q75 + w_80 * q80;

        if (w_total - 1.0).abs() > 0.0001 {
            estimation *= 1.0 / w_total;
        }
    } else {
        let w_mean = 0.2;
        let w_median = 0.2;
        let w_66 = 0.25;
        let w_75 = 0.15;
        let w_80 = 0.10;
        let w_max = 0.10;

        // Logging weights (optional)

        // Calculating estimation
        estimation =
            w_mean * mean + w_median * median + w_66 * q66 + w_75 * q75 + w_80 * q80 + w_max * max;
    }

    estimation
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{configure_log4rs, services::PriceCalculator::calculate};
    use serde_json::json;

    #[test]
    fn test_getCount() {
        configure_log4rs("resources/log4rs.yml");
        let search = &mut StatisticSearchPayload {
            make: Some("BMW".to_string()),
            model: Some("320".to_string()),
            year: Some(2018),
            engine: Some(vec!["Petrol".to_string()]),
            power: Some(200),
            mileage: Some(61234),
            gearbox: Some("Automatic".to_string()),
            search: Some("xdrive".to_string()),
            ..Default::default()
        };

        let vehicles = ESTIMATED_PRICES_DATA.clone();
        let (filterConditions, count) = getCount(search, &vehicles);
        assert_eq!(count, 12);
        let result = vehicles.clone().filter(filterConditions).collect().unwrap();
        assert_eq!(result.height(), 12);
    }

    #[test]
    fn test_calculate_rsd_02() {
        configure_log4rs("resources/log4rs.yml");
        let search = &mut StatisticSearchPayload {
            make: Some("BMW".to_string()),
            model: Some("320".to_string()),
            year: Some(2018),
            engine: Some(vec!["Petrol".to_string()]),
            power: Some(200),
            mileage: Some(61234),
            gearbox: Some("Automatic".to_string()),
            search: Some("xdrive".to_string()),
            ..Default::default()
        };

        let result = calculate(search.clone());
        info!("Result: {:?}", result);
        let count = result.get("count").unwrap();
        assert_eq!(count, &json!(vec![12]));
        info!("Count: {:?}", result.get("count").unwrap());
        assert_eq!(result.get("count").unwrap(), &json!(vec![12]));
        assert_eq!(result.get("min").unwrap(), &json!(vec![27309]));
        assert_eq!(result.get("mean").unwrap(), &json!(vec![28809.75]));
        assert_eq!(result.get("median").unwrap(), &json!(vec![28789.0]));
        assert_eq!(result.get("quantile_66").unwrap(), &json!(vec![29035.0]));
        assert_eq!(result.get("quantile_80").unwrap(), &json!(vec![29171.0]));
        assert_eq!(result.get("max").unwrap(), &json!(vec![30337]));
        assert_eq!(result.get("std").unwrap(), &json!(vec![740.2255338132366]));
        assert_eq!(
            result.get("rsd").unwrap(),
            &json!(vec![0.025693577133200966])
        );

        let estimation = calculate_estimated_value(&result);
        info!("Estimation: {}", estimation);
        assert_eq!(estimation, 29463.82658382678);
    }

    #[test]
    fn test_calculate_rsd_40() {
        configure_log4rs("resources/log4rs.yml");
        let search = &mut StatisticSearchPayload {
            make: Some("BMW".to_string()),
            model: Some("320".to_string()),
            year: Some(2018),

            ..Default::default()
        };

        let result = calculate(search.clone());
        info!("Result: {:?}", result);
        let count = result.get("count").unwrap();
        assert_eq!(count, &json!(vec![118]));
        assert_eq!(
            result.get("rsd").unwrap(),
            &json!(vec![0.40588874095168387])
        );

        let estimation = calculate_estimated_value(&result);
        info!("Estimation: {}", estimation);
        assert_eq!(estimation.round() as i32, 29639);
    }

    #[test]
    fn test_calculate_rsd_28() {
        configure_log4rs("resources/log4rs.yml");
        let search = &mut StatisticSearchPayload {
            make: Some("Audi".to_string()),
            model: Some("A4".to_string()),
            mileage: Some(55000),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            year: Some(2018),

            ..Default::default()
        };

        let result = calculate(search.clone());
        info!("Result: {:?}", result);
        let count = result.get("count").unwrap();
        assert_eq!(count, &json!(vec![18]));
        assert_eq!(result.get("rsd").unwrap(), &json!(vec![0.2809766355426564]));

        let estimation = calculate_estimated_value(&result);
        info!("Estimation: {}", estimation);
        assert_eq!(estimation.round() as i32, 29134);
    }

    #[test]
    fn test_getEstimatedPriceStat() {
        configure_log4rs("resources/log4rs.yml");
        let search = StatisticSearchPayload {
            make: Some("Audi".to_string()),
            model: Some("A4".to_string()),
            mileage: Some(55000),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            year: Some(2018),
            stat_column: Some("estimated_price_in_eur".to_string()),
            ..Default::default()
        };
        let stat_json = calculateStatistic(search);
        info!("Result: {:?}", stat_json);
        assert_eq!(stat_json.get("count").unwrap(), &json!(18));
        assert_eq!(stat_json.get("rsd").unwrap(), &json!(28));
        assert_eq!(stat_json.get("mean").unwrap(), &json!(27064));
        assert_eq!(stat_json.get("median").unwrap(), &json!(28750));
        assert_eq!(stat_json.get("estimation").unwrap(), &json!(29134));
    }
    #[test]
    fn test_getPriceStat() {
        configure_log4rs("resources/log4rs.yml");
        let search = StatisticSearchPayload {
            make: Some("Audi".to_string()),
            model: Some("A4".to_string()),
            mileage: Some(55000),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            year: Some(2018),
            stat_column: Some("price_in_eur".to_string()),
            ..Default::default()
        };
        let stat_json = calculateStatistic(search);
        info!("Result: {:?}", stat_json);
        assert_eq!(stat_json.get("count").unwrap(), &json!(18));
        assert_eq!(stat_json.get("rsd").unwrap(), &json!(18));
        assert_eq!(stat_json.get("mean").unwrap(), &json!(23115));
        assert_eq!(stat_json.get("median").unwrap(), &json!(22800));
        assert_eq!(stat_json.get("estimation").unwrap(), &json!(24658));
        assert_eq!(stat_json.get("quantile_80").unwrap(), &json!(26890));
    }
}
