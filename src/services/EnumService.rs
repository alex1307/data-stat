use std::collections::HashMap;

use lazy_static::lazy_static;
use polars::{
    chunked_array::ops::SortMultipleOptions,
    datatypes::DataType,
    lazy::{
        dsl::{col, lit},
        frame::IntoLazy,
    },
};

use crate::PRICE_DATA;

lazy_static! {
    pub static ref MILEAGE_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("0".to_string(), "Any".to_string());
        map.insert("5000".to_string(), "5,000km".to_string());
        map.insert("10000".to_string(), "10,000km".to_string());
        map.insert("20000".to_string(), "20,000km".to_string());
        map.insert("30000".to_string(), "30,000km".to_string());
        map.insert("40000".to_string(), "40,000km".to_string());
        map.insert("50000".to_string(), "50,000km".to_string());
        map.insert("60000".to_string(), "60,000 km".to_string());
        map.insert("70000".to_string(), "70,000 km".to_string());
        map.insert("80000".to_string(), "80,000 km".to_string());
        map.insert("90000".to_string(), "90,000 km".to_string());
        map.insert("100000".to_string().to_string(), "100,000km".to_string());
        map.insert("125000".to_string(), "125,000 km".to_string());
        map.insert("150000".to_string(), "150,000 km".to_string());
        map
    };
    pub static ref CC_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("0".to_string(), "Any".to_string());
        map.insert("900".to_string(), "900".to_string());
        map.insert("1000".to_string(), "1,000".to_string());
        map.insert("1200".to_string(), "1,200".to_string());
        map.insert("1500".to_string(), "1,500".to_string());
        map.insert("1600".to_string(), "1,600".to_string());
        map.insert("1800".to_string(), "1,800".to_string());
        map.insert("2000".to_string(), "2,000".to_string());
        map.insert("2200".to_string(), "2,200".to_string());
        map.insert("2500".to_string(), "2,500".to_string());
        map.insert("3000".to_string(), "3,000".to_string());
        map.insert("3500".to_string(), "3,500".to_string());
        map.insert("4000".to_string(), "4,000".to_string());
        map.insert("4500".to_string(), "4,500".to_string());
        map
    };
    pub static ref POWER_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("0".to_string(), "Any".to_string());
        map.insert("34".to_string(), "34".to_string());
        map.insert("50".to_string(), "50".to_string());
        map.insert("60".to_string(), "60".to_string());
        map.insert("75".to_string(), "75".to_string());
        map.insert("90".to_string(), "90".to_string());
        map.insert("101".to_string(), "101".to_string());
        map.insert("118".to_string(), "118".to_string());
        map.insert("131".to_string(), "131".to_string());
        map.insert("150".to_string(), "150".to_string());
        map.insert("200".to_string(), "200".to_string());
        map.insert("252".to_string(), "252".to_string());
        map.insert("303".to_string(), "303".to_string());
        map.insert("358".to_string(), "358".to_string());
        map.insert("402".to_string(), "402".to_string());
        map.insert("454".to_string(), "454".to_string());
        map
    };
    pub static ref MAKE_FILTER: HashMap<String, String> = unique_values(&PRICE_DATA, "make");
    pub static ref ENGINE_FILTER: HashMap<String, String> = unique_values(&PRICE_DATA, "engine");
    pub static ref GEARBOX_FILTER: HashMap<String, String> = unique_values(&PRICE_DATA, "gearbox");
    pub static ref PRICE_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("0".to_string(), "Any".to_string());
        map.insert("5000".to_string(), "5,000".to_string());
        map.insert("10000".to_string(), "10,000".to_string());
        map.insert("20000".to_string(), "20,000".to_string());
        map.insert("30000".to_string(), "30,000".to_string());
        map.insert("40000".to_string(), "40,000".to_string());
        map.insert("50000".to_string(), "50,000".to_string());
        map.insert("60000".to_string(), "60,000".to_string());
        map.insert("70000".to_string(), "70,000".to_string());
        map.insert("80000".to_string(), "80,000".to_string());
        map.insert("90000".to_string(), "90,000".to_string());
        map.insert("100000".to_string(), "100,000".to_string());
        map
    };
    pub static ref YEAR_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("2014".to_string(), "2014".to_string());
        map.insert("2015".to_string(), "2015".to_string());
        map.insert("2016".to_string(), "2016".to_string());
        map.insert("2017".to_string(), "2017".to_string());
        map.insert("2018".to_string(), "2018".to_string());
        map.insert("2019".to_string(), "2019".to_string());
        map.insert("2020".to_string(), "2020".to_string());
        map.insert("2021".to_string(), "2021".to_string());
        map.insert("2022".to_string(), "2022".to_string());
        map.insert("2023".to_string(), "2023".to_string());
        map.insert("2024".to_string(), "2024".to_string());
        map
    };
    pub static ref SOURCE_FILTER: HashMap<String, String> = unique_values(&PRICE_DATA, "source");
    pub static ref GROUP_BY_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("source".to_string(), "Source".to_string());
        map.insert("make".to_string(), "Make".to_string());
        map.insert("model".to_string(), "Model".to_string());
        map.insert("engine".to_string(), "Engine".to_string());
        map.insert("gearbox".to_string(), "Gearbox".to_string());
        map.insert("year".to_string(), "Year".to_string());
        map.insert("created_on".to_string(), "Created On".to_string());
        map.insert("last_updated_on".to_string(), "Last Updated On".to_string());
        map
    };
    pub static ref SORT_BY_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("".to_string(), " Order by:".to_string());
        map.insert("source".to_string(), "Source".to_string());
        map.insert("make".to_string(), "Make".to_string());
        map.insert("model".to_string(), "Model".to_string());
        map.insert("cc".to_string(), "cc".to_string());
        map.insert("power_ps".to_string(), "Power".to_string());
        map.insert("engine".to_string(), "Engine".to_string());
        map.insert("gearbox".to_string(), "Gearbox".to_string());
        map.insert("price".to_string(), "Price".to_string());
        map.insert(
            "estimatated_price".to_string(),
            "Estimated Price".to_string(),
        );
        map.insert("year".to_string(), "Year".to_string());
        map.insert("created_on".to_string(), "Created On".to_string());
        map.insert("last_updated_on".to_string(), "Last Updated On".to_string());
        map
    };
    pub static ref ASC_FILTER: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("asc".to_string(), "Ascending".to_string());
        map.insert("desc".to_string(), "Descending".to_string());
        map
    };
}

pub fn select(filter: &str) -> HashMap<String, String> {
    if filter.is_empty() {
        return HashMap::new();
    }
    match filter.to_lowercase().as_str() {
        "mileage" => MILEAGE_FILTER.clone(),
        "power" => POWER_FILTER.clone(),
        "make" => MAKE_FILTER.clone(),
        "engine" => ENGINE_FILTER.clone(),
        "gearbox" => GEARBOX_FILTER.clone(),
        "price" => PRICE_FILTER.clone(),
        "year" => YEAR_FILTER.clone(),
        "cc" => CC_FILTER.clone(),
        "source" => SOURCE_FILTER.clone(),
        "group_by" => GROUP_BY_FILTER.clone(),
        "sort_by" => SORT_BY_FILTER.clone(),
        "asc" => ASC_FILTER.clone(),
        _ => HashMap::new(),
    }
}

fn unique_values(df: &polars::prelude::LazyFrame, column_name: &str) -> HashMap<String, String> {
    let columns = vec!["key".to_string(), "value".to_string()];
    let unique = &df
        .clone()
        .lazy()
        .select([
            col(column_name)
                .cast(DataType::String)
                .str()
                .replace_all(lit(" "), lit(""), false)
                .str()
                .replace_all(lit("-"), lit(""), false)
                .alias("key"),
            col(column_name).alias("value"),
        ])
        .unique(Some(columns), polars::frame::UniqueKeepStrategy::Any)
        .sort(
            ["value"],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: true,
                ..Default::default()
            },
        )
        .collect()
        .unwrap();
    let series = unique.get_columns();
    let mut map = HashMap::new();
    let keys = series[0].str().unwrap();
    let values = series[1].str().unwrap();
    let count = keys.len();
    for i in 0..count {
        let key = keys.get(i).unwrap();
        let value = values.get(i).unwrap();
        if key == "NOTFOUND" {
            continue;
        }

        map.insert(key.to_string(), value.to_string());
    }
    if column_name == "make" {
        map.insert("".to_string(), " Select".to_string());
    }
    map
}
