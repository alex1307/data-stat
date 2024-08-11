use std::collections::{BTreeMap, HashMap};

use chrono::{Duration, Utc};
use lazy_static::lazy_static;
use log::info;
use polars::{
    chunked_array::ops::SortMultipleOptions,
    lazy::{
        dsl::{col, lit},
        frame::{IntoLazy, LazyFrame},
    },
};

lazy_static! {
    pub static ref MILEAGE_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
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
    pub static ref MILEAGE_STATUS_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
        map.insert("1".to_string(), "Less than 20 000 km".to_string());
        map.insert("2".to_string(), "Betweeen 20 000 km. and 40 000 km.".to_string());
        map.insert("3".to_string(), "Betweeen 40 000 km. and 60 000 km.".to_string());
        map.insert("4".to_string(), "Betweeen 60 000 km. and 80 000 km.".to_string());
        map.insert("5".to_string(), "Betweeen 80 000 km. and 100 000 km.".to_string());
        map.insert("6".to_string(), "Betweeen 100 000 km. and 120 000 km.".to_string());
        map.insert("7".to_string(), "Betweeen 120 000 km. and 150 000 km.".to_string());
        map.insert("10".to_string(), "Over 150 000 km.".to_string());
        map
    };
    pub static ref CC_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
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
    pub static ref POWER_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
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
    pub static ref PRICE_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
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
    pub static ref DISCOUNT_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
        map.insert("0".to_string(), "0 %".to_string());
        map.insert("5".to_string(), "5%".to_string());
        map.insert("10".to_string(), "10%".to_string());
        map.insert("15".to_string(), "15%".to_string());
        map.insert("25".to_string(), "25%".to_string());
        map.insert("40".to_string(), "40%".to_string());
        map
    };
    pub static ref SAVE_DIFF_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
        map.insert("0".to_string(), "Any".to_string());
        map.insert("1500".to_string(), "€ 1,500".to_string());
        map.insert("2500".to_string(), "€ 2,500".to_string());
        map.insert("4000".to_string(), "€ 4,000".to_string());
        map.insert("7500".to_string(), "€ 7,500".to_string());
        map.insert("10000".to_string(), "€ 10,000".to_string());
        map.insert("15000".to_string(), "€ 15,000".to_string());
        map.insert("20000".to_string(), "€ 20,000".to_string());
        map.insert("50000".to_string(), "€ 50,000".to_string());
        map
    };
    pub static ref PUBLISHED_ON_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
        map.insert("0".to_string(), "Any".to_string());
        let today = Utc::now().date_naive();
        let yesterday = today - Duration::days(1);
        let three_days_ago = today - Duration::days(3);
        let five_days_ago = today - Duration::days(5);
        let week_ago = today - Duration::weeks(1);
        let two_weeks_ago = today - Duration::weeks(2);
        let year_ago = today - Duration::days(365);

        let mut date_vec = vec![(today, "Today".to_string()),
            (yesterday, "Yesterday".to_string()),
            (three_days_ago, "3 days ago".to_string()),
            (five_days_ago, "5 days ago".to_string()),
            (week_ago, "1 week ago".to_string()),
            (two_weeks_ago, "2 weeks ago".to_string()),
            (year_ago, "More than 2 weeks....".to_string())];



        // Sort the vector in descending order based on the date
        date_vec.sort_by(|a, b| b.0.cmp(&a.0));

        let mut map = BTreeMap::new();
        map.insert("0".to_string(), "Any".to_string());

        for (date, label) in date_vec {
            map.insert(date.format("%Y-%m-%d").to_string(), label);
        }
        map
    };
    pub static ref YEAR_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
        map.insert("0".to_string(), "Any".to_string());
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
    pub static ref GROUP_BY_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
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
    pub static ref SORT_BY_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
        map.insert("".to_string(), " Order by:".to_string());
        map.insert("source".to_string(), "Source".to_string());
        map.insert("make".to_string(), "Make".to_string());
        map.insert("model".to_string(), "Model".to_string());
        map.insert("power_ps".to_string(), "Power".to_string());
        map.insert("price".to_string(), "Price".to_string());
        map.insert("save_diff".to_string(), "Save Difference".to_string());
        map.insert("discount".to_string(), "Discount".to_string());
        map.insert("year".to_string(), "Year".to_string());
        map.insert("created_on".to_string(), "Created On".to_string());
        map
    };
    pub static ref ASC_FILTER: BTreeMap<String, String> = {
        let mut map = BTreeMap::new();
        map.insert("asc".to_string(), "Ascending".to_string());
        map.insert("desc".to_string(), "Descending".to_string());
        map
    };
}

pub fn select(filter: &str, df: &LazyFrame) -> HashMap<String, String> {
    if filter.is_empty() {
        return HashMap::new();
    }
    let btreemap = match filter.to_lowercase().as_str() {
        "mileage" => MILEAGE_FILTER.clone(),
        "mileage_status" => MILEAGE_STATUS_FILTER.clone(),
        "power" => POWER_FILTER.clone(),
        "make" => unique_values(df, filter),
        "engine" => unique_values(df, filter),
        "gearbox" => unique_values(df, filter),
        "price" => PRICE_FILTER.clone(),
        "estimated_price" => PRICE_FILTER.clone(),
        "year" => YEAR_FILTER.clone(),
        "discount" => DISCOUNT_FILTER.clone(),
        "increase" => DISCOUNT_FILTER.clone(),
        "createdon" => PUBLISHED_ON_FILTER.clone(),
        "savediff" => SAVE_DIFF_FILTER.clone(),
        "overcharge" => SAVE_DIFF_FILTER.clone(),
        "cc" => CC_FILTER.clone(),
        "source" => unique_values(df, filter),
        "group_by" => GROUP_BY_FILTER.clone(),
        "sort_by" => SORT_BY_FILTER.clone(),
        "asc" => ASC_FILTER.clone(),
        _ => BTreeMap::new(),
    };
    if filter == "createdOn" {
        let mut sorted_vec: Vec<_> = btreemap.into_iter().collect();
        sorted_vec.sort_by(|a: &(String, String), b| b.0.cmp(&a.0));
        let mut sorted_map = HashMap::new();
        for (key, value) in sorted_vec {
            sorted_map.insert(key, value);
        }
        return sorted_map;
    }
    btreemap.into_iter().collect()
}

fn unique_values(df: &polars::prelude::LazyFrame, column_name: &str) -> BTreeMap<String, String> {
    let unique = &df
        .clone()
        .lazy()
        .select([col(column_name)])
        .unique(None, polars::frame::UniqueKeepStrategy::First)
        .sort(
            [column_name],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![true],
                ..Default::default()
            },
        )
        .collect()
        .unwrap();
    let series = unique.get_columns();
    info!("Unique values: {:?}", series[0].len());
    let mut map = BTreeMap::new();
    let keys = series[0].str().unwrap();

    info!("Unique values: {:?}", keys);

    for k in keys.into_iter().flatten() {
        if k.to_uppercase() == "NOTFOUND" {
            continue;
        }
        map.insert(k.to_string(), k.to_string());
    }

    map
}

pub fn models(make: &str, df: &LazyFrame) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();

    let df = df
        .clone()
        .lazy()
        .filter(col("make").eq(lit(make)))
        .select([col("model")])
        .unique(None, polars::frame::UniqueKeepStrategy::First)
        .sort(
            ["model"],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![true],
                ..Default::default()
            },
        )
        .collect()
        .unwrap();
    let series = df.get_columns();
    let keys = series[0].str().unwrap();
    for k in keys.into_iter().flatten() {
        map.insert(k.to_string(), k.to_string());
    }
    map
}
