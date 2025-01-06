use std::{collections::HashMap, fmt::Debug, vec};

use chrono::NaiveDate;
use log::info;
use polars::{
    chunked_array::ops::SortMultipleOptions,
    datatypes::DataType,
    frame::DataFrame,
    lazy::dsl::{col, Expr},
    prelude::{Literal, PlSmallStr},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{model::AxumAPIModel::StatisticSearchPayload, HIDDEN_COLUMNS, VEHICLES_DATA};

use super::Utils::to_predicate;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PredicateFilter<T: ToOwned + ToString + Debug + Clone + Literal> {
    Like(HashMap<String, T>, bool),
    In(String, Vec<T>),
    Eq(HashMap<String, T>, bool),
    Gt(HashMap<String, T>, bool),
    Lt(HashMap<String, T>, bool),
    Gte(HashMap<String, T>, bool),
    Lte(HashMap<String, T>, bool),
}

#[derive(Debug, Serialize, Deserialize)]
struct Agreggator {
    source: String,
    year: String,
    price: i64,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct FinalJson<X, Y> {
    name: String,
    axis: Vec<X>,
    data: HashMap<String, Vec<Y>>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FilterPayload {
    pub source: Option<String>,
    pub group_by: Vec<String>,
    pub aggregate: Option<HashMap<String, Vec<GroupFunc>>>,
    pub search: Option<String>,
    pub filter_string: Vec<PredicateFilter<String>>,
    pub filter_i32: Vec<PredicateFilter<i32>>,
    pub filter_f64: Vec<PredicateFilter<f64>>,
    pub filter_date: Vec<PredicateFilter<NaiveDate>>,
    pub sort: Vec<SortBy>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SortBy {
    #[serde(rename = "asc")]
    Ascending(String, bool),
    #[serde(rename = "desc")]
    Descending(String, bool),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GroupFunc {
    #[serde(rename = "min")]
    Min,
    #[serde(rename = "max")]
    Max,
    #[serde(rename = "sum")]
    Sum,
    #[serde(rename = "median")]
    Median,
    #[serde(rename = "mean")]
    Mean,
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "quantile")]
    Quantile(f64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PriceFilter {
    source: Option<Vec<String>>,
    make: Option<Vec<String>>,
    model: Option<Vec<String>>,
    model_like: Option<String>,
    engine: Option<Vec<String>>,
    gearbox: Option<Vec<String>>,

    year_gt: Option<i32>,
    year_lt: Option<i32>,
    year_eq: Option<i32>,
    year_gte: Option<i32>,
    year_lte: Option<i32>,

    price_eq: Option<i32>,
    price_lt: Option<i32>,
    price_gt: Option<i32>,
    price_lte: Option<i32>,
    price_gte: Option<i32>,

    estimated_price_eq: Option<i32>,
    estimated_price_lt: Option<i32>,
    estimated_price_lte: Option<i32>,
    estimated_price_gt: Option<i32>,
    estimated_price_gte: Option<i32>,

    mileage_eq: Option<i32>,
    mileage_lt: Option<i32>,
    mileage_gt: Option<i32>,
    mileage_lte: Option<i32>,
    mileage_gte: Option<i32>,

    power_eq: Option<i32>,
    power_lt: Option<i32>,
    power_gt: Option<i32>,
    power_lte: Option<i32>,
    power_gte: Option<i32>,

    cc_eq: Option<i32>,
    cc_lt: Option<i32>,
    cc_gt: Option<i32>,
    cc_lte: Option<i32>,
    cc_gte: Option<i32>,

    created_on_before: Option<String>,
    created_on_after: Option<String>,
    created_on_eq: Option<String>,

    last_updated_on_before: Option<String>,
    last_updated_on_after: Option<String>,
    last_updated_on_eq: Option<String>,
}

pub fn group_by(aggregator: HashMap<String, Vec<GroupFunc>>) -> impl AsRef<[Expr]> {
    let mut agg_exprs = vec![];
    for (c, funcs) in aggregator.iter() {
        for f in funcs.iter() {
            let agg_expr = match f {
                GroupFunc::Min => col(c).min().alias("Min"),
                GroupFunc::Max => col(c).max().alias("Max"),
                GroupFunc::Sum => col(c).sum().alias("Sum"),
                GroupFunc::Median => col(c).median().alias("Median"),
                GroupFunc::Mean => col(c).mean().alias("Avg"),
                GroupFunc::Count => col(c).count().alias("Count"),
                GroupFunc::Quantile(p) => col(c)
                    .quantile((*p).into(), polars::prelude::QuantileMethod::Nearest)
                    .alias(format!("Q_{}", p)),
            };
            agg_exprs.push(agg_expr);
        }
    }
    agg_exprs
}

pub fn sort(df: polars::prelude::LazyFrame, sort: Vec<SortBy>) -> polars::prelude::LazyFrame {
    let mut columns: Vec<PlSmallStr> = Vec::new();
    let mut orders = Vec::new();

    for sort in sort.iter() {
        match sort {
            SortBy::Ascending(col, _) => {
                columns.push(col.into());
                orders.push(false);
            }
            SortBy::Descending(col, _) => {
                columns.push(col.into());
                orders.push(true);
            }
        }
    }

    df.sort(
        columns,
        SortMultipleOptions::new().with_order_descending(false),
    )
}

pub fn to_generic_json(data: &DataFrame) -> HashMap<String, Value> {
    let mut json = HashMap::new();
    let column_values = data.get_columns();
    json.insert("itemsCount".to_owned(), data.height().into());
    info!("Found results: {}", data.height());
    info!("Column count: {}", column_values.len());
    let mut metadata = HashMap::new();

    let mut meta = vec![];
    for (idx, cv) in column_values.iter().enumerate() {
        let name = cv.name().to_string();
        metadata.insert("column_name".to_owned(), json!(name));
        metadata.insert("column_index".to_owned(), json!(idx));
        metadata.insert("column_dtype".to_owned(), json!(cv.dtype().to_string()));
        if HIDDEN_COLUMNS.iter().any(|c| c == &name) {
            metadata.insert("visible".to_owned(), json!(false));
        } else {
            metadata.insert("visible".to_owned(), json!(true));
        }

        meta.push(metadata.clone());

        let values = if cv.dtype() == &DataType::Int32 {
            let values = cv.i32().unwrap().to_vec();
            json!(values)
        } else if cv.dtype() == &DataType::Int64 {
            let values = cv.i64().unwrap().to_vec();
            json!(values)
        } else if cv.dtype() == &DataType::UInt32 {
            let values = cv.u32().unwrap().to_vec();
            json!(values)
        } else if cv.dtype() == &DataType::UInt64 {
            let values = cv.u64().unwrap().to_vec();
            json!(values)
        } else if cv.dtype() == &DataType::Float32 {
            let values = cv.f32().unwrap().to_vec();
            json!(values)
        } else if cv.dtype() == &DataType::Float64 {
            let values = cv.f64().unwrap().to_vec();
            json!(values)
        } else {
            let values = cv
                .str()
                .unwrap()
                .iter()
                .map(|v| json!(v.unwrap_or_default()))
                .collect::<Vec<_>>();
            json!(values)
        };
        json.insert(cv.name().to_string(), values);
    }
    json.insert("metadata".to_owned(), json!(meta));
    json
}

pub fn search(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df = VEHICLES_DATA.clone();

    // Group by the required columns and calculate the required statistics

    let filterConditions = to_predicate(search.clone());

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
            col("save_diff_in_eur"),
            col("extra_charge_in_eur"),
            col("equipment"),
            col("url"),
            col("created_on"),
            col("updated_on"),
        ])
        .filter(filterConditions)
        .limit(100);
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

#[cfg(test)]
mod tests {

    use polars::{
        chunked_array::ops::SortMultipleOptions, datatypes::DataType, lazy::frame::IntoLazy,
        prelude::lit,
    };

    use crate::{services::Utils::to_like_predicate, VEHICLES_DATA};

    use super::*;

    #[test]
    fn test_to_like_predicate() {
        let mut filter = HashMap::new();
        filter.insert("model".to_string(), "Toyota".to_string());
        filter.insert("make".to_string(), "Corolla".to_string());
        let predicate = to_like_predicate(filter, true);
        assert!(predicate.is_some());
    }

    #[test]
    fn test_unique() {
        let column_name = "power_ps";
        let mut eq_filter = HashMap::new();
        eq_filter.insert(column_name.to_string(), "Volvo");

        // let predicate1 = to_predicate(make_filter).unwrap();
        let columns = vec!["key".to_string(), "value".to_string()];
        let unique = &VEHICLES_DATA
            .clone()
            .lazy()
            .select([
                col(column_name).alias("value"),
                col(column_name)
                    .cast(DataType::String)
                    .str()
                    .to_uppercase()
                    .str()
                    .replace_all(lit(" "), lit(""), false)
                    .alias("key"),
            ])
            // .filter(predicate1)
            .unique(Some(columns), polars::frame::UniqueKeepStrategy::Any)
            .sort(
                vec!["value"],
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![true],
                    ..Default::default()
                },
            )
            .collect()
            .unwrap();
        let series = unique.get_columns();
        let binding = series[1].clone();
        let models = binding.str().unwrap();
        info!("Found {:?}", models.len());
        for m in models {
            info!("{:?}", m);
        }
    }
}
