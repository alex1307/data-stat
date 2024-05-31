use std::{collections::HashMap, fmt::Debug, vec};

use log::info;
use polars::{
    chunked_array::ops::SortMultipleOptions,
    datatypes::DataType,
    frame::DataFrame,
    lazy::{
        dsl::{col, lit, Expr},
        frame::LazyFrame,
    },
    prelude::Literal,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

enum Compare {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
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
    pub group_by: Vec<String>,
    pub aggregate: Option<HashMap<String, Vec<GroupFunc>>>,
    pub filter_string: Vec<PredicateFilter<String>>,
    pub filter_i32: Vec<PredicateFilter<i32>>,
    pub filter_f64: Vec<PredicateFilter<f64>>,
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

fn to_like_predicate<T: ToString + ToOwned + Debug + Literal>(
    filter: HashMap<String, T>,
    join_and: bool,
) -> Option<polars::lazy::dsl::Expr> {
    let mut predicates = vec![];
    for (c, v) in filter.iter() {
        let p = if v.to_string().starts_with("*") {
            col(c).str().ends_with(lit(v.to_string().replace('*', "")))
        } else if v.to_string().ends_with("*") {
            col(c)
                .str()
                .starts_with(lit(v.to_string().replace('*', "")))
        } else {
            col(c).str().contains(lit(v.to_string()), false)
        };

        predicates.push(p);
    }
    if predicates.is_empty() {
        return None;
    }
    if predicates.len() == 1 {
        return Some(predicates[0].clone());
    }
    let mut predicate = predicates[0].clone();
    for p in predicates.iter().skip(1) {
        if join_and {
            predicate = predicate.and(p.clone());
        } else {
            predicate = predicate.or(p.clone());
        }
    }
    Some(predicate)
}

fn to_in_predicate<T: ToOwned + ToString + Debug + Clone + Literal>(
    column: &str,
    values: Vec<T>,
) -> Option<polars::lazy::dsl::Expr> {
    if values.is_empty() || column.is_empty() {
        return None;
    }
    let mut predicates = vec![];
    for v in values.iter() {
        let p = col(column).eq(lit(v.clone()));
        predicates.push(p);
    }
    let mut predicate = predicates[0].clone();
    for p in predicates.iter().skip(1) {
        predicate = predicate.or(p.clone());
    }
    Some(predicate)
}

pub fn to_predicate<T: ToOwned + ToString + Debug + Clone + Literal>(
    filter: PredicateFilter<T>,
) -> Option<polars::lazy::dsl::Expr> {
    match filter {
        PredicateFilter::Like(filter, join_and) => to_like_predicate(filter, join_and),
        PredicateFilter::In(column, values) => to_in_predicate(&column, values),
        PredicateFilter::Eq(key_values, join_and) => {
            to_compare_predicate(key_values, Compare::Eq, join_and)
        }
        PredicateFilter::Gt(key_values, join_and) => {
            to_compare_predicate(key_values, Compare::Gt, join_and)
        }
        PredicateFilter::Lt(key_values, join_and) => {
            to_compare_predicate(key_values, Compare::Lt, join_and)
        }
        PredicateFilter::Gte(key_values, join_and) => {
            to_compare_predicate(key_values, Compare::Gte, join_and)
        }
        PredicateFilter::Lte(key_values, join_and) => {
            to_compare_predicate(key_values, Compare::Lte, join_and)
        }
    }
}

fn to_compare_predicate<T: Debug + ToOwned + ToString + Clone + Literal>(
    key_values: HashMap<String, T>,
    compare: Compare,
    join_and: bool,
) -> Option<polars::lazy::dsl::Expr> {
    if key_values.is_empty() {
        return None;
    }
    let mut predicates = vec![];
    for (c, v) in key_values.iter() {
        info!("column {}: value {}", c, v.to_string());
        let predicate = match compare {
            Compare::Eq => col(c).eq(lit(v.clone())),
            Compare::Gt => col(c).gt(lit(v.clone())),
            Compare::Lt => col(c).lt(lit(v.clone())),
            Compare::Gte => col(c).gt_eq(lit(v.clone())),
            Compare::Lte => col(c).lt_eq(lit(v.clone())),
        };
        predicates.push(predicate);
    }
    info!("predicates {:?}", predicates);
    let mut predicate = predicates[0].clone();
    for p in predicates.iter().skip(1) {
        if join_and {
            predicate = predicate.and(p.clone());
        } else {
            predicate = predicate.or(p.clone());
        }
    }
    Some(predicate)
}

pub fn group_by(aggregator: HashMap<String, Vec<GroupFunc>>) -> impl AsRef<[Expr]> {
    let mut agg_exprs = vec![];
    for (c, funcs) in aggregator.iter() {
        for f in funcs.iter() {
            let agg_expr = match f {
                GroupFunc::Min => col(c).min().alias(&format!("{}_min", c)),
                GroupFunc::Max => col(c).max().alias(&format!("{}_max", c)),
                GroupFunc::Sum => col(c).sum().alias(&format!("{}_sum", c)),
                GroupFunc::Median => col(c).median().alias(&format!("{}_median", c)),
                GroupFunc::Mean => col(c).mean().alias(&format!("{}_mean", c)),
                GroupFunc::Count => col(c).count().alias(&format!("{}_count", c)),
                GroupFunc::Quantile(p) => col(c)
                    .quantile(
                        (*p).into(),
                        polars::prelude::QuantileInterpolOptions::Nearest,
                    )
                    .alias(&format!("{}_quantile_{}", c, p)),
            };
            agg_exprs.push(agg_expr);
        }
    }
    agg_exprs
}

pub fn sort(df: polars::prelude::LazyFrame, sort: Vec<SortBy>) -> polars::prelude::LazyFrame {
    let mut columns = Vec::new();
    let mut orders = Vec::new();

    for sort in sort.iter() {
        match sort {
            SortBy::Ascending(col, _) => {
                columns.push(col);
                orders.push(false);
            }
            SortBy::Descending(col, _) => {
                columns.push(col);
                orders.push(true);
            }
        }
    }

    df.sort(
        &columns,
        SortMultipleOptions::new().with_order_descendings(orders),
    )
}

pub fn apply_filter(
    df: &polars::prelude::LazyFrame,
    filter: FilterPayload,
) -> polars::prelude::DataFrame {
    let mut results: LazyFrame = df.clone();
    let mut predicates = vec![];
    if !filter.filter_string.is_empty() {
        for f in filter.filter_string.iter() {
            predicates.push(to_predicate(f.clone()).unwrap());
        }
    }
    if !filter.filter_i32.is_empty() {
        for f in filter.filter_i32.iter() {
            predicates.push(to_predicate(f.clone()).unwrap());
        }
    }
    if !filter.filter_f64.is_empty() {
        for f in filter.filter_f64.iter() {
            predicates.push(to_predicate(f.clone()).unwrap());
        }
    }
    if !predicates.is_empty() {
        if predicates.len() == 1 {
            results = results.filter(predicates[0].clone());
        } else {
            let mut predicate = predicates[0].clone();
            for p in predicates.iter().skip(1) {
                predicate = predicate.and(p.clone());
            }
            results = results.filter(predicate);
        }
    }

    if let Some(agg) = filter.aggregate {
        let agg_exprs = group_by(agg.clone());
        if !filter.group_by.is_empty() {
            let by = filter.group_by.iter().map(|k| col(k)).collect::<Vec<_>>();
            info!("Group by columns: {:?}", by);
            results = results.group_by(by).agg(agg_exprs);
        } else {
            info!("Group by columns not provided");
        }
    } else {
        info!("Aggregator not provided");
    }
    if !filter.sort.is_empty() {
        results = sort(results, filter.sort);
    }

    results.collect().unwrap()
}

pub fn to_generic_json(data: &DataFrame) -> HashMap<String, Value> {
    let mut json = HashMap::new();
    let column_values = data.get_columns();
    json.insert("count".to_owned(), data.height().into());
    info!("Column count: {}", column_values.len());
    let mut metadata = HashMap::new();
    let mut idx = 0;
    let mut meta = vec![];
    for cv in column_values {
        metadata.insert("column_name".to_owned(), json!(cv.name()));
        metadata.insert("column_index".to_owned(), json!(idx));
        metadata.insert("column_dtype".to_owned(), json!(cv.dtype().to_string()));
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
        } else if cv.dtype() == &DataType::Date {
            let values = cv
                .cast(&DataType::String)
                .unwrap()
                .iter()
                .map(|v| json!(v.get_str().unwrap_or_default()))
                .collect::<Vec<_>>();
            json!(values)
        } else {
            let values = cv
                .iter()
                .map(|v| json!(v.get_str().unwrap_or_default()))
                .collect::<Vec<_>>();
            json!(values)
        };
        json.insert(cv.name().to_owned(), values);
        idx += 1;
    }
    json.insert("metadata".to_owned(), json!(meta));
    json
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use plotpy::{generate3d, Contour, Curve, Histogram, Plot, RayEndpoint, Surface};
    use polars::{
        chunked_array::ops::SortMultipleOptions, datatypes::DataType, lazy::frame::IntoLazy,
    };

    use crate::PRICE_DATA;

    use super::*;

    #[test]
    fn test_to_eq_predicate() {
        let mut eq_filter = HashMap::new();
        eq_filter.insert("source".to_string(), "autouncle.fr");
        eq_filter.insert("make".to_string(), "Mercedes-Benz");
        let mut year_filter = HashMap::new();
        year_filter.insert("year".to_string(), 2020);
        let make_filter = PredicateFilter::Eq(eq_filter, true);
        let year_eq_filter = PredicateFilter::Eq(year_filter, true);
        let predicate1 = to_predicate(make_filter).unwrap();
        let predicate2 = to_predicate(year_eq_filter).unwrap();
        let predicate = predicate1.and(predicate2);
        let df = PRICE_DATA
            .clone()
            .lazy()
            .filter(predicate)
            .collect()
            .unwrap();
        info!("{:?}", df);
    }
    #[test]
    fn test_trim_and_upper_model() {
        let mut model_filter = HashMap::new();
        model_filter.insert("model".to_string(), "*AMG".to_owned());
        let mut eq_filter = HashMap::new();
        eq_filter.insert("source".to_string(), "autouncle.fr");
        eq_filter.insert("make".to_string(), "Mercedes-Benz");
        let mut year_filter = HashMap::new();
        year_filter.insert("year".to_string(), 2020);
        let make_filter = PredicateFilter::Eq(eq_filter, true);
        let year_eq_filter = PredicateFilter::Eq(year_filter, true);
        let model_like_filter = PredicateFilter::Like(model_filter, true);
        let predicate1 = to_predicate(make_filter).unwrap();
        let predicate2 = to_predicate(year_eq_filter).unwrap();
        let predicate3 = to_predicate(model_like_filter).unwrap();
        let predicate = predicate1.and(predicate2).and(predicate3);
        let df = PRICE_DATA
            .clone()
            .lazy()
            .select([
                col("source"),
                col("make"),
                col("model")
                    .str()
                    .to_uppercase()
                    .str()
                    .replace_all(lit(" "), lit(""), false)
                    .alias("model"),
                col("year"),
                col("price"),
            ])
            .filter(predicate)
            .collect()
            .unwrap();
        info!("{:?}", df);
    }

    #[test]
    fn test_to_like_predicate() {
        let mut filter = HashMap::new();
        filter.insert("model".to_string(), "Toyota".to_string());
        filter.insert("make".to_string(), "Corolla".to_string());
        let predicate = to_like_predicate(filter, true);
        assert!(predicate.is_some());
    }

    #[test]
    fn draw_countour() {
        let n = 21;
        let (x, y, z) = generate3d(-2.0, 2.0, -2.0, 2.0, n, n, |x, y| x * x - y * y);

        // configure contour
        let mut contour = Contour::new();
        contour
            .set_colorbar_label("temperature")
            .set_colormap_name("terrain")
            .set_selected_level(0.0, true);

        // draw contour
        contour.draw(&x, &y, &z);

        // add contour to plot
        let mut plot = Plot::new();
        plot.add(&contour);
        plot.set_labels("x", "y");

        // save figure
        plot.save("resources/contour.jpg").unwrap();
    }
    #[test]
    fn draw_sample() {
        let r = &[1.0, 1.0, 1.0];
        let c = &[-1.0, -1.0, -1.0];
        let k = &[0.5, 0.5, 0.5];
        let mut star = Surface::new();
        star.set_colormap_name("jet")
            .draw_superquadric(c, r, k, -180.0, 180.0, -90.0, 90.0, 40, 20)
            .unwrap();

        // pyramids
        let c = &[1.0, -1.0, -1.0];
        let k = &[1.0, 1.0, 1.0];
        let mut pyramids = Surface::new();
        pyramids
            .set_colormap_name("inferno")
            .draw_superquadric(c, r, k, -180.0, 180.0, -90.0, 90.0, 40, 20)
            .unwrap();

        // rounded cube
        let c = &[-1.0, 1.0, 1.0];
        let k = &[4.0, 4.0, 4.0];
        let mut cube = Surface::new();
        cube.set_surf_color("#ee29f2")
            .draw_superquadric(c, r, k, -180.0, 180.0, -90.0, 90.0, 40, 20)
            .unwrap();

        // sphere
        let c = &[0.0, 0.0, 0.0];
        let k = &[2.0, 2.0, 2.0];
        let mut sphere = Surface::new();
        sphere
            .set_colormap_name("rainbow")
            .draw_superquadric(c, r, k, -180.0, 180.0, -90.0, 90.0, 40, 20)
            .unwrap();

        // sphere (direct)
        let mut sphere_direct = Surface::new();
        sphere_direct
            .draw_sphere(&[1.0, 1.0, 1.0], 1.0, 40, 20)
            .unwrap();

        // add features to plot
        let mut plot = Plot::new();
        plot.add(&star)
            .add(&pyramids)
            .add(&cube)
            .add(&sphere)
            .add(&sphere_direct);

        // save figure
        plot.set_equal_axes(true)
            .set_figure_size_points(600.0, 600.0)
            .save("resources/superquadric.svg")
            .unwrap();
        fs::remove_file("resources/superquadric.py").unwrap();
    }
    #[test]
    fn test_histogram() {
        let mut histogram = Histogram::new();
        histogram
            .set_colors(&vec!["#cd0000", "#1862ab", "#cd8c00"])
            .set_style("barstacked")
            .set_number_bins(2)
            .set_stacked(true);

        // draw histogram
        let values = vec![
            vec![1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 4, 5, 6],
            vec![-1],
            vec![0], // first series                                              // third series
        ];
        let labels = ["first", "second", "third"];
        histogram.draw(&values, &labels);

        // add histogram to plot
        let mut plot = Plot::new();
        plot.set_label_x("X")
            .set_label_y("Y")
            .set_range(-10.0, 10.0, 0.0, 15.0)
            .set_title("Histogram");

        plot.add(&histogram);

        // save figure
        let path = Path::new("resources").join("histogram.jpg");
        plot.save(&path).unwrap();
        fs::remove_file("resources/histogram.py").unwrap_or_default();
    }

    #[test]
    fn distribution_plot() {
        let mut curve1 = Curve::new();
        curve1
            .set_line_alpha(0.7)
            .set_line_color("#cd0000")
            .set_line_style("--")
            .set_line_width(2.0)
            .set_marker_color("#1862ab")
            .set_marker_every(2)
            .set_marker_void(false)
            .set_marker_line_color("#cda500")
            .set_marker_line_width(3.0)
            .set_marker_size(8.0)
            .set_marker_style("p");

        // another curve
        let mut curve2 = Curve::new();
        curve2
            .set_line_style("None")
            .set_marker_line_color("#1862ab")
            .set_marker_style("8")
            .set_marker_void(true);

        // draw curves
        let x = &[1.0, 2.0, 3.0];
        let y = &[1.0, 1.41421356, 1.73205081];
        let y2 = &[1.0, 1.5, 2.0];
        curve1.draw(x, y);
        curve2.draw(x, y2);

        // draw ray
        let mut ray1 = Curve::new();
        let mut ray2 = Curve::new();
        let mut ray3 = Curve::new();
        let mut ray4 = Curve::new();
        ray1.set_line_color("orange");
        ray2.set_line_color("gold");
        ray3.set_line_color("yellow");
        ray4.set_line_color("#9b7014");
        ray1.draw_ray(2.0, 0.0, RayEndpoint::Coords(8.0, 0.5));
        ray2.draw_ray(2.0, 0.0, RayEndpoint::Slope(0.2));
        ray3.draw_ray(2.0, 0.0, RayEndpoint::Horizontal);
        ray4.draw_ray(2.0, 0.0, RayEndpoint::Vertical);

        // add curves to plot
        let mut plot = Plot::new();
        plot.add(&curve1)
            .add(&curve2)
            .add(&ray1)
            .add(&ray2)
            .add(&ray3)
            .add(&ray4);

        // save figure

        plot.save("resources/curve.jpg").unwrap();
        fs::remove_file("resources/curve.py").unwrap_or_default();
    }

    #[test]
    fn test_from_json() {
        let json = r#"{
            "group_by": ["source", "year"],
            "aggregate": {
                "price": ["max", "count", {"quantile":0.25}]
            },
            "sort": [{"asc": ["source", true]}, {"asc": ["year", true]}],
            "filter_string": [
                {"Like":[{"make":"Mercedes-Benz"}, true]}
            ],
            "filter_i32": [
                {"Gte":[{"mileage":10000, "price": 10000}, true]},
                {"Lte":[{"mileage":60000, "price": 50000}, true]},
                {"In":["year",[2016, 2017, 2018]]}
            ],
            "filter_f64": []
        }"#;
        let payload: FilterPayload = serde_json::from_str(json).unwrap();
        let df = apply_filter(&PRICE_DATA, payload);
        let row_count = df.height();
        info!("Row count: {}", row_count);

        let x = df.get_column_names();
        let y = df.get_columns();

        let mut axis = vec![];

        for i in 0..row_count {
            let first = y[0].get(i).unwrap().to_string();
            let second = y[1].get(i).unwrap().to_string();
            axis.push((i, first, second));
        }
        let mut values_i32 = HashMap::new();
        let mut values_f64 = HashMap::new();

        for i in 2..y.len() {
            let dtype = y[i].dtype();

            if dtype == &DataType::Int32
                || dtype == &DataType::Int64
                || dtype == &DataType::UInt32
                || dtype == &DataType::UInt64
            {
                let v = y[i].cast(&DataType::Int64).unwrap();
                let slice: Vec<Option<i64>> = v.i64().unwrap().to_vec();
                let mut data = vec![];
                for i in 0..row_count {
                    let (idx, src, year) = axis[i].clone();
                    data.push((idx, src, year, slice[i].unwrap()));
                }
                values_i32.insert(v.name().to_owned(), data.clone());
            } else if dtype == &DataType::Float32 || dtype == &DataType::Float64 {
                let v = y[i].cast(&DataType::Float64).unwrap();
                let slice: Vec<Option<f64>> = v.f64().unwrap().to_vec();
                let mut data = vec![];
                for i in 0..row_count {
                    let (idx, src, year) = axis[i].clone();
                    data.push((idx, src, year, slice[i].unwrap()));
                }
                values_f64.insert(v.name().to_owned(), data.clone());
            } else {
                // let v = y[i].cast(&DataType::String).unwrap();
                // let slice: Vec<Option<String>> = v
                //     .str()
                //     .unwrap()
                //     .into_iter()
                //     .map(|s| match s {
                //         Some(s) => Some(s.to_string()),
                //         None => None,
                //     })
                //     .collect();
                // data_str.insert(v.name().to_owned(), slice.clone());
                // slice
                continue;
            }
        }
        for (k, values) in values_i32.into_iter() {
            info!("{:?}", k);

            let mut final_json: FinalJson<String, i64> = FinalJson {
                name: k.clone(),
                axis: Vec::new(),
                data: HashMap::new(),
            };

            for (idx, src, year, value) in values {
                final_json.axis.push(year.clone());
                final_json
                    .data
                    .entry(src.clone())
                    .or_insert_with(Vec::new)
                    .push(value);

                info!("{} {} {} {} {}", k, idx, src, year, value);
            }
            final_json.axis.sort();
            final_json.axis.dedup();

            // Serialize to JSON
            let json = serde_json::to_string(&final_json).unwrap();
            info!("{}", json);
            info!("----------------------------------------------------------");
        }
    }

    #[test]
    fn test_to_in_predicate() {
        let json = r#"{
            "group_by": ["source", "year"],
            "aggregate": {
                "price": ["max", "count", {"quantile":0.25}]
            },
            "sort": [{"asc": ["source", true]}, {"asc": ["year", true]}],
            "filter_string": {
                "Like":[{"make":"Mercedes-Benz"}, true, true]
            },
            "filter_i32": {
                "Eq":[{"year":2014}, true]
            }
        }"#;
        let json = r#"{
            "group_by": [
                "source",
                "year"
            ],
            "aggregate": {
                "price": [
                    "max",
                    "count",
                    {
                        "quantile": 0.25
                    }
                ]
            },
            "sort": [
                {
                    "asc": [
                        "year",
                        true
                    ]
                },
                {
                    "asc": [
                        "source",
                        true
                    ]
                }
            ],
            "filter_string": [
                {
                    "Like": [
                        {
                            "make": "BMW"
                        },
                        true,
                        true
                    ]
                },
                {
                    "In": [
                        "engine",
                        [
                            "Diesel",
                            "Petrol"
                        ]
                    ]
                }
            ],
            "filter_i32": [
                {
                    "In": [
                        "year",
                        [
                            2017,
                            2018,
                            2019
                        ]
                    ]
                },
                {
                    "Gte": [
                        {
                            "power_ps": 150
                        },
                        true
                    ]
                },
                {
                    "Lte": [
                        {
                            "mileage": 100000
                        },
                        true
                    ]
                }
            ],
            "filter_f64": []
        }"#;
        let payload: FilterPayload = serde_json::from_str(json).unwrap();
        let df = apply_filter(&PRICE_DATA, payload);
    }

    #[test]
    fn test_unique() {
        let column_name = "power_ps";
        let mut eq_filter = HashMap::new();
        eq_filter.insert(column_name.to_string(), "Volvo");
        let make_filter = PredicateFilter::Eq(eq_filter, true);
        // let predicate1 = to_predicate(make_filter).unwrap();
        let columns = vec!["key".to_string(), "value".to_string()];
        let unique = &PRICE_DATA
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
                    nulls_last: true,
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
