use std::{collections::HashMap, fmt::Debug};

use polars::{
    lazy::dsl::{col, lit, Expr},
    prelude::Literal,
};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PredicateFilter<T: ToOwned + ToString + Debug + Clone + Literal> {
    Like(HashMap<String, T>, bool, bool),
    In(String, Vec<T>),
    Eq(HashMap<String, T>, bool),
    Gt(HashMap<String, T>, bool),
    Lt(HashMap<String, T>, bool),
    Gte(HashMap<String, T>, bool),
    Lte(HashMap<String, T>, bool),
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FilterPayload {
    pub aggregate: Option<HashMap<String, Vec<GroupFunc>>>,
    pub filter_string: Option<PredicateFilter<String>>,
    pub filter_i32: Option<PredicateFilter<i32>>,
    pub filter_f64: Option<PredicateFilter<f64>>,
    pub sort: Vec<SortBy>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SortBy {
    #[serde(rename = "asc")]
    Ascending(String, bool, bool),
    #[serde(rename = "desc")]
    Descending(String, bool, bool),
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
    strict: bool,
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
        PredicateFilter::Like(filter, strict, join_and) => {
            to_like_predicate(filter, strict, join_and)
        }
        PredicateFilter::In(column, values) => to_in_predicate(&column, values),
        PredicateFilter::Eq(key_values, join_and) => to_eq_predicate(key_values, join_and),
        _ => None,
    }
}

fn to_eq_predicate<T: Debug + ToOwned + ToString + Clone + Literal>(
    key_values: HashMap<String, T>,
    join_and: bool,
) -> Option<polars::lazy::dsl::Expr> {
    if key_values.is_empty() {
        return None;
    }
    let mut predicates = vec![];
    for (c, v) in key_values.iter() {
        println!("column {}: value {}", c, v.to_string());
        let p = col(c).eq(lit(v.clone()));
        predicates.push(p);
    }
    println!("predicates {:?}", predicates);
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
                GroupFunc::Min => col(c).min().alias(format!("{}_min", c).as_str()),
                GroupFunc::Max => col(c).max().alias(format!("{}_max", c).as_str()),
                GroupFunc::Sum => col(c).sum().alias(format!("{}_sum", c).as_str()),
                GroupFunc::Median => col(c).median().alias(format!("{}_median", c).as_str()),
                GroupFunc::Mean => col(c).mean().alias(format!("{}_mean", c).as_str()),
                GroupFunc::Count => col(c).count().alias(format!("{}_count", c).as_str()),
                GroupFunc::Quantile(p) => col(c)
                    .quantile(
                        (*p).into(),
                        polars::prelude::QuantileInterpolOptions::Nearest,
                    )
                    .alias(format!("{}_quantile_{}", c, p).as_str()),
            };
            agg_exprs.push(agg_expr);
        }
    }
    agg_exprs
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use plotpy::{generate3d, Contour, Curve, Histogram, Plot, RayEndpoint, Surface};
    use polars::lazy::frame::IntoLazy;

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
        println!("{:?}", df);
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
        let model_like_filter = PredicateFilter::Like(model_filter, true, true);
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
        println!("{:?}", df);
    }

    #[test]
    fn test_to_like_predicate() {
        let mut filter = HashMap::new();
        filter.insert("model".to_string(), "Toyota".to_string());
        filter.insert("make".to_string(), "Corolla".to_string());
        let predicate = to_like_predicate(filter, true, true);
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
}
