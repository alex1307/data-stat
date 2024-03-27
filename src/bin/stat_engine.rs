use std::collections::HashMap;

use data_statistics::{
    services::PriceStatistic::{to_predicate, PredicateFilter},
    PRICE_DATA,
};
use polars::{
    chunked_array::ops::SortOptions,
    lazy::{dsl::col, frame::IntoLazy},
};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let df_csv = PRICE_DATA.clone();
    let filter = col("price").gt(1000).and(col("year").gt(2013));
    let df_csv = &df_csv
        .clone()
        .lazy()
        .filter(filter)
        .select(vec![
            col("source"),
            col("make"),
            col("model"),
            col("year"),
            col("price"),
        ])
        .collect()?;
    println!("{:?}", df_csv);

    let df = df_csv
        .clone()
        .lazy()
        .group_by(vec!["source", "year"])
        .agg(vec![
            col("price").min().alias("min_price"),
            col("price").max().alias("max_price"),
            col("price").sum().alias("sum_price"),
            col("price").median().alias("median_price"),
            col("price").mean().alias("mean_price"),
            col("price").count().alias("count"),
        ])
        .sort(
            "source",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .sort(
            "year",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .collect()?;
    println!("{:?}", df);

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
    let df = PRICE_DATA.clone().lazy().filter(predicate).collect()?;
    println!("{:?}", df);
    Ok(())
}
