use std::{collections::HashMap, vec};

use polars::prelude::{col, SortMultipleOptions};
use serde_json::Value;

use crate::{
    services::{PriceService::to_aggregator, VehicleService::to_generic_json},
    VEHICLE_STATIC_DATA,
};

use super::PriceService::{to_predicate, StatisticSearchPayload};

pub fn chartData(search: StatisticSearchPayload) -> HashMap<String, Value> {
    let df = VEHICLE_STATIC_DATA.clone();
    if search.group.is_empty() {
        return HashMap::new();
    } else {
        let filterConditions = to_predicate(search.clone());
        let stat_column = search.stat_column.unwrap_or("advert_id".to_string());
        let aggregators = if stat_column == "advert_id" {
            to_aggregator(vec!["count".to_string()], &stat_column)
        } else {
            to_aggregator(search.aggregators.clone(), &stat_column)
        };

        let by = search.group.iter().map(|k| col(k)).collect::<Vec<_>>();
        let df = df
            .with_columns(&by)
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
            let sort = SortMultipleOptions::new()
                .with_order_descending_multi(orders)
                .with_nulls_last(true);
            df.sort(&columns, sort).collect().unwrap()
        } else {
            let sort = SortMultipleOptions::new()
                .with_order_descending_multi(vec![false])
                .with_nulls_last(true);
            df.sort(&vec!["count".to_string()], sort).collect().unwrap()
        };
        to_generic_json(&result)
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::{col, lit, when, IntoLazy, SortMultipleOptions};

    use crate::{configure_log4rs, services::VehicleService::to_generic_json, PRICE_DATA};

    #[test]
    fn test_PriceDistribution() {
        configure_log4rs("resources/log4rs.yml");
        let column = "price_in_eur".to_string();
        let df = PRICE_DATA.clone();
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
