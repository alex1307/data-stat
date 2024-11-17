#[cfg(test)]

mod chart_services_test {
    use std::{fs::File, io::Write, vec};

    use data_statistics::{
        configure_log4rs,
        model::{
            AxumAPIModel::{Order, StatisticSearchPayload},
            DistributionType,
            Intervals::{Interval, SortedIntervals},
        },
        services::{
            ChartServices::{calculate, clean_data, data_to_bins, get_statistic_data},
            VehicleService::to_generic_json,
        },
        PRICE_DATA,
    };
    use log::info;

    use polars::prelude::{col, lit, when, IntoLazy, SortMultipleOptions};

    #[test]
    fn test_price_distribution() {
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
            .sort(vec!["price_breakdown_order".to_string()], sort)
            .collect()
            .unwrap();
        log::info!("{:?}", grouped_df);
        let json = to_generic_json(&grouped_df);
        log::info!("{:?}", json);
    }

    #[test]
    fn test_interval_distribution() {
        configure_log4rs("resources/log4rs.yml");
        let column = "price_in_eur";
        let intervals = vec![
            Interval {
                column: "price_in_eur".to_string(),
                start: 0,
                end: 10_000,
                category: "1".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 10_000,
                end: 20_000,
                category: "2".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 20_000,
                end: 30_000,
                category: "3".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 30_000,
                end: 40_000,
                category: "4".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 40_000,
                end: 50_000,
                category: "5".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 50_000,
                end: 60_000,
                category: "6".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 60_000,
                end: 70_000,
                category: "7".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 70_000,
                end: 80_000,
                category: "8".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 80_000,
                end: 90_000,
                category: "9".to_string(),
                index: 0,
            },
            Interval {
                column: "price_in_eur".to_string(),
                start: 90_000,
                end: 100_000,
                category: "10".to_string(),
                index: 0,
            },
        ];

        let search = StatisticSearchPayload {
            make: Some("Audi".to_string()),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            stat_column: Some("price_in_eur".to_string()),
            order: vec![{
                Order {
                    column: "stat_category".to_string(),
                    asc: false,
                }
            }],
            ..Default::default()
        };
        let result = calculate(column, SortedIntervals::from(intervals), &search);
        if let Ok(json) = result {
            info!("{:?}", json);
        }
    }

    #[test]
    fn test_qunatile_distribution() {
        configure_log4rs("resources/log4rs.yml");

        let search = StatisticSearchPayload {
            make: Some("Audi".to_string()),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            stat_column: Some("price_in_eur".to_string()),
            order: vec![{
                Order {
                    column: "stat_category".to_string(),
                    asc: false,
                }
            }],
            ..Default::default()
        };
        let statistics = get_statistic_data("price_in_eur", &search, None, 10).unwrap();
        info!("{:?}", statistics);
        let quantiles = statistics.quantiles;
        let mut intervals = vec![Interval {
            column: "price_in_eur".to_string(),
            start: statistics.min,
            end: quantiles[0].value as i32,
            category: "1".to_string(),
            index: 0,
        }];

        for i in 1..quantiles.len() {
            let interval = Interval {
                column: "price_in_eur".to_string(),
                start: quantiles[i - 1].value as i32,
                end: quantiles[i].value as i32,
                category: (i + 1).to_string(),
                index: i,
            };
            intervals.push(interval);
        }
        let json = calculate(
            "price_in_eur",
            SortedIntervals::from(intervals.clone()),
            &search,
        );
        info!("{:?}", json);
        let json = calculate("price_in_eur", SortedIntervals::from(intervals), &search);
        if let Ok(json) = json {
            info!("{:?}", json);
        }
    }

    #[test]
    pub fn test_intervals() {
        configure_log4rs("resources/log4rs.yml");
        let search = StatisticSearchPayload {
            make: Some("Audi".to_string()),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            stat_column: Some("price_in_eur".to_string()),
            order: vec![{
                Order {
                    column: "stat_category".to_string(),
                    asc: false,
                }
            }],
            ..Default::default()
        };
        let stat_interval = clean_data("price_in_eur", search.clone());
        info!("{:?}", stat_interval);
        let step = 20_000;
        let mut intervals = Vec::new();
        for i in 0..10 {
            let interval: Interval<i32> = Interval {
                column: "price_in_eur".to_string(),
                start: stat_interval.start + i * step,
                end: stat_interval.start + (i + 1) * step,
                category: i.to_string(),
                index: 0,
            };

            intervals.push(interval);
        }
        let json = calculate("price_in_eur", SortedIntervals::from(intervals), &search);
        info!("{:?}", json);
    }

    #[test]
    fn test_data_to_bins_by_interval() {
        configure_log4rs("resources/log4rs.yml");
        let column = "price_in_eur";
        let filter = StatisticSearchPayload {
            make: Some("Audi".to_string()),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            stat_column: Some("price_in_eur".to_string()),
            order: vec![{
                Order {
                    column: "stat_category".to_string(),
                    asc: false,
                }
            }],
            ..Default::default()
        };
        let all = true;
        let distribution_type = DistributionType::ByInterval;
        let number_of_bins = 4;

        let result = data_to_bins(column, filter, all, distribution_type, number_of_bins);

        assert!(result.is_ok(), "Result should be Ok");
        let chart_data = result.unwrap();

        // Assert the number of intervals is correct
        assert_eq!(chart_data.data.len(), number_of_bins);

        // Assert the intervals are evenly spaced
        for (i, interval) in chart_data.data.iter().enumerate() {
            assert_eq!(
                interval.category,
                i.to_string(),
                "Category mismatch for interval {}",
                i
            );
        }
        let json = serde_json::to_string_pretty(&chart_data);
        info!("{:?}", json);
        let mut file = File::create("chart_data.json").unwrap();
        let _ = file.write_all(json.unwrap().as_bytes()).unwrap();
    }

    #[test]
    fn test_data_to_bins_by_quantile() {
        configure_log4rs("resources/log4rs.yml");
        let column = "price_in_eur";
        let filter = StatisticSearchPayload {
            make: Some("Audi".to_string()),
            engine: Some(vec!["Petrol".to_string(), "Diesel".to_string()]),
            stat_column: Some("price_in_eur".to_string()),

            ..Default::default()
        };
        let all = true;
        let distribution_type = DistributionType::ByQuantile;
        let number_of_bins = 4;

        let result = data_to_bins(column, filter, all, distribution_type, number_of_bins);

        assert!(result.is_ok(), "Result should be Ok");
        let chart_data = result.unwrap();

        // Assert the number of intervals is correct
        assert_eq!(chart_data.data.len(), number_of_bins);

        // Assert the intervals are evenly spaced
        for (i, interval) in chart_data.data.iter().enumerate() {
            assert_eq!(
                interval.category,
                (i + 1).to_string(),
                "Category mismatch for interval {}",
                i
            );
        }
        let json = serde_json::to_string_pretty(&chart_data);
        info!("{:?}", json);
        let mut file = File::create("chart_data.json").unwrap();
        let _ = file.write_all(json.unwrap().as_bytes()).unwrap();
    }
}
