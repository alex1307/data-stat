#[cfg(test)]
pub mod quantiles_test {
    use data_statistics::model::Quantiles::generate_quantiles;

    #[test]
    fn test_generate_quantiles() {
        let column = "column".to_string();
        let number_of_bins = 5;
        let quantiles = generate_quantiles(&column, number_of_bins);
        assert_eq!(quantiles.len(), number_of_bins);
        assert_eq!(quantiles[0].quantile, 0.2);
        assert_eq!(quantiles[0].alias, "q20");
        assert_eq!(quantiles[4].quantile, 1.0);
        assert_eq!(quantiles[4].alias, "q100");

        let quantiles = generate_quantiles(&column, 7);
        assert_eq!(quantiles.len(), 7);
        assert_eq!(quantiles[0].quantile, 1.0 / 7.0);
        assert_eq!(quantiles[0].alias, "q14");
        assert_eq!(quantiles[0].bin, 1);
        assert_eq!(quantiles[1].quantile, 2.0 / 7.0);
        assert_eq!(quantiles[1].alias, "q29");
        assert_eq!(quantiles[1].bin, 2);

        assert_eq!(quantiles[2].quantile, 3.0 / 7.0);
        assert_eq!(quantiles[2].alias, "q43");
        assert_eq!(quantiles[2].bin, 3);

        assert_eq!(quantiles[6].quantile, 1.0);
        assert_eq!(quantiles[6].alias, "q100");
        assert_eq!(quantiles[6].bin, 7);
    }
}
