use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Quantile {
    pub column: String,
    pub quantile: f64,
    pub value: f64,
    pub bin: usize,
    pub alias: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Quantiles {
    pub column: String,
    pub quantiles: Vec<Quantile>,
}

pub fn generate_quantiles(column: &str, number_of_bins: usize) -> Vec<Quantile> {
    // Check if number_of_bins is valid
    if number_of_bins < 1 {
        return vec![];
    }

    // Generate quantiles dynamically
    let mut quantiles = Vec::new();
    for bin in 1..=number_of_bins {
        let quantile_value = bin as f64 / number_of_bins as f64; // Compute quantile fraction
        let alias = format!("q{}", (quantile_value * 100.0).round() as usize); // Generate alias like q12, q24, etc.

        // Add the quantile struct to the vector
        quantiles.push(Quantile {
            column: column.to_string(),
            quantile: quantile_value,
            value: 0.0, // Placeholder; replace this with actual computation if needed
            bin,
            alias,
        });
    }

    // Ensure the quantiles are sorted by their quantile value
    quantiles.sort_by(|a, b| {
        a.quantile
            .partial_cmp(&b.quantile)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    quantiles
}
