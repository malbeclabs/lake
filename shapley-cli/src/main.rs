use network_shapley::{
    shapley::ShapleyInput,
    types::{Demand, Device, PrivateLink, PublicLink},
};
use serde::{Deserialize, Serialize};
use std::io::{self, Read};

/// Deserializable wrapper for ShapleyInput (the crate doesn't derive Deserialize on it).
#[derive(Deserialize)]
struct InputJson {
    private_links: Vec<PrivateLink>,
    devices: Vec<Device>,
    demands: Vec<Demand>,
    public_links: Vec<PublicLink>,
    operator_uptime: f64,
    contiguity_bonus: f64,
    demand_multiplier: f64,
}

/// Output format: one entry per operator with value and proportion.
#[derive(Serialize)]
struct OperatorValue {
    operator: String,
    value: f64,
    proportion: f64,
}

fn main() {
    let mut input_json = String::new();
    io::stdin()
        .read_to_string(&mut input_json)
        .expect("failed to read stdin");

    let parsed: InputJson =
        serde_json::from_str(&input_json).expect("failed to parse input JSON");

    let input = ShapleyInput {
        private_links: parsed.private_links,
        devices: parsed.devices,
        demands: parsed.demands,
        public_links: parsed.public_links,
        operator_uptime: parsed.operator_uptime,
        contiguity_bonus: parsed.contiguity_bonus,
        demand_multiplier: parsed.demand_multiplier,
    };

    let result = input.compute().expect("shapley computation failed");

    let output: Vec<OperatorValue> = result
        .into_iter()
        .map(|(operator, sv)| OperatorValue {
            operator,
            value: sv.value,
            proportion: sv.proportion,
        })
        .collect();

    let json = serde_json::to_string(&output).expect("failed to serialize output");
    println!("{json}");
}
