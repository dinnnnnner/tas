use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct RawSample {
    pub device_id: String,
    pub sensor_id: usize,
    pub t_sec: f64,
    pub value: f64,
    pub req_id: u64,
}

#[derive(Clone, Debug)]
pub struct SignalSample {
    pub signal_id: String,
    pub device_id: String,
    pub t_sec: f64,
    pub value: f64,
    pub req_id: u64,
}

#[derive(Clone, Debug)]
pub struct SignalSpec {
    pub id: String,
    pub name: String,
    pub unit: String,
    pub decimals: usize,
    pub kind: SignalKind,
}

#[derive(Clone, Debug)]
pub enum SignalKind {
    SourceSensor {
        sensor_id: usize,
    },
    Derived {
        formula: DerivedFormula,
    },
}

#[derive(Clone, Debug)]
pub enum DerivedFormula {
    ScaleOffset {
        input_signal_id: String,
        scale: f64,
        offset: f64,
    },
    OffsetScale {
        input_signal_id: String,
        input_offset: f64,
        scale: f64,
    },
}

pub struct SignalProcessor {
    specs: Vec<SignalSpec>,
    latest_values: HashMap<(String, String), SignalSample>,
}

impl SignalProcessor {
    pub fn new(specs: Vec<SignalSpec>) -> Self {
        Self {
            specs,
            latest_values: HashMap::new(),
        }
    }

    pub fn specs(&self) -> &[SignalSpec] {
        &self.specs
    }

    pub fn spec(&self, signal_id: &str) -> Option<&SignalSpec> {
        self.specs.iter().find(|spec| spec.id == signal_id)
    }

    pub fn ingest_raw(&mut self, raw: RawSample) -> Vec<SignalSample> {
        let mut out = Vec::new();

        for spec in &self.specs {
            if let SignalKind::SourceSensor { sensor_id } = spec.kind {
                if sensor_id == raw.sensor_id {
                    out.push(SignalSample {
                        signal_id: spec.id.clone(),
                        device_id: raw.device_id.clone(),
                        t_sec: raw.t_sec,
                        value: raw.value,
                        req_id: raw.req_id,
                    });
                }
            }
        }

        for sample in &out {
            self.latest_values.insert(
                (sample.device_id.clone(), sample.signal_id.clone()),
                sample.clone(),
            );
        }

        let mut derived = Vec::new();
        for spec in &self.specs {
            if let SignalKind::Derived { formula } = &spec.kind {
                if let Some(sample) = self.compute_derived(formula, &spec.id, &raw.device_id, raw.t_sec, raw.req_id) {
                    derived.push(sample);
                }
            }
        }

        for sample in &derived {
            self.latest_values.insert(
                (sample.device_id.clone(), sample.signal_id.clone()),
                sample.clone(),
            );
        }

        out.extend(derived);
        out
    }

    fn compute_derived(
        &self,
        formula: &DerivedFormula,
        signal_id: &str,
        device_id: &str,
        t_sec: f64,
        req_id: u64,
    ) -> Option<SignalSample> {
        match formula {
            DerivedFormula::ScaleOffset {
                input_signal_id,
                scale,
                offset,
            } => {
                let input = self
                    .latest_values
                    .get(&(device_id.to_string(), input_signal_id.clone()))?;
                Some(SignalSample {
                    signal_id: signal_id.to_string(),
                    device_id: device_id.to_string(),
                    t_sec,
                    value: input.value * *scale + *offset,
                    req_id,
                })
            }
            DerivedFormula::OffsetScale {
                input_signal_id,
                input_offset,
                scale,
            } => {
                let input = self
                    .latest_values
                    .get(&(device_id.to_string(), input_signal_id.clone()))?;
                Some(SignalSample {
                    signal_id: signal_id.to_string(),
                    device_id: device_id.to_string(),
                    t_sec,
                    value: (input.value - *input_offset) * *scale,
                    req_id,
                })
            }
        }
    }
}

pub fn default_signal_specs(sensor_count: usize) -> Vec<SignalSpec> {
    let mut specs = Vec::new();

    for sensor_id in 0..sensor_count {
        let (name, unit) = match sensor_id {
            0 => ("sent_t1_angle".to_string(), "deg".to_string()),
            1 => ("sent_t1_torque".to_string(), "Nm".to_string()),
            2 => ("sent_t2_angle".to_string(), "deg".to_string()),
            3 => ("sent_t2_torque".to_string(), "Nm".to_string()),
            4 => ("sent_s_angle".to_string(), "deg".to_string()),
            _ => (format!("Sensor {}", sensor_id), "raw".to_string()),
        };

        specs.push(SignalSpec {
            id: format!("sensor_{sensor_id}_raw"),
            name,
            unit,
            decimals: 3,
            kind: SignalKind::SourceSensor { sensor_id },
        });
    }

    specs.push(SignalSpec {
        id: "sensor_0_angle".to_string(),
        name: "sensor_P1_angle".to_string(),
        unit: "deg".to_string(),
        decimals: 3,
        kind: SignalKind::Derived {
            formula: DerivedFormula::OffsetScale {
                input_signal_id: "sensor_0_raw".to_string(),
                input_offset: 2048.0,
                scale: 40.0 / 4092.0,
            },
        },
    });
    specs.push(SignalSpec {
        id: "sensor_1_angle".to_string(),
        name: "sensor_T1_angle".to_string(),
        unit: "deg".to_string(),
        decimals: 3,
        kind: SignalKind::Derived {
            formula: DerivedFormula::OffsetScale {
                input_signal_id: "sensor_1_raw".to_string(),
                input_offset: 2047.5,
                scale: 12.0 / 4079.0,
            },
        },
    });
    specs.push(SignalSpec {
        id: "sensor_2_angle".to_string(),
        name: "sensor_P2_angle".to_string(),
        unit: "deg".to_string(),
        decimals: 3,
        kind: SignalKind::Derived {
            formula: DerivedFormula::OffsetScale {
                input_signal_id: "sensor_2_raw".to_string(),
                input_offset: 2048.0,
                scale: -40.0 / 4092.0,
            },
        },
    });
    specs.push(SignalSpec {
        id: "sensor_3_angle".to_string(),
        name: "sensor_T2_angle".to_string(),
        unit: "deg".to_string(),
        decimals: 3,
        kind: SignalKind::Derived {
            formula: DerivedFormula::OffsetScale {
                input_signal_id: "sensor_3_raw".to_string(),
                input_offset: 2047.5,
                scale: -12.0 / 4079.0,
            },
        },
    });

    specs
}
