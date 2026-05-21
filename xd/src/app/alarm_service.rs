use crate::bus::EventBus;
use crate::domain::AlarmLevel;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Per-sensor threshold rule.
///
/// - high/high_clear: raise high alarm when value >= high, clear when value <= high_clear.
/// - low/low_clear: raise low alarm when value <= low, clear when value >= low_clear.
#[derive(Clone, Debug)]
pub struct AlarmRule {
    pub high: Option<f64>,
    pub high_clear: Option<f64>,
    pub low: Option<f64>,
    pub low_clear: Option<f64>,
    pub level: AlarmLevel,
    pub name: String,
}

impl Default for AlarmRule {
    fn default() -> Self {
        Self {
            high: Some(90.0),
            high_clear: Some(85.0),
            low: Some(10.0),
            low_clear: Some(15.0),
            level: AlarmLevel::Warning,
            name: "default_rule".to_string(),
        }
    }
}

/// App-layer alarm evaluator.
///
/// Legacy built-in test alarms have been removed. Runtime CAN alarms are
/// handled by the UI threshold rules instead.
#[derive(Clone)]
pub struct AlarmService {
    rules: Arc<RwLock<HashMap<usize, AlarmRule>>>,
}

impl AlarmService {
    pub fn new(_bus: EventBus) -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set or replace rule by sensor id.
    pub fn set_rule(&self, sensor_id: usize, rule: AlarmRule) {
        if let Ok(mut rules) = self.rules.write() {
            rules.insert(sensor_id, rule);
        }
    }

    /// Evaluate one telemetry sample.
    pub fn evaluate_sample(&self, _device_id: &str, _sensor_id: usize, _value: f64) {}
}
