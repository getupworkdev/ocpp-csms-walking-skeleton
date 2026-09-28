//! Session pricing: a flat session fee plus a per-kWh rate.
//!
//! Everything is integer maths in minor currency units. Energy comes from the
//! charger's register (meterStop - meterStart), never from summing MeterValues,
//! so samples that arrive late, twice, or not at all cannot change the price.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tariff {
    pub price_per_kwh_minor: i64,
    pub session_fee_minor: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Priced {
    pub energy_wh: i64,
    pub energy_cost_minor: i64,
    pub cost_minor: i64,
}

impl Tariff {
    /// Price a finished session. A register that went backwards (meter swap,
    /// rollover, bad firmware) is charged as zero energy rather than a refund.
    pub fn price(&self, meter_start_wh: i64, meter_stop_wh: i64) -> Priced {
        let energy_wh = meter_stop_wh.saturating_sub(meter_start_wh).max(0);
        // Wh * (minor/kWh) / 1000, rounded half up.
        let energy_cost_minor = energy_wh
            .saturating_mul(self.price_per_kwh_minor)
            .saturating_add(500)
            / 1000;
        Priced {
            energy_wh,
            energy_cost_minor,
            cost_minor: self.session_fee_minor.saturating_add(energy_cost_minor),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EUR_35: Tariff = Tariff {
        price_per_kwh_minor: 35,
        session_fee_minor: 50,
    };

    #[test]
    fn fee_plus_energy() {
        // 12.345 kWh at 0.35 = 4.32075 -> 4.32, plus 0.50 fee.
        let p = EUR_35.price(1_000, 13_345);
        assert_eq!(p.energy_wh, 12_345);
        assert_eq!(p.energy_cost_minor, 432);
        assert_eq!(p.cost_minor, 482);
    }

    #[test]
    fn rounds_half_up() {
        // 10 Wh at 50/kWh = 0.5 minor -> 1.
        let t = Tariff {
            price_per_kwh_minor: 50,
            session_fee_minor: 0,
        };
        assert_eq!(t.price(0, 10).cost_minor, 1);
        assert_eq!(t.price(0, 9).cost_minor, 0);
    }

    #[test]
    fn zero_energy_is_just_the_fee() {
        assert_eq!(EUR_35.price(500, 500).cost_minor, 50);
    }

    #[test]
    fn register_going_backwards_is_not_a_refund() {
        let p = EUR_35.price(9_000, 1_000);
        assert_eq!(p.energy_wh, 0);
        assert_eq!(p.cost_minor, 50);
    }
}
