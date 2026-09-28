use std::process::ExitCode;
use std::time::Duration;

use charger_sim::{ChargePoint, Offline, Scenario, run};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
charger-sim [options]

  --url <ws-url>          CSMS OCPP endpoint      (default ws://localhost:8180/ocpp)
  --id <charger-id>       charge point identity   (default SIM-001)
  --tag <id-tag>          idTag to charge with    (default DEMO-TAG-1)
  --samples <n>           MeterValues to send     (default 5)
  --wh-per-sample <wh>    energy per sample       (default 1500)
  --interval-ms <ms>      time between samples    (default 1000)
  --offline-after <n>     lose the link after n samples, reconnect after the stop
  --offline-secs <s>      how long to stay offline (default 5)
  --lose-ack              send the first offline sample, drop before its answer
";

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("could not start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(simulate(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn simulate(args: Args) -> anyhow::Result<()> {
    let mut cp = ChargePoint::connect(&args.url, &args.id).await?;
    let report = run(&mut cp, &args.scenario).await?;
    println!(
        "transaction {} stopped: {} Wh -> {} Wh ({} Wh), {} message(s) replayed after reconnect",
        report.transaction_id,
        report.meter_start_wh,
        report.meter_stop_wh,
        report.meter_stop_wh - report.meter_start_wh,
        report.replayed
    );
    Ok(())
}

struct Args {
    url: String,
    id: String,
    scenario: Scenario,
}

impl Args {
    fn parse(mut it: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = Args {
            url: "ws://localhost:8180/ocpp".into(),
            id: "SIM-001".into(),
            scenario: Scenario {
                interval: Duration::from_secs(1),
                ..Scenario::default()
            },
        };
        let mut offline_after = None;
        let mut offline_secs = 5;
        let mut lose_ack = false;

        while let Some(flag) = it.next() {
            let mut value = || it.next().ok_or(format!("{flag} needs a value"));
            match flag.as_str() {
                "--url" => args.url = value()?,
                "--id" => args.id = value()?,
                "--tag" => args.scenario.id_tag = value()?,
                "--samples" => args.scenario.samples = number(&value()?)?,
                "--wh-per-sample" => args.scenario.wh_per_sample = number(&value()?)?,
                "--interval-ms" => {
                    args.scenario.interval = Duration::from_millis(number(&value()?)?)
                }
                "--offline-after" => offline_after = Some(number(&value()?)?),
                "--offline-secs" => offline_secs = number(&value()?)?,
                "--lose-ack" => lose_ack = true,
                "-h" | "--help" => return Err("usage:".into()),
                other => return Err(format!("unknown option {other}")),
            }
        }

        if let Some(after_samples) = offline_after {
            if after_samples >= args.scenario.samples {
                return Err("--offline-after must be less than --samples".into());
            }
            args.scenario.offline = Some(Offline {
                after_samples,
                for_duration: Duration::from_secs(offline_secs),
                lose_ack,
            });
        }
        Ok(args)
    }
}

fn number<T: std::str::FromStr>(s: &str) -> Result<T, String> {
    s.parse().map_err(|_| format!("not a number: {s}"))
}
