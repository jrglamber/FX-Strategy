# FX Asia Continuation Logger v2

This is the narrowed FX forward-test logger based on the historical gauntlet.

## Research focus

Asia range continuation during New York.

Pairs to use first:

- AUDUSD
- EURUSD
- GBPUSD
- USDCAD

Chart timeframe:

- 5m

Sessions use Europe/London time:

- Asia range: 00:00–07:00
- London reference range: 07:00–12:00
- NY signal window: 12:30–17:00

## Signal logic

Long:

- Candle closes above Asia high during NY signal window.

Short:

- Candle closes below Asia low during NY signal window.

Default setting is one signal per direction per pair per day.

## Stop models tracked

The Pine sends outcome results for all of these:

- fixed_10pips
- fixed_15pips
- fixed_20pips
- fixed_25pips
- fixed_30pips
- atr_2.0
- atr_2.5
- london_midpoint
- opposite_london_side

## Outcome horizons

- 60m
- 120m
- 240m

Each outcome stores:

- raw %
- raw R
- fixed-stop %
- fixed-stop R
- MFE %
- MAE %
- MFE R
- MAE R
- SL hit yes/no

## Railway settings

Keep the same Railway service if desired.

Required variables:

```text
DB_PATH=/app/data/fx_session_logger.sqlite
WEBHOOK_SECRET=your_secret_here
```

Start command:

```text
gunicorn app:app
```

Volume mount path:

```text
/app/data
```

## TradingView alert setup

For each chart/pair:

1. Add `fx_asia_continuation_logger_v2.pine` to the 5m chart.
2. Set Pair override if the chart ticker is not clean, for example `AUDUSD`.
3. Create alert.
4. Condition: `FX Asia Continuation Logger v2`.
5. Alert option: `Any alert() function call`.
6. Webhook URL:

```text
https://your-railway-domain.up.railway.app/webhook?secret=your_secret_here
```

7. Leave the Pine JSON secret input blank if using the URL secret.
8. Frequency is controlled by the script with `alert.freq_once_per_bar_close`.

## Dashboard

Main endpoints:

```text
/
 /health
 /summary
 /download/v2-signals.csv
 /download/v2-outcomes.csv
 /download/v2-merged.csv
 /download/v2-summary-all.csv
 /download/all.zip
```

## Important

This app stores v2 data in separate tables:

- v2_signals
- v2_outcomes
- raw_events

It does not delete old v1 tables.
