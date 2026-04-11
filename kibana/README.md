# Kibana Design

Create two data views:

- `logs-infra_feature_windows-*`
- `logs-infra_predictions-*`

Recommended dashboards:

1. Fleet Risk Overview
- Lens table grouped by `machine_id`
- Metrics: latest `p_fail_5m`, `risk_band`, `predicted_failure_type`, `missing_ratio`
- Sort descending by `p_fail_5m`

2. Host Timeline
- Line chart on `@timestamp`
- Series: `cpu_pct_mean`, `mem_pct_mean`, `latency_p95_ms_mean`, `error_count_sum`
- Overlay area or line for `p_fail_5m`

3. Failure Probability Gauge
- Gauge on latest `p_fail_5m`
- Breaks: `0.0-0.25` low, `0.25-0.35` medium, `0.35-0.8` high, `0.8+` critical

4. Leading Indicators
- Histogram of `predicted_failure_type`
- Heatmap of `machine_id` x `risk_band`

Recommended alert rules:

1. Index threshold rule on `logs-infra_predictions-*`
- Group by `machine_id`
- Condition: max of `p_fail_5m` is above `0.80`
- Time window: last `2 minutes`
- Require at least `2` consecutive evaluations

2. Elasticsearch query rule for early-warning confirmation
- Query: `risk_band:(high OR critical) AND missing_ratio:[0 TO 0.5]`
- Add recovery action when the same machine drops below threshold for 3 evaluations

Operational notes:

- Use `machine_id` and `cluster_id` in all alert messages so the on-call can pivot quickly.
- Keep feature windows and predictions in separate panels so operators can compare telemetry with the model score.
- Suppress repeated alerts per `machine_id` for at least 10 minutes to avoid paging storms.
