{{ config(
    materialized='view'
) }}

SELECT
    wdm.measurement_date,
    wdm.avg_temperature_celsius,
    wdm.avg_humidity_percent,
    wdm.avg_wind_speed_ms,
    wdm.total_precipitation_mm,
    wa.temperature_anomaly,
    wa.anomaly_severity,
    wc.avg_comfort_index AS daily_avg_comfort_index,
    wc.worst_comfort_category AS daily_worst_comfort,
    wdm.high_temperature_alert,
    wdm.high_humidity_alert,
    wdm.high_wind_alert,
    wdm.high_precipitation_alert,
    wa.significant_anomaly_alert,
    max(wc.low_comfort_alert) AS daily_low_comfort_alert
FROM {{ ref('mart_weather_daily_metrics') }} wdm
LEFT JOIN {{ ref('mart_weather_anomalies') }} wa
    ON wdm.measurement_date = wa.measurement_date
LEFT JOIN {{ ref('mart_weather_comfort') }} wc
    ON wdm.measurement_date = wc.measurement_date
GROUP BY
    wdm.measurement_date,
    wdm.avg_temperature_celsius,
    wdm.avg_humidity_percent,
    wdm.avg_wind_speed_ms,
    wdm.total_precipitation_mm,
    wa.temperature_anomaly,
    wa.anomaly_severity,
    wc.avg_comfort_index,
    wc.worst_comfort_category,
    wdm.high_temperature_alert,
    wdm.high_humidity_alert,
    wdm.high_wind_alert,
    wdm.high_precipitation_alert,
    wa.significant_anomaly_alert