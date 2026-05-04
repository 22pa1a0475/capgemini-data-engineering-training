{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id'
    )
}}

{% set cols = [
    'trip_id',
    'vehicle_id',
    'customer_id',
    'driver_id',
    'trip_start_time',
    'trip_end_time',
    'distance_km',
    'start_location',
    'end_location',
    'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}

FROM
    {{ source("source_bronze", "trips") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT max_ts FROM (
        SELECT coalesce(MAX(last_updated_timestamp),'1900-01-01') AS max_ts
        FROM {{ this }}
    )
)
{% endif %}