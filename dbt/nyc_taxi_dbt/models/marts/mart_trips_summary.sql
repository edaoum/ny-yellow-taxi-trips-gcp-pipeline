SELECT
    DATE(t.pickup_datetime)                     AS trip_date,
    EXTRACT(YEAR FROM t.pickup_datetime)        AS year,
    EXTRACT(MONTH FROM t.pickup_datetime)       AS month,
    EXTRACT(HOUR FROM t.pickup_datetime)        AS pickup_hour,
    pz.Borough                                  AS pickup_borough,
    dz.Borough                                  AS dropoff_borough,
    t.payment_type,
    COUNT(*)                                    AS total_trips,
    ROUND(AVG(t.total_amount), 2)               AS avg_total_amount,
    ROUND(AVG(t.trip_distance), 2)              AS avg_trip_distance,
    ROUND(AVG(t.tip_amount), 2)                 AS avg_tip_amount,
    ROUND(SUM(t.total_amount), 2)               AS total_revenue
FROM {{ ref('stg_trips') }} t
LEFT JOIN {{ source('raw_yellowtrips', 'taxi_zone') }} pz
    ON t.pickup_location_id = pz.LocationID
LEFT JOIN {{ source('raw_yellowtrips', 'taxi_zone') }} dz
    ON t.dropoff_location_id = dz.LocationID
GROUP BY
    trip_date, year, month, pickup_hour,
    pickup_borough, dropoff_borough, payment_type
