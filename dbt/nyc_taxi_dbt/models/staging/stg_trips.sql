SELECT
    VendorID                                    AS vendor_id,
    tpep_pickup_datetime                        AS pickup_datetime,
    tpep_dropoff_datetime                       AS dropoff_datetime,
    CAST(passenger_count AS INT64)              AS passenger_count,
    trip_distance,
    RatecodeID                                  AS rate_code_id,
    store_and_fwd_flag,
    PULocationID                                AS pickup_location_id,
    DOLocationID                                AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    source_file
FROM {{ source('raw_yellowtrips', 'trips') }}
WHERE
    passenger_count > 0
    AND trip_distance > 0
    AND payment_type != 6
    AND total_amount > 0
