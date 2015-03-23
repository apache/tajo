create external table if not exists ${DB}.household_demographics
(
    hd_demo_sk                int4,
    hd_income_band_sk         int4,
    hd_buy_potential          text,
    hd_dep_count              int4,
    hd_vehicle_count          int4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';