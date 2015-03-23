create external table if not exists ${DB}.income_band(
      ib_income_band_sk         int4
,     ib_lower_bound            int4
,     ib_upper_bound            int4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';