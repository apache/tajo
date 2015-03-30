create external table if not exists ${DB}.reason(
      r_reason_sk               int4
,     r_reason_id               text
,     r_reason_desc             text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';