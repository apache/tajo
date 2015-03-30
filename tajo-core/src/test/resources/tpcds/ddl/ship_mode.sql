create external table if not exists ${DB}.ship_mode(
      sm_ship_mode_sk           int4
,     sm_ship_mode_id           text
,     sm_type                   text
,     sm_code                   text
,     sm_carrier                text
,     sm_contract               text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';