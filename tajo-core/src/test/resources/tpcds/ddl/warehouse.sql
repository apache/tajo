create external table if not exists ${DB}.warehouse(
      w_warehouse_sk            int4
,     w_warehouse_id            text
,     w_warehouse_name          text
,     w_warehouse_sq_ft         int4
,     w_street_number           text
,     w_street_name             text
,     w_street_type             text
,     w_suite_number            text
,     w_city                    text
,     w_county                  text
,     w_state                   text
,     w_zip                     text
,     w_country                 text
,     w_gmt_offset              float4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';