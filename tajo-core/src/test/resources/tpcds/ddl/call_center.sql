create external table if not exists ${DB}.call_center (
      cc_call_center_sk         int4
,     cc_call_center_id         text              
,     cc_rec_start_date        date
,     cc_rec_end_date          date
,     cc_closed_date_sk         int4
,     cc_open_date_sk           int4
,     cc_name                   text                   
,     cc_class                  text                   
,     cc_employees              int4
,     cc_sq_ft                  int4
,     cc_hours                  text                      
,     cc_manager                text                   
,     cc_mkt_id                 int4
,     cc_mkt_class              text                      
,     cc_mkt_desc               text                  
,     cc_market_manager         text                   
,     cc_division               int4
,     cc_division_name          text                   
,     cc_company                int4
,     cc_company_name           text                      
,     cc_street_number          text                      
,     cc_street_name            text                   
,     cc_street_type            text                      
,     cc_suite_number           text                      
,     cc_city                   text                   
,     cc_county                 text                   
,     cc_state                  text                       
,     cc_zip                    text                      
,     cc_country                text                   
,     cc_gmt_offset             float4
,     cc_tax_percentage         float4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';
