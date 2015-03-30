create external table if not exists ${DB}.customer_address
(
    ca_address_sk             int4,
    ca_address_id             text,
    ca_street_number          text,
    ca_street_name            text,
    ca_street_type            text,
    ca_suite_number           text,
    ca_city                   text,
    ca_county                 text,
    ca_state                  text,
    ca_zip                    text,
    ca_country                text,
    ca_gmt_offset             float4,
    ca_location_type          text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';
