create external table if not exists ${DB}.store_returns
(
    sr_returned_date_sk       int4,
    sr_return_time_sk         int4,
    sr_item_sk                int4,
    sr_customer_sk            int4,
    sr_cdemo_sk               int4,
    sr_hdemo_sk               int4,
    sr_addr_sk                int4,
    sr_store_sk               int4,
    sr_reason_sk              int4,
    sr_ticket_number          int4,
    sr_return_quantity        int4,
    sr_return_amt             float4,
    sr_return_tax             float4,
    sr_return_amt_inc_tax     float4,
    sr_fee                    float4,
    sr_return_ship_cost       float4,
    sr_refunded_cash          float4,
    sr_reversed_charge        float4,
    sr_store_credit           float4,
    sr_net_loss               float4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';