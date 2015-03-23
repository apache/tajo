create external table if not exists ${DB}.web_returns
(
    wr_returned_date_sk       int4,
    wr_returned_time_sk       int4,
    wr_item_sk                int4,
    wr_refunded_customer_sk   int4,
    wr_refunded_cdemo_sk      int4,
    wr_refunded_hdemo_sk      int4,
    wr_refunded_addr_sk       int4,
    wr_returning_customer_sk  int4,
    wr_returning_cdemo_sk     int4,
    wr_returning_hdemo_sk     int4,
    wr_returning_addr_sk      int4,
    wr_web_page_sk            int4,
    wr_reason_sk              int4,
    wr_order_number           int4,
    wr_return_quantity        int4,
    wr_return_amt             float4,
    wr_return_tax             float4,
    wr_return_amt_inc_tax     float4,
    wr_fee                    float4,
    wr_return_ship_cost       float4,
    wr_refunded_cash          float4,
    wr_reversed_charge        float4,
    wr_account_credit         float4,
    wr_net_loss               float4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';