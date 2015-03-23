create external table if not exists ${DB}.store_sales
(
    ss_sold_date_sk           int4,
    ss_sold_time_sk           int4,
    ss_item_sk                int4,
    ss_customer_sk            int4,
    ss_cdemo_sk               int4,
    ss_hdemo_sk               int4,
    ss_addr_sk                int4,
    ss_store_sk               int4,
    ss_promo_sk               int4,
    ss_ticket_number          int4,
    ss_quantity               int4,
    ss_wholesale_cost         float4,
    ss_list_price             float4,
    ss_sales_price            float4,
    ss_ext_discount_amt       float4,
    ss_ext_sales_price        float4,
    ss_ext_wholesale_cost     float4,
    ss_ext_list_price         float4,
    ss_ext_tax                float4,
    ss_coupon_amt             float4,
    ss_net_paid               float4,
    ss_net_paid_inc_tax       float4,
    ss_net_profit             float4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';