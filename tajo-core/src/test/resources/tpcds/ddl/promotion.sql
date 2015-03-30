create external table if not exists ${DB}.promotion
(
    p_promo_sk                int4,
    p_promo_id                text,
    p_start_date_sk           int4,
    p_end_date_sk             int4,
    p_item_sk                 int4,
    p_cost                    float4,
    p_response_target         int4,
    p_promo_name              text,
    p_channel_dmail           text,
    p_channel_email           text,
    p_channel_catalog         text,
    p_channel_tv              text,
    p_channel_radio           text,
    p_channel_press           text,
    p_channel_event           text,
    p_channel_demo            text,
    p_channel_details         text,
    p_purpose                 text,
    p_discount_active         text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';