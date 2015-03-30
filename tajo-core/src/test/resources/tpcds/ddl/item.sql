create external table if not exists ${DB}.item
(
    i_item_sk                 int4,
    i_item_id                 text,
    i_rec_start_date          date,
    i_rec_end_date            date,
    i_item_desc               text,
    i_current_price           float4,
    i_wholesale_cost          float4,
    i_brand_id                int4,
    i_brand                   text,
    i_class_id                int4,
    i_class                   text,
    i_category_id             int4,
    i_category                text,
    i_manufact_id             int4,
    i_manufact                text,
    i_size                    text,
    i_formulation             text,
    i_color                   text,
    i_units                   text,
    i_container               text,
    i_manager_id              int4,
    i_product_name            text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';