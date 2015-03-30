create external table if not exists ${DB}.web_page(
      wp_web_page_sk            int4
,     wp_web_page_id            text
,     wp_rec_start_date        date
,     wp_rec_end_date          date
,     wp_creation_date_sk       int4
,     wp_access_date_sk         int4
,     wp_autogen_flag           text
,     wp_customer_sk            int4
,     wp_url                    text
,     wp_type                   text
,     wp_char_count             int4
,     wp_link_count             int4
,     wp_image_count            int4
,     wp_max_ad_count           int4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';