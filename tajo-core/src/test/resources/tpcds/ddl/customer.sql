create external table if not exists ${DB}.customer
(
    c_customer_sk             int4,
    c_customer_id             text,
    c_current_cdemo_sk        int4,
    c_current_hdemo_sk        int4,
    c_current_addr_sk         int4,
    c_first_shipto_date_sk    int4,
    c_first_sales_date_sk     int4,
    c_salutation              text,
    c_first_name              text,
    c_last_name               text,
    c_preferred_cust_flag     text,
    c_birth_day               int4,
    c_birth_month             int4,
    c_birth_year              int4,
    c_birth_country           text,
    c_login                   text,
    c_email_address           text,
    c_last_review_date        text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';
