create external table if not exists ${DB}.customer_demographics
(
    cd_demo_sk                int4,
    cd_gender                 text,
    cd_marital_status         text,
    cd_education_status       text,
    cd_purchase_estimate      int4,
    cd_credit_rating          text,
    cd_dep_count              int4,
    cd_dep_employed_count     int4,
    cd_dep_college_count      int4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';