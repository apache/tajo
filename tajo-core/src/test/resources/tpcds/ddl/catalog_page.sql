create external table if not exists ${DB}.catalog_page (
      cp_catalog_page_sk        int4
,     cp_catalog_page_id        text
,     cp_start_date_sk          int4
,     cp_end_date_sk            int4
,     cp_department             text
,     cp_catalog_number         int4
,     cp_catalog_page_number    int4
,     cp_description            text
,     cp_type                   text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';
