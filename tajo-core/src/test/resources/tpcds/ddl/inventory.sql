create external table if not exists ${DB}.inventory
(
    inv_date_sk  int4,
    inv_item_sk	  int4,
    inv_warehouse_sk     int4,
    inv_quantity_on_hand  int4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';