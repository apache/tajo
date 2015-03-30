create external table if not exists ${DB}.time_dim
(
    t_time_sk                 int4,
    t_time_id                 text,
    t_time                    int4,
    t_hour                    int4,
    t_minute                  int4,
    t_second                  int4,
    t_am_pm                   text,
    t_shift                   text,
    t_sub_shift               text,
    t_meal_time               text
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';