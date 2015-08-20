create external table if not exists lineitemspecial (
    l_orderkey INT4, l_shipinstruct TEXT, l_shipmode TEXT)
using text with ('text.delimiter'='|', 'text.null'='NULL') location ${table.path};
