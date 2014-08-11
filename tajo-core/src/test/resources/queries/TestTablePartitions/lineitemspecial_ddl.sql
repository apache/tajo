create external table if not exists lineitemspecial (
    l_orderkey INT4, l_shipinstruct TEXT, l_shipmode TEXT)
using csv with ('csvfile.delimiter'='|', 'csvfile.null'='NULL') location ${table.path};
