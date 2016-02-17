create external table if not exists nation_parquet (
    n_nationkey int,
    n_name text,
    n_regionkey int,
    n_comment text)
    using parquet location ${table.path};