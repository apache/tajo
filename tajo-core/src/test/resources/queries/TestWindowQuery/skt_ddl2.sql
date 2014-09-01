CREATE TABLE tm_s_sdp_mthly_web_rpts (app_grp_dtl_cd TEXT, seg_grp_dtl_cd TEXT, eqp_net_cl_cd TEXT, uv INT8, uv_rnk INT4, uv_icdc_pct FLOAT4, uv_imprt_rt FLOAT4, uv_tot_rnk INT4, pv INT8, pv_rnk INT4, pv_icdc_pct FLOAT4, pv_imprt_rt FLOAT4, pv_tot_rnk INT4, avg_pv FLOAT8, avg_pv_rnk INT4, avg_pv_icdc_pct FLOAT4, avg_pv_imprt_rt FLOAT4, avg_pv_tot_rnk INT4, use_freq FLOAT8, use_freq_rnk INT4, use_freq_icdc_pct FLOAT4, use_freq_imprt_rt FLOAT4, use_freq_tot_rnk INT4, oper_dt_hms TEXT) USING PARQUET WITH ('parquet.enable.dictionary'='false', 'transient_lastDdlTime'='1409240651', 'parquet.compression'='snappy', 'parquet.validation'='false', 'parquet.page.size'='1048576', 'parquet.block.size'='16777216') PARTITION BY COLUMN(strd_ym TEXT, app_grp_cd TEXT, seg_grp_cd TEXT);