select a.app_grp_dtl_cd 
     , a.seg_grp_dtl_cd 
     , a.eqp_net_cl_cd  
     , a.uv 
     , a.uv_rnk 
     , case 
           when b.uv = 0 then 100.0 
           when b.uv is null then 100.0 
           else (a.uv - b.uv)::float4 / b.uv::float4 * 100 
       end uv_icdc_pct 
     , 100.00 as uv_imprt_rt -- uv 비중률 
     , 0 as uv_tot_rnk -- uv 전체순위  
     , a.pv 
     , a.pv_rnk 
     , case 
           when b.pv = 0 then 100.0 
           when b.pv is null then 100.0 
           else (a.pv - b.pv)::float4 / b.pv::float4 * 100 
       end pv_icdc_pct 
     , 100.00 as pv_imprt_rt -- pv 비중률 
     , 0 as pv_tot_rnk -- pv 전체순위  
     , a.avg_pv 
     , a.avg_pv_rnk 
     , case 
           when b.avg_pv = 0 then 100.0 
           when b.avg_pv is null then 100.0 
           else (a.avg_pv - b.avg_pv) / b.avg_pv * 100 
       end avg_pv_icdc_pct 
     , 100.00 as avg_pv_imprt_rt -- 평균pv 비중율 
     , 0 as avg_pv_tot_rnk -- 평균pv 전체순위   
     , a.use_freq 
     , a.use_freq_rnk 
     , case 
           when b.use_freq = 0 then 100.0 
           when b.use_freq is null then 100.0 
           else (a.use_freq - b.use_freq) / b.use_freq * 100 
       end use_freq_icdc_pct 
     , 100.00 as use_freq_imprt_rt -- 이용빈도 비중율 
     , 0 as use_freq_tot_rnk -- 이용빈도 전체순위  
     , a.oper_dt_hms 
     , a.strd_ym 
     , a.app_grp_cd 
     , a.seg_grp_cd 
  from 
       (select app_grp_dtl_cd 
            , seg_grp_dtl_cd 
            , eqp_net_cl_cd_alias as eqp_net_cl_cd  -- uv 
            , uv_cnt as uv -- uv카운트 
            , row_number() over(partition by strd_ym, seg_grp_dtl_cd order by uv_cnt desc, app_grp_dtl_cd asc ) as uv_rnk -- uv 순위  
              -- pv 
            , pv_cnt as pv -- pv카운트 
            , row_number() over(partition by strd_ym, seg_grp_dtl_cd order by pv_cnt desc, app_grp_dtl_cd asc ) as pv_rnk -- pv 순위  
              --평균 pv 
            , avg_pv_cnt as avg_pv -- 평균pv 
            , row_number() over(partition by strd_ym, seg_grp_dtl_cd order by avg_pv_cnt desc, app_grp_dtl_cd asc ) as avg_pv_rnk -- 평균pv 순위  
              -- 이용빈도 
            , use_freq -- 이용빈도 수 
            , row_number() over(partition by strd_ym, seg_grp_dtl_cd order by use_freq desc, app_grp_dtl_cd asc ) as use_freq_rnk -- 이용빈도 순위  
            , to_char(current_timestamp, 'yyyymmddhh24miss') as oper_dt_hms 
            , strd_ym 
            , app_grp_cd 
            , seg_grp_cd 
         from 
              (select kwd as app_grp_dtl_cd 
                   , coalesce(a.sex_cd, '##') as seg_grp_dtl_cd -- 상세 성별 
                   , '**' as eqp_net_cl_cd_alias -- 단말망구분없음 
                   , count(distinct a.svc_mgmt_num) as uv_cnt 
                   , sum(pv) as pv_cnt 
                   , sum(pv)::float4 / count(distinct a.svc_mgmt_num)::float4 as avg_pv_cnt 
                   , sum(use_dcnt)::float4 / count(distinct svc_mgmt_num)::float4 as use_freq -- 이용빈도 
                   , strd_ym 
                   , '81' as app_grp_cd -- web_app_grp 81:전체 
                   , '02' as seg_grp_cd -- 01:전체, 02:sex_cd(성별), 03:age_cl_cd(연령대), 04:prod_grp_cd(요금제클래스), 05:prod_grp_dtl_cd(요금제유형), 22:pmth_arpu_amt_rng_cd(전월appu금액대별), 23:pmth_data_exhst_qty_cd(전월데이터소진율), 31:srch_host(검색도메인), 10:dow_cd(요일), 11:tm_rng_cd(전일시간대), 12:tm_rng_cd(평일시간대wd), 13:tm_rng_cd(주말시간대we) 
                from tm_s_sdp_m_dow_web_sum  a
               where strd_ym = '${C_MONTH}' 
                     --and kwd is not null 
               group by a.strd_ym 
                   , app_grp_dtl_cd 
                   , seg_grp_dtl_cd 
               having count(distinct a.svc_mgmt_num) >= 10
              ) x 
       ) a 
-- 이전달 데이터 
   left outer join tm_s_sdp_mthly_web_rpts b
       on to_char(add_months(to_date(a.strd_ym, 'yyyymm'), -1), 'yyyymm') = b.strd_ym 
       and a.eqp_net_cl_cd = b.eqp_net_cl_cd 
       and a.app_grp_cd = b.app_grp_cd 
       and a.app_grp_dtl_cd = b.app_grp_dtl_cd 
       and a.seg_grp_cd = b.seg_grp_cd 
       and a.seg_grp_dtl_cd = b.seg_grp_dtl_cd 
where a.uv_rnk <= 1000 or a.pv_rnk <= 1000 or a.avg_pv_rnk <= 1000 or a.use_freq_rnk <= 1000
;