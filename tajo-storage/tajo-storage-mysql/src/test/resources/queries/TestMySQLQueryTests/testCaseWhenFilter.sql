select * from lineitem where
  case
    when l_returnflag = 'N' then true
    else false
  end
;