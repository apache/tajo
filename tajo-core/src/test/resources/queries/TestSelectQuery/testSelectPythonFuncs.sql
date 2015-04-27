select helloworld(), sum_py(n_nationkey, n_regionkey) as sum, concat_py(n_name) as concat
from nation where n_nationkey < 5