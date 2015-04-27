select n1.n_name, n2.n_name
from nation n1 natural join nation n2
order by n2.n_name;