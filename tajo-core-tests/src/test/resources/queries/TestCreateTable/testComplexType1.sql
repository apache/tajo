CREATE TABLE d11.complex_type1 (
  col1 array<double>,
  col2 array<record (last_name text, first_name text, age int)>,
  col3 array<map<text, text>>,
  col4 array<array<double>>
);