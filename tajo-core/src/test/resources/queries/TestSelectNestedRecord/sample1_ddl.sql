CREATE EXTERNAL TABLE ${0} (
  title TEXT,
  name RECORD (
    first_name TEXT,
    last_name TEXT
  )
) USING JSON LOCATION ${table.path};