CREATE EXTERNAL TABLE ${0} (xx text, yy text, zz text) USING CSV WITH('text.delimiter'='+') LOCATION ${table.path};
