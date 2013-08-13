/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

parser grammar SQLParser;

options {
	language=Java;
	tokenVocab=SQLLexer;
}

@header {
}

@members {
}

/*
===============================================================================
  <data types>
===============================================================================
*/

data_type
  : predefined_type
  ;

predefined_type
  : character_string_type
  | national_character_string_type
  | binary_large_object_string_type
  | numeric_type
  | boolean_type
  | datetime_type
  | bit_type
  | binary_type
  ;

character_string_type
  : CHARACTER type_length?
  | CHAR type_length?  |
  | CHARACTER VARYING type_length?
  | CHAR VARYING type_length?
  | VARCHAR type_length?
  | TEXT
  ;

type_length
  : LEFT_PAREN NUMBER RIGHT_PAREN
  ;

national_character_string_type
  : NATIONAL CHARACTER type_length?
  | NATIONAL CHAR type_length?
  | NCHAR type_length?
  | NATIONAL CHARACTER VARYING type_length?
  | NATIONAL CHAR VARYING type_length?
  | NCHAR VARYING type_length?
  | NVARCHAR type_length?
  ;

binary_large_object_string_type
  : BLOB type_length?
  | BYTEA type_length?
  ;

numeric_type
  : exact_numeric_type | approximate_numeric_type
  ;

exact_numeric_type
  : NUMERIC (precision_param)?
  | DECIMAL (precision_param)?
  | DEC (precision_param)?
  | INT1
  | TINYINT
  | INT2
  | SMALLINT
  | INT4
  | INT
  | INTEGER
  | INT8
  | BIGINT
  ;

approximate_numeric_type
  : FLOAT (precision_param)?
  | FLOAT4
  | REAL
  | FLOAT8
  | DOUBLE
  | DOUBLE PRECISION
  ;

precision_param
  : LEFT_PAREN precision=NUMBER RIGHT_PAREN
  | LEFT_PAREN precision=NUMBER COMMA scale=NUMBER RIGHT_PAREN
  ;

boolean_type
  : BOOLEAN
  | BOOL
  ;

datetime_type
  : DATE
  | TIME
  | TIME WITH TIME ZONE
  | TIMETZ
  | TIMESTAMP
  | TIMESTAMP WITH TIME ZONE
  | TIMESTAMPTZ
  ;

bit_type
  : BIT type_length?
  | VARBIT type_length?
  | BIT VARYING type_length?
  ;

binary_type
  : BINARY type_length?
  | BINARY VARYING type_length?
  | VARBINARY type_length?
  ;

/*
===============================================================================
  SQL statement (Start Symbol)
===============================================================================
*/
sql
  : statement EOF
  ;

statement
  : data_statement
  | data_change_statement
  | schema_statement
  | index_statement
  ;

data_statement
  : query_expression
  ;

data_change_statement
  : insert_statement
  ;

schema_statement
  : create_table_statement
  | drop_table_statement
  ;

index_statement
  : CREATE (u=UNIQUE)? INDEX n=Identifier ON t=table_name (m=method_specifier)?
    LEFT_PAREN s=sort_specifier_list RIGHT_PAREN p=param_clause?
  ;

create_table_statement
  : CREATE EXTERNAL TABLE table_name table_elements USING file_type=Identifier
    (param_clause)? (LOCATION path=Character_String_Literal)
  | CREATE TABLE table_name (table_elements)? (USING file_type=Identifier)?
    (param_clause)? (AS query_expression)?
  ;

table_elements
  : LEFT_PAREN field_element (COMMA field_element)* RIGHT_PAREN
  ;

field_element
  : name=Identifier field_type
  ;

field_type
  : data_type
  ;

param_clause
  : WITH LEFT_PAREN param (COMMA param)* RIGHT_PAREN
  ;

param
  : key=Character_String_Literal EQUAL value=numeric_value_expression
  ;

method_specifier
  : USING m=Identifier
  ;

drop_table_statement
  : DROP TABLE table_name
  ;

/*
===============================================================================
  <insert stmt>
===============================================================================
*/

insert_statement
  : INSERT INTO table_name (LEFT_PAREN column_name_list RIGHT_PAREN)? VALUES array
  ;

/*
===============================================================================
  <query_expression>
===============================================================================
*/
query_expression
  : query_expression_body
  ;

query_expression_body
  : non_join_query_expression
  | joined_table
  ;

non_join_query_expression
  : (non_join_query_term
  | joined_table (UNION | EXCEPT) (ALL|DISTINCT)? query_term)
    ((UNION | EXCEPT) (ALL|DISTINCT)? query_term)*
  ;

query_term
  : non_join_query_term
  | joined_table
  ;

non_join_query_term
  : ( non_join_query_primary
  | joined_table INTERSECT (ALL|DISTINCT)? query_primary)
    (INTERSECT (ALL|DISTINCT)? query_primary)*
  ;

query_primary
  : non_join_query_primary
  | joined_table
  ;

non_join_query_primary
  : simple_table
  | LEFT_PAREN non_join_query_expression RIGHT_PAREN
  ;

simple_table
  : query_specification
  | explicit_table
  ;

explicit_table
  : TABLE table_or_query_name 
  ;

table_or_query_name
  : table_name
  | Identifier
  ;

table_name
  : Identifier  ( DOT  Identifier (  DOT Identifier )? )?
  ;

query_specification
  : SELECT set_qualifier? select_list table_expression?
  ;

select_list
  : MULTIPLY
  | derived_column (COMMA derived_column)*
  ;

set_qualifier
  : DISTINCT
  | ALL
  ;

derived_column
  : boolean_value_expression as_clause?
  ;

column_reference
  : (tb_name=Identifier DOT)? name=Identifier
  ;

as_clause
  : (AS)? Identifier
  ;

column_reference_list
  : column_reference (COMMA column_reference)*
  ;

/*
===============================================================================
  <table expression>
===============================================================================
*/

table_expression
  : from_clause
    where_clause?
    groupby_clause?
    having_clause?
    orderby_clause?
    limit_clause?
  ;

/*
===============================================================================
  <from clause>
===============================================================================
*/

from_clause
  : FROM table_reference_list
  ;

table_reference_list
  :table_reference (COMMA table_reference)*
  ;

table_reference
  : joined_table
  | table_primary
  ;

joined_table
//  : table_primary (cross_join | qualified_join | natural_join | union_join)+
  : table_primary joined_table_primary+
  ;

joined_table_primary
  : CROSS JOIN right=table_primary
  | (t=join_type)? JOIN right=table_primary s=join_specification
  | NATURAL (t=join_type)? JOIN right=table_primary
  | UNION JOIN right=table_primary
  ;

cross_join
  : CROSS JOIN r=table_primary
  ;

qualified_join
  : (t=join_type)? JOIN r=table_primary s=join_specification
  ;

natural_join
  : NATURAL (t=join_type)? JOIN r=table_primary
  ;

union_join
  : UNION JOIN r=table_primary
  ;

join_type
  : INNER
  | t=outer_join_type
  ;

outer_join_type
  : outer_join_type_part2 OUTER?
  ;

outer_join_type_part2
  : LEFT
  | RIGHT
  | FULL
  ;

join_specification
  : join_condition
  | named_columns_join
  ;

join_condition
  : ON search_condition
  ;

named_columns_join
  : USING LEFT_PAREN f=column_reference_list RIGHT_PAREN
  ;

table_primary
  : table_or_query_name ((AS)? alias=Identifier)? (LEFT_PAREN column_name_list RIGHT_PAREN)?
  | derived_table (AS)? name=Identifier (LEFT_PAREN column_name_list RIGHT_PAREN)?
  ;

column_name_list
  :  Identifier  ( COMMA Identifier  )*
  ;

derived_table
  : table_subquery
  ;

where_clause
  : WHERE search_condition
  ;

search_condition
  : boolean_value_expression
  ;

/*
==============================================================================================
  <subquery>

  Specify a scalar value, a row, or a table derived from a query_expression .
==============================================================================================
*/

scalar_subquery
  :  subquery
  ;

row_subquery
  :  subquery
  ;

table_subquery
  : subquery
  ;

subquery
	:  LEFT_PAREN query_expression RIGHT_PAREN
	;

/*
===============================================================================
  <routine invocation>

  Invoke an SQL-invoked routine.
===============================================================================
*/

routine_invocation
  : COUNT LEFT_PAREN sql_argument_list RIGHT_PAREN
  | COUNT LEFT_PAREN MULTIPLY RIGHT_PAREN
  | Identifier LEFT_PAREN sql_argument_list? RIGHT_PAREN
  ;

sql_argument_list
  : boolean_value_expression (COMMA boolean_value_expression)*
  ;

/*
===============================================================================
  <groupby clause>
===============================================================================
*/
groupby_clause
  : GROUP BY g=grouping_element_list
  ;

grouping_element_list
  : grouping_element (COMMA grouping_element)*
  ;

grouping_element
  : ordinary_grouping_set
  | rollup_list
  | cube_list
  | empty_grouping_set
  ;

ordinary_grouping_set
  : column_reference_list
  | LEFT_PAREN column_reference_list RIGHT_PAREN
  ;

rollup_list
  : ROLLUP LEFT_PAREN c=ordinary_grouping_set RIGHT_PAREN
  ;

cube_list
  : CUBE LEFT_PAREN c=ordinary_grouping_set RIGHT_PAREN
  ;

empty_grouping_set
  : LEFT_PAREN RIGHT_PAREN
  ;

having_clause
  : HAVING boolean_value_expression
  ;

/*
===============================================================================
  <orderby clause>

  Specify a comparison of two row values.
===============================================================================
*/

orderby_clause
  : ORDER BY sort_specifier_list
  ;

sort_specifier_list
  : sort_specifier (COMMA sort_specifier)*
  ;

sort_specifier
  : column=column_reference order=order_specification? null_order=null_ordering?
  ;

order_specification
  : ASC
  | DESC
  ;

limit_clause
  : LIMIT e=numeric_value_expression
  ;

null_ordering
  : NULL FIRST
  | NULL LAST
  ;

/*
===============================================================================
  <boolean value expression>
===============================================================================
*/

boolean_value_expression
  : and_predicate (OR and_predicate)*
  ;

and_predicate
  : boolean_factor (AND boolean_factor)*
  ;

boolean_factor
  : boolean_test
  | NOT boolean_test
  ;

boolean_test
  : boolean_primary is_clause?
  ;

is_clause
  : IS NOT? t=truth_value
  ;

truth_value
  : TRUE | FALSE | UNKNOWN
  ;

boolean_primary
  : predicate
  | numeric_value_expression  
  | case_expression
  | LEFT_PAREN boolean_value_expression RIGHT_PAREN
  ;

/*
===============================================================================
  <predicate>
===============================================================================
*/

predicate
  : comparison_predicate
  | in_predicate
  | like_predicate
  | null_predicate
  ;

/*
===============================================================================
  <comparison_predicate>

  Specify a comparison of two row values.
===============================================================================
*/
comparison_predicate
  : left=numeric_value_expression c=comp_op right=numeric_value_expression
  ;

comp_op
  : EQUAL
  | NOT_EQUAL
  | LTH
  | LEQ
  | GTH
  | GEQ
  ;

/*
===============================================================================
  <in_predicate>

  Specify a quantified comparison.
===============================================================================
*/

in_predicate
  : predicand=numeric_value_expression  NOT? IN in_predicate_value
  ;


in_predicate_value
  : table_subquery
  | LEFT_PAREN in_value_list  RIGHT_PAREN
  ;

in_value_list
  : numeric_value_expression  ( COMMA numeric_value_expression  )*
  ;

/*
===============================================================================
  <like_predicate>

  Specify a pattern-match comparison.
===============================================================================
*/

like_predicate
  : f=column_reference NOT? LIKE s=Character_String_Literal
  ;

/*
===============================================================================
  <null_predicate>

  Specify a test for a null value.
===============================================================================
*/

null_predicate
  : predicand=numeric_value_expression IS (n=NOT)? NULL
  ;

/*
==============================================================================================
  <quantified_comparison_predicate>

  Specify a quantified comparison.
==============================================================================================
*/

quantified_comparison_predicate
  : l=numeric_value_expression  c=comp_op q=quantifier s=table_subquery
  ;

quantifier : all  | some ;

all : ALL;

some : SOME | ANY;

/*
==============================================================================================
  <exists_predicate>

  Specify a test for a non_empty set.
==============================================================================================
*/

exists_predicate
  : EXISTS s=table_subquery
  ;


/*
==============================================================================================
  unique_predicate

  Specify a test for the absence of duplicate rows
==============================================================================================
*/

unique_predicate
  : UNIQUE s=table_subquery
  ;

/*
===============================================================================
  <numeric_value_expression>

  Specify a comparison of two row values.
===============================================================================
*/

numeric_value_expression
  : left=term ((PLUS|MINUS) right=term)*
  ;

term
  : left=numeric_primary ((MULTIPLY|DIVIDE|MODULAR) right=numeric_primary)*
  ;

array
  : LEFT_PAREN numeric_value_expression (COMMA numeric_value_expression )* RIGHT_PAREN
  ;

numeric_primary
  : literal
  | column_reference  
  | routine_invocation
  | scalar_subquery
  | LEFT_PAREN numeric_value_expression RIGHT_PAREN
  ;

literal
  : string_value_expr
  | signed_numerical_literal
  | NULL
  ;

string_value_expr
  : Character_String_Literal
  ;

signed_numerical_literal
  : sign? unsigned_numerical_literal
  ;

unsigned_numerical_literal
  : NUMBER
  | REAL_NUMBER
  ;

sign
  : PLUS | MINUS
  ;

/*
===============================================================================
  case_expression
===============================================================================
*/

case_expression
  : case_specification
  ;

case_abbreviation
  : NULLIF LEFT_PAREN numeric_value_expression COMMA boolean_value_expression  RIGHT_PAREN
  | COALESCE LEFT_PAREN numeric_value_expression ( COMMA boolean_value_expression  )+ RIGHT_PAREN
  ;

case_specification
  : simple_case
  | searched_case
  ;

simple_case
  : CASE boolean_value_expression ( simple_when_clause )+ ( else_clause  )? END
  ;

searched_case
  : CASE (searched_when_clause)+ (else_clause)? END
  ;

simple_when_clause : WHEN numeric_value_expression THEN result ;

searched_when_clause
  : WHEN c=search_condition THEN r=result
  ;

else_clause
  : ELSE r=result
  ;

result
  : numeric_value_expression | NULL
  ;