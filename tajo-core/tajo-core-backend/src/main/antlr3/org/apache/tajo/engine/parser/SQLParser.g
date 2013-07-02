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
	backtrack=true;
	memoize=true;
	output=AST;
	ASTLabelType=CommonTree;
	tokenVocab=SQLLexer;
}

tokens {
  ALL;
  COLUMN;
  COUNT_VAL;
  COUNT_ROWS;
  CREATE_INDEX;
  CREATE_TABLE;
  DROP_TABLE;
  DESC_TABLE;
  EMPTY_GROUPING_SET;
  FIELD_NAME;
  FIELD_DEF;
  FUNCTION;
  FUNC_ARGS;
  GROUP_BY;
  NULL_ORDER;
  ORDER;
  ORDER_BY;
  PARAM;
  PARAMS;
  SEL_LIST;
  SET_QUALIFIER;
  SHOW_TABLE;
  SHOW_FUNCTION;
  SORT_KEY;
  SORT_SPECIFIERS;
  STORE;
  STORE_TYPE;
  TABLE_DEF;
  TARGET_FIELDS;
  VALUES;
}

@header {
package org.apache.tajo.engine.parser;

import java.util.List;
import java.util.ArrayList;
import org.apache.tajo.engine.query.exception.TQLParseError;
}

@members {
   @Override
   public void reportError(RecognitionException e) {
     throw new TQLParseError(getErrorHeader(e));
   }

   @Override
   public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
     String hdr = getErrorHeader(e);
     String msg = getErrorMessage(e, tokenNames);
     throw new TQLParseError(hdr + ":" + msg);
   }
}


/*
===============================================================================
  <data types>
===============================================================================
*/

data_type
  : boolean_type
  | bit_type
  | varbit_type
  | binary_type
  | varbinary_type
  | blob_type
  | INET4
  | character_string_type
  | datetime_type
  | numeric_type
  ;

character_string_type
  : char_type
  | varchar_type
  | nchar_type
  | nvarchar_type
  | TEXT
  ;

numeric_type
  : int1_type
  | int2_type
  | int4_type
  | int8_type
  | float4_type
  | float_type
  | float8_type
  | number_type
  ;

datetime_type
  : DATE
  | TIME
  | timetz_type
  | TIMESTAMP
  | timestamptz_type
  ;

precision_param
  : LEFT_PAREN! NUMBER RIGHT_PAREN!
  | LEFT_PAREN! NUMBER COMMA! NUMBER RIGHT_PAREN!
  ;
type_length
  : LEFT_PAREN! NUMBER RIGHT_PAREN!
  ;

boolean_type
  : BOOLEAN
  | BOOL -> BOOLEAN
  ;
bit_type
  : BIT type_length? -> BIT
  ;
varbit_type
  : VARBIT type_length? -> VARBIT
  | BIT VARYING type_length? -> VARBIT
  ;
int1_type
  : INT1
  | TINYINT -> INT1
  ;
int2_type
  : INT2
  | SMALLINT -> INT2
  ;
int4_type
  : INT4
  | INT -> INT4
  | INTEGER -> INT4
  ;
int8_type
  : INT8
  | BIGINT -> INT8
  ;
float4_type
  : FLOAT4
  | REAL -> FLOAT4
  ;
float_type : FLOAT type_length? -> ^(FLOAT type_length?);
float8_type
  : FLOAT8
  | DOUBLE -> FLOAT8
  | DOUBLE PRECISION -> FLOAT8
  ;
number_type
  : NUMERIC (precision_param)? -> NUMERIC
  | DECIMAL (precision_param)? -> NUMERIC
  ;
char_type
  : CHAR type_length? -> CHAR
  | CHARACTER type_length? -> CHAR
  ;
varchar_type
  : VARCHAR type_length? -> VARCHAR
  | CHARACTER VARYING type_length? -> VARCHAR
  ;
nchar_type
  : NCHAR type_length? -> NCHAR
  | NATIONAL CHARACTER type_length? -> NCHAR
  ;
nvarchar_type
  : NVARCHAR type_length? -> NVARCHAR
  | NATIONAL CHARACTER VARYING type_length? -> NVARCHAR
  ;
timetz_type
  : TIMETZ
  | TIME WITH TIME ZONE -> TIMETZ
  ;
timestamptz_type
  : TIMESTAMPTZ
  | TIMESTAMP WITH TIME ZONE -> TIMESTAMPTZ
  ;
binary_type
  : BINARY type_length?
  ;
varbinary_type
  : VARBINARY type_length?
  | BINARY VARYING type_length?
  ;
blob_type
  : BLOB
  | BYTEA -> BLOB
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
  | set_stmt
  ;

data_change_statement
  : insert_statement
  ;

schema_statement
  : create_table_statement
  | DROP TABLE table -> ^(DROP_TABLE table)
  ;

index_statement
  : CREATE (u=UNIQUE)? INDEX n=Identifier ON t=table (m=method_specifier)?
    LEFT_PAREN s=sort_specifier_list RIGHT_PAREN p=param_clause?
    -> ^(CREATE_INDEX $u? $m? $p? $n $t $s)
  ;

create_table_statement
  : CREATE EXTERNAL TABLE t=table def=table_elements USING f=Identifier
    p=param_clause? (LOCATION path=Character_String_Literal)
      -> ^(CREATE_TABLE $t EXTERNAL ^(TABLE_DEF $def) ^(USING $f) $p?
         ^(LOCATION $path))
  | CREATE TABLE t=table (def=table_elements)? (USING s=Identifier)?
    (p=param_clause)? (AS q=query_expression)?
      -> ^(CREATE_TABLE $t ^(TABLE_DEF $def)? ^(USING $s)? $p? ^(AS $q)?)
  ;

table_elements
  : LEFT_PAREN field_element (COMMA field_element)* RIGHT_PAREN
    -> field_element+
  ;

field_element
  : Identifier field_type -> ^(FIELD_DEF Identifier field_type)
  ;

field_type
  : data_type
  ;

/*
===============================================================================
  <insert stmt>
===============================================================================
*/

insert_statement
  : INSERT INTO table (LEFT_PAREN column_reference_list RIGHT_PAREN)? VALUES array
  -> ^(INSERT ^(TABLE table) ^(VALUES array) ^(TARGET_FIELDS column_reference_list)?)
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
  : (non_join_query_term | joined_table (UNION | EXCEPT)^ (ALL|DISTINCT)? query_term)
    ((UNION | EXCEPT)^ (ALL|DISTINCT)? query_term)*
  ;

query_term
  : non_join_query_term
  | joined_table
  ;

non_join_query_term
  : ( non_join_query_primary | joined_table INTERSECT^ (ALL|DISTINCT)? query_primary)
    (INTERSECT^ (ALL|DISTINCT)? query_primary)*
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
  options {k=1;}
  : query_specification
  ;

query_specification
  : SELECT set_qualifier? select_list from_clause? where_clause? groupby_clause? having_clause?
    orderby_clause? limit_clause?
    -> ^(SELECT from_clause? set_qualifier? select_list where_clause? groupby_clause?
    having_clause? orderby_clause? limit_clause?)
  ;

select_list
  : MULTIPLY -> ^(SEL_LIST ALL)
  | derived_column (COMMA derived_column)* -> ^(SEL_LIST derived_column+)
  ;

set_qualifier
  : DISTINCT -> ^(SET_QUALIFIER DISTINCT)
  | ALL -> ^(SET_QUALIFIER ALL)
  ;

derived_column
  : boolean_value_expression as_clause? -> ^(COLUMN boolean_value_expression as_clause?)
  ;

column_reference
	:	(t=Identifier DOT)? b=Identifier -> ^(FIELD_NAME $b $t?)
	;

as_clause
  : (AS)? column_reference
  ;

column_reference_list
	:	column_reference (COMMA column_reference)* -> column_reference+
	;

table
  : Identifier
  ;

from_clause
  : FROM^ table_reference_list
  ;

table_reference_list
  :table_reference (COMMA table_reference)* -> table_reference+
  ;

table_reference
  : table_primary
  | joined_table
  ;

joined_table
  : table_primary (cross_join | qualified_join | natural_join | union_join)+
  ;

joined_table_prim
  : cross_join
  | qualified_join
  | natural_join
  | union_join
  ;

cross_join
  : CROSS JOIN r=table_primary -> ^(JOIN CROSS $r)
  ;

qualified_join
  : (t=join_type)? JOIN r=table_primary s=join_specification -> ^(JOIN $t? $r $s)
  ;

natural_join
  : NATURAL (t=join_type)? JOIN r=table_primary -> ^(JOIN NATURAL $t? $r)
  ;

union_join
  : UNION JOIN r=table_primary -> ^(JOIN UNION $r)
  ;

join_type
  : INNER
  | t=outer_join_type OUTER -> ^(OUTER $t)
  | t=outer_join_type -> ^(OUTER $t)
  ;

outer_join_type
  : LEFT
  | RIGHT
  | FULL
  ;

join_specification
  : join_condition
  | named_columns_join
  ;

join_condition
  : ON^ search_condition
  ;

named_columns_join
  : USING LEFT_PAREN f=column_reference_list RIGHT_PAREN -> ^(USING $f)
  ;

table_primary
  : table ((AS)? a=Identifier)? -> ^(TABLE table ($a)?)
  ;

where_clause
  : WHERE^ search_condition
  ;

/*
===============================================================================
  <routine invocation>

  Invoke an SQL-invoked routine.
===============================================================================
*/

routine_invocation
	: Identifier LEFT_PAREN funcArgs? RIGHT_PAREN -> ^(FUNCTION[$Identifier.text] funcArgs?)
	| COUNT LEFT_PAREN funcArgs RIGHT_PAREN -> ^(COUNT_VAL funcArgs)
	| COUNT LEFT_PAREN MULTIPLY RIGHT_PAREN -> ^(COUNT_ROWS)
	;

funcArgs
  : boolean_value_expression (COMMA boolean_value_expression)* -> boolean_value_expression+
  ;

/*
===============================================================================
  <groupby clause>
===============================================================================
*/
groupby_clause
  : GROUP BY g=grouping_element_list -> ^(GROUP_BY $g)
  ;

grouping_element_list
  : grouping_element (COMMA grouping_element)* -> grouping_element+
  ;

grouping_element
  : ordinary_grouping_set
  | rollup_list
  | cube_list
  | empty_grouping_set
  ;

ordinary_grouping_set
  : column_reference_list
  | LEFT_PAREN! column_reference_list RIGHT_PAREN!
  ;

rollup_list
  : ROLLUP LEFT_PAREN c=ordinary_grouping_set RIGHT_PAREN -> ^(ROLLUP $c)
  ;

cube_list
  : CUBE LEFT_PAREN c=ordinary_grouping_set RIGHT_PAREN -> ^(CUBE $c)
  ;

empty_grouping_set
  : LEFT_PAREN RIGHT_PAREN -> ^(EMPTY_GROUPING_SET)
  ;

having_clause
  : HAVING^ boolean_value_expression
  ;

/*
===============================================================================
  <orderby clause>

  Specify a comparison of two row values.
===============================================================================
*/

orderby_clause
  : ORDER BY sort_specifier_list -> ^(ORDER_BY sort_specifier_list)
  ;

sort_specifier_list
  : sort_specifier (COMMA sort_specifier)* -> ^(SORT_SPECIFIERS sort_specifier+)
  ;

sort_specifier
  : fn=column_reference a=order_specification? o=null_ordering? -> ^(SORT_KEY $fn $a? $o?)
  ;

order_specification
  : ASC -> ^(ORDER ASC)
  | DESC -> ^(ORDER DESC)
  ;

limit_clause
  : LIMIT e=numeric_value_expression -> ^(LIMIT $e)
  ;

null_ordering
  : NULL FIRST -> ^(NULL_ORDER FIRST)
  | NULL LAST -> ^(NULL_ORDER LAST)
  ;

/*
===============================================================================
  <set stmt>

  Specify a comparison of two row values.
===============================================================================
*/

set_stmt
	:	SET (UNION|INTERSECT|EXCEPT) table
	;

search_condition
	:	boolean_value_expression
	;

param_clause
  : WITH LEFT_PAREN param (COMMA param)* RIGHT_PAREN -> ^(PARAMS param+)
  ;

param
  : k=Character_String_Literal EQUAL v=numeric_value_expression -> ^(PARAM $k $v)
  ;

method_specifier
  : USING m=Identifier -> ^(USING[$m.text])
  ;

/*
===============================================================================
  <boolean value expression>
===============================================================================
*/

boolean_value_expression
	:	and_predicate (OR^ and_predicate)*
	;

and_predicate
  :	boolean_factor (AND^ boolean_factor)*
	;

boolean_factor
  : boolean_test
  | NOT boolean_test -> ^(NOT boolean_test)
  ;

boolean_test
  : boolean_primary is_clause?
  ;

is_clause
  : IS NOT? t=truth_value -> ^(IS NOT? $t)
  ;

truth_value
  : TRUE | FALSE | UNKNOWN
  ;

boolean_primary
  : predicate
  | numeric_value_expression
  | LEFT_PAREN! boolean_value_expression RIGHT_PAREN!
  | case_expression
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
  options{k=1;}
	:	l=numeric_value_expression c=comp_op r=numeric_value_expression -> ^($c $l $r)
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

in_predicate : v=numeric_value_expression  NOT? IN a=in_predicate_value -> ^(IN $v $a NOT?);

in_predicate_value
  : LEFT_PAREN! in_value_list  RIGHT_PAREN!
	;

in_value_list
  : numeric_value_expression  ( COMMA numeric_value_expression  )* -> numeric_value_expression+;

/*
===============================================================================
  <like_predicate>

  Specify a pattern-match comparison.
===============================================================================
*/

like_predicate
  : f=column_reference NOT? LIKE s=Character_String_Literal -> ^(LIKE NOT? $f $s)
  ;

/*
===============================================================================
  <null_predicate>

  Specify a test for a null value.
===============================================================================
*/

null_predicate
  : f=numeric_value_expression IS (n=NOT)? NULL -> ^(IS $f NULL $n?)
  ;

/*
===============================================================================
  <numeric_value_expression>

  Specify a comparison of two row values.
===============================================================================
*/

numeric_value_expression
	:	term ((PLUS|MINUS)^ term)*
	;

term
  :	numeric_primary ((MULTIPLY|DIVIDE|MODULAR)^ numeric_primary)*
	;

array
  : LEFT_PAREN literal (COMMA literal )* RIGHT_PAREN -> literal+
  ;

numeric_primary
  :	literal
	| column_reference
	|	LEFT_PAREN! numeric_value_expression RIGHT_PAREN!
	| routine_invocation
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
  : CASE numeric_value_expression ( simple_when_clause )+ ( else_clause  )? END
  ;

searched_case
  : CASE^ (searched_when_clause)+ (else_clause)? END!
  ;

simple_when_clause : WHEN numeric_value_expression THEN result ;

searched_when_clause
  : WHEN c=search_condition THEN r=result -> ^(WHEN $c $r)
  ;

else_clause
  : ELSE r=result -> ^(ELSE $r)
  ;

result
  : numeric_value_expression | NULL
  ;