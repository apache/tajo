/*
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

/*
==============================================================================================
 Based on BNF Grammer for ISO/IEC 9075-2:2003 Database Language SQL (SQL-2003) SQL/Foundation
 Refer to http://savage.net.au/SQL/sql-2003-2.bnf.html
==============================================================================================
*/

parser grammar SQLParser;

options {
	language=Java;
	tokenVocab=SQLLexer;
	backtrack=true;
	memoize=true;
	output=AST;
	ASTLabelType=CommonTree;
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
  SESSION_CLEAR;
  SET_QUALIFIER;
  SHOW_TABLE;
  SHOW_FUNCTION;
  SORT_KEY;
  SORT_SPECIFIERS;
  STORE;
  STORE_TYPE;
  SUBQUERY;
  TABLE_DEF;
  TARGET_FIELDS;
}

@header {
package tajo.frontend.sql;

import java.util.List;
import java.util.ArrayList;
import tajo.frontend.sql.SQLParseError;
}

@members {
   @Override
   public void reportError(RecognitionException e) {
     throw new SQLParseError(getErrorHeader(e));
   }

   @Override
   public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
     String hdr = getErrorHeader(e);
     String msg = getErrorMessage(e, tokenNames);
     throw new SQLParseError(hdr + ":" + msg);
   }
}

// SQL Main
sql
  : statement EOF
  ;

statement
  : sessionStatement
  | dataStatement
  | dataChangeStatement
  | schemaStatement
  | indexStatement
  ;

sessionStatement
  : SESSION CLEAR -> ^(SESSION_CLEAR)
  ;
  
dataStatement
  : query_expression
  | set_stmt
  ;
  
dataChangeStatement
  : insertStmt
  ;
  
schemaStatement
  : createTableStatement
  | DROP TABLE table_name -> ^(DROP_TABLE table_name)
  ;
  
indexStatement
  : CREATE (u=UNIQUE)? INDEX n=identifier ON t=table_name (m=method_specifier)?
    Left_Paren s=sort_specifier_list Right_Paren p=param_clause?
    -> ^(CREATE_INDEX $u? $m? $p? $n $t $s)
  ;

createTableStatement
  : CREATE EXTERNAL TABLE t=table_name def=tableElements USING f=identifier p=param_clause? (LOCATION path=Character_String_Literal)
      -> ^(CREATE_TABLE $t EXTERNAL ^(TABLE_DEF $def) ^(USING $f) $p? ^(LOCATION $path))
  | CREATE TABLE t=table_name (def=tableElements)? (USING f=identifier)? (p=param_clause)? (AS q=query_expression)?
      -> ^(CREATE_TABLE $t ^(TABLE_DEF $def)? ^(USING $f)? $p? ^(AS $q)?)
  ;

tableElements
  : Left_Paren fieldElement (Comma fieldElement)* Right_Paren -> fieldElement+
  ;

fieldElement
  : identifier fieldType -> ^(FIELD_DEF identifier fieldType)
  ;

fieldType
  : BOOL
  | BYTE
  | CHAR
  | INT
  | LONG
  | FLOAT
  | DOUBLE
  | TEXT
  | DATE
  | BYTES
  | IPv4
  | VARCHAR
  | STRING
  ;

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
  | Left_Paren non_join_query_expression Right_Paren
  ;

simple_table
options {k=1;}
  : query_specification
  ;

query_specification
  : SELECT setQualifier? selectList from_clause? where_clause? groupby_clause? having_clause?
    orderby_clause? limit_clause?

    -> ^(SELECT from_clause? setQualifier? selectList where_clause? groupby_clause? having_clause?
    orderby_clause? limit_clause?)
  ;

insertStmt
  : INSERT INTO table_name (Left_Paren column_reference Right_Paren)? VALUES array
    -> ^(INSERT ^(TABLE table_name) ^(VALUES array) ^(TARGET_FIELDS column_reference)?)
  ;

selectList
  : Asterisk -> ^(SEL_LIST ALL)
  | derivedColumn (Comma derivedColumn)* -> ^(SEL_LIST derivedColumn+)
  ;

setQualifier
  : DISTINCT -> ^(SET_QUALIFIER DISTINCT)
  | ALL -> ^(SET_QUALIFIER ALL)
  ;

derivedColumn
  : value_expression asClause? -> ^(COLUMN value_expression asClause?)
  ;

fieldName
	:	(t=identifier Period)? b=identifier -> ^(FIELD_NAME $b $t?)
	;

asClause
  : (AS)? fieldName
  ;

column_reference
	:	fieldName (Comma fieldName)* -> fieldName+
	;

funcCall
	: identifier Left_Paren Right_Paren -> ^(FUNCTION[$identifier.text])
	| identifier Left_Paren funcArgs? Right_Paren -> ^(FUNCTION[$identifier.text] funcArgs?)
	| COUNT Left_Paren funcArgs Right_Paren -> ^(COUNT_VAL funcArgs)
	| COUNT Left_Paren Asterisk Right_Paren -> ^(COUNT_ROWS)
	;

funcArgs
  : value_expression (Comma value_expression)* -> value_expression+
  ;

from_clause
  : FROM^ table_reference_list
  ;

table_reference_list
  : table_reference (Comma table_reference)* -> table_reference+
  ;

table_reference
  : joined_table
  | table_primary
  ;

joined_table
  : table_primary (
    cross_join
  | qualified_join
  | natural_join
  | union_join )+
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
  options {k=1;}
  : INNER
  | t=outer_join_type -> ^(OUTER $t)
  ;

outer_join_type
  : outer_join_type_part2 (OUTER?)!
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
  : ON^ search_condition
  ;

named_columns_join
  : USING Left_Paren f=column_reference Right_Paren -> ^(USING $f)
  ;

table_primary
  : table_name ((AS)? a=identifier)? -> ^(TABLE table_name ($a)?)
  | t=derived_table (AS)? a=identifier-> ^(SUBQUERY $t $a)
  ;

derived_table
  : table_subquery
  ;



where_clause
  : WHERE^ search_condition
  ;

groupby_clause
  : GROUP BY g=grouping_element_list -> ^(GROUP_BY $g)
  ;

grouping_element_list
  : grouping_element (Comma grouping_element)* -> grouping_element+
  ;

grouping_element
  : ordinary_grouping_set
  | rollup_list
  | cube_list
  | empty_grouping_set
  ;

ordinary_grouping_set
  : column_reference
  | Left_Paren! column_reference Right_Paren!
  ;

rollup_list
  : ROLLUP Left_Paren c=ordinary_grouping_set Right_Paren -> ^(ROLLUP $c)
  ;

cube_list
  : CUBE Left_Paren c=ordinary_grouping_set Right_Paren -> ^(CUBE $c)
  ;

empty_grouping_set
  : Left_Paren Right_Paren -> ^(EMPTY_GROUPING_SET)
  ;

having_clause
  : HAVING^ boolean_value_expression
  ;

orderby_clause
  : ORDER BY sort_specifier_list -> ^(ORDER_BY sort_specifier_list)
  ;

sort_specifier_list
  : sort_specifier (Comma sort_specifier)* -> ^(SORT_SPECIFIERS sort_specifier+)
  ;

sort_specifier
  : fn=fieldName a=order_specification? o=null_ordering? -> ^(SORT_KEY $fn $a? $o?)
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

set_stmt
	:	SET (UNION|INTERSECT|DIFF) table_name
	;

search_condition
	:	boolean_value_expression
	;

param_clause
  : WITH Left_Paren param (Comma param)* Right_Paren -> ^(PARAMS param+)
  ;

param
  : k=Character_String_Literal Equals_Operator v=boolean_value_expression -> ^(PARAM $k $v)
  ;

method_specifier
  : USING m=identifier -> ^(USING[$m.text])
  ;

/*
==============================================================================================
  5.3 <literal> (p143)

  Progress: Almost done

  TODO:
    * literal should be fixed
    * NULL should be removed
==============================================================================================
*/

literal
  options{k=1;}
  : general_literal
  ;

unsigned_literal
  : unsigned_numerical_literal
  | general_literal
  ;

general_literal
  options{k=1;}
  : unsigned_numerical_literal
  | Character_String_Literal
  |	National_Character_String_Literal
  | Bit_String_Literal
  | Hex_String_Literal
  |	boolean_literal
  | day_time_literal
  | interval_literal
  | NULL
  ;

boolean_literal
  : TRUE | FALSE | UNKNOWN
  ;

signed_numerical_literal
  : Signed_Integer
  | Signed_Large_Integer
  | Signed_Float
  ;

unsigned_numerical_literal
  : Unsigned_Integer
  | Unsigned_Large_Integer
  | Unsigned_Float
  ;


/*
==============================================================================================
  5.4 Names and identifiers (p151)
==============================================================================================
*/

identifier                  : Regular_Identifier | Unicode_Identifier;
sql_language_identifier     : Regular_Identifier;
user_identifier             : Regular_Identifier;
schema_name                 : identifier  ( Period  identifier )? ;
fully_qualified_identifier  : identifier  ( Period  identifier (  Period  identifier )? )?;


table_name
  : identifier  ( Period  identifier (  Period  identifier )? )?
  ;

identifier_chain            : identifier  ( Period identifier  )*;

/*
==============================================================================================
  6.25 value_expression  (p236)

  Specify a value.

  TODO:
    * collection_value_expression
    * collection_value_constructor
==============================================================================================
*/


value_expression
options{k=2;}
  : common_value_expression
  | boolean_value_expression
  | row_value_expression
  ;

common_value_expression
options {k=1;}
  : numeric_value_expression
  | string_value_expression
  ;

user_defined_type_value_expression
  options {k=1;}
  : value_expression_primary
  ;

reference_value_expression
  options {k=1;}
  : value_expression_primary
  ;

/*
==============================================================================================
  6.26 numeric_value_expression  (p240)

  Specify a numeric value.
==============================================================================================
*/

numeric_value_expression
  options {k=1;}
	:	term ((Plus_Sign|Minus_Sign)^ term)*
	;

term
  options {k=1;}
  :	factor ((Asterisk|Slash|Percent)^ factor)*
	;

factor
  options {k=1;}
  : (sign)? numeric_primary
	;

numeric_primary
  options{k=1;}
  : value_expression_primary
  ;

array
  : Left_Paren literal (Comma literal )* Right_Paren -> literal+
  ;

/*
==============================================================================================
  6.28 string_value_expression  (p251)

  Specify a character string value or a binary string value.
  TODO:
    * collate_clause
==============================================================================================
*/

string_value_expression
  options{k=1;}
  : character_value_expression; //  | blob_value_expression ;

character_value_expression
  options{k=1;}
  : character_factor ( Concatenation_Operator  character_factor )*;

character_factor
  options{k=1;}
  :   character_primary
  ;

character_primary
  options{k=1;}
  : value_expression_primary
  ;

/*
==============================================================================================
 6.3 value expression primary

==============================================================================================
*/

value_expression_primary
  : parenthesized_value_expression
  | nonparenthesized_value_expression_primary
  ;

parenthesized_value_expression
  options{k=1;}
  :  Left_Paren! value_expression  Right_Paren!
  ;

/*
==============================================================================================
 6.34 boolean_value_expression  (p277)
 Progress: Completed
==============================================================================================
*/

boolean_value_expression
  options{k=1;}
	:	boolean_term (OR^ boolean_term)*
	;

boolean_term
  options{k=1;}
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
  options {k=1;}
  : predicate
  | boolean_predicand
  ;

boolean_predicand
  options {k=1;}
  : parenthesized_boolean_value_expression
  | nonparenthesized_value_expression_primary
  ;

parenthesized_boolean_value_expression
  options {k=1;}
  :  Left_Paren! boolean_value_expression  Right_Paren!
  ;

nonparenthesized_value_expression_primary
  : fieldName
  | unsigned_value_specification
  | q=scalar_subquery -> ^(SUBQUERY $q)
  | case_expression
  | funcCall
  | NULL
  ;


predicate
  options {k=1;}
  : comparison_predicate
  | in_predicate
  | like_predicate
  | null_predicate
  ;

in_predicate
	:	numeric_value_expression NOT? IN array -> ^(IN numeric_value_expression array NOT?)
	;

like_predicate
  : f=fieldName NOT? LIKE s=Character_String_Literal -> ^(LIKE NOT? $f $s)
  ;

null_predicate
  : f=numeric_value_expression IS (n=NOT)? NULL -> ^(IS $f NULL $n?)
  ;

/*
==============================================================================================
  6.4 <value specification> and <target specification> (p176)
==============================================================================================
*/

value_specification
    options{k=1;}
    :   literal
    |   general_value_specification
    ;

unsigned_value_specification
    options{k=1;}
    :   unsigned_literal
    |   general_value_specification ;

general_value_specification
    : identifier_chain
    ;

/*
==============================================================================================
  6.11 case_expression  (p197)

  Progress: Almost done
  TODO:
    * when_operand
    * overlaps_predicate_part_1
==============================================================================================
*/

case_expression
  : case_specification
  ;

case_abbreviation
  : NULLIF Left_Paren value_expression  Comma value_expression  Right_Paren
  | COALESCE Left_Paren value_expression  ( Comma value_expression  )+ Right_Paren
  ;

case_specification
  : simple_case
  | searched_case
  ;

simple_case
  : CASE case_operand  ( simple_when_clause )+ ( else_clause  )? END
  ;

searched_case
  : CASE^ (searched_when_clause)+ (else_clause)? END!
  ;

simple_when_clause   :  WHEN when_operand  THEN result ;

searched_when_clause
  : WHEN c=search_condition THEN r=result -> ^(WHEN $c $r)
  ;

else_clause
  : ELSE r=result -> ^(ELSE $r)
  ;

case_operand
  options {k=1;}
  : row_value_predicand
//| overlaps_predicate_part_1
  ;

when_operand
  options {k=1;}
  : row_value_predicand
  ;

result
  : result_expression | NULL
  ;

result_expression : value_expression;

/*
==============================================================================================
  7 Query expressions
    7.1 <row value constructor> (p293)

  Specify a value or list of values to be constructed into a row or partial row.

  TODO
    * explicit_row_value_constructor
    * Others
==============================================================================================
*/

row_value_constructor  :
        common_value_expression
    |   boolean_value_expression
//  |   explicit_row_value_constructor
    ;

row_value_constructor_predicand
    options {k=1;}
    :   common_value_expression
    |   boolean_predicand
//  |   explicit_row_value_constructor
    ;

/*
==============================================================================================
  7.15 <subquery> (p368)

  Specify a scalar value, a row, or a table derived from a query_expression .
==============================================================================================
*/

scalar_subquery
	options {k=1;}
	:  subquery
	;

row_subquery
	options {k=1;}
	:  subquery
	;

table_subquery
  : subquery
  ;

subquery
	options {k=1;}
	:  Left_Paren! query_expression Right_Paren!
	;

/*
==============================================================================================
  7.2 row_value_expression  (p296)

  Specify a row value.

  TODO
    * explicit_row_value_constructor
==============================================================================================
*/

row_value_expression
    options {k=1;}
    :   row_value_special_case
  //|   explicit_row_value_constructor
    ;

table_row_value_expression
    options {k=1;}
    :   row_value_special_case
    |   row_value_constructor
    ;

contextually_typed_row_value_expression
    options {k=1;}
    :   row_value_special_case
//  |   contextually_typed_row_value_constructor
    ;

row_value_predicand
    options {k=1;}
    : row_value_constructor_predicand
    | row_value_special_case
    ;

row_value_special_case
    options {k=1;}
    :  nonparenthesized_value_expression_primary
    ;

/*
==============================================================================================
  8.2 <comparison predicate> (p373)

  Progress: completed
==============================================================================================
*/

comparison_predicate
  options{k=1;}
	:	l=row_value_predicand c=comp_op r=row_value_predicand -> ^($c $l $r)
	;

comp_op
  :	Equals_Operator
  |	Not_Equals_Operator
  |	Less_Than_Operator
  |	Less_Or_Equals_Operator
  |	Greater_Than_Operator
  |	Greater_Or_Equals_Operator
  ;

/*
==============================================================================================
  The Quote symbol  rule consists of two immediately adjacent Quote marks with no spaces.
  As usual, this would be best handled in the lexical analyzer, not in the grammar.
==============================================================================================
*/

quote_symbol : Quote Quote;
sign : Plus_Sign | Minus_Sign;

datetime_literal
  : date_literal  | time_literal  | timestamp_literal
  ;
date_literal
  : DATE Quote years_value  Minus_Sign months_value Minus_Sign days_value  Quote
  ;
time_literal
  : TIME Quote hours_value  Colon minutes_value  Colon seconds_value
    (  ( Plus_Sign | Minus_Sign )  hours_value  Colon minutes_value )?  Quote
  ;
timestamp_literal
  : TIMESTAMP Quote  years_value  Minus_Sign months_value Minus_Sign days_value
    Space hours_value  Colon minutes_value  Colon seconds_value
    (  ( Plus_Sign | Minus_Sign )  hours_value  Colon minutes_value )?
    Quote
  ;
interval_literal
  : INTERVAL
    Quote ( Plus_Sign | Minus_Sign )? ( year_month_literal | day_time_literal ) Quote
  ;

year_month_literal : years_value  | years_value  Minus_Sign months_value ;
day_time_literal   : day_time_interval  | time_interval ;
day_time_interval
  : DAYS days_value  Space hours_value  ( Colon minutes_value  ( Colon seconds_value  )? )?
  ;
time_interval      : HOURS hours_value  ( Colon minutes_value   ( Colon seconds_value )? )?;

years_value        : Unsigned_Integer ;
months_value       : Unsigned_Integer ;
days_value         : Unsigned_Integer ;
hours_value        : Unsigned_Integer ;
minutes_value      : Unsigned_Integer ;
seconds_value      : Unsigned_Integer  ( Period ( Unsigned_Integer  )? )?;
