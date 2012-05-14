grammar NQL;

options {
	language=Java;
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
  CROSS_JOIN;
  DROP_TABLE;
  DESC_TABLE;
  EMPTY_GROUPING_SET;
  FIELD_NAME;  
  FIELD_DEF;
  FUNCTION;    
  FUNC_ARGS;
  GROUP_BY;  
  INNER_JOIN;
  NATURAL_JOIN;
  NULL_ORDER;  
  OUTER_JOIN;  
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
  TABLE_DEF;
  TARGET_FIELDS;  
  VALUES;
}

@header {
package nta.engine.parser;

import java.util.List;
import java.util.ArrayList;
}

@lexer::header {
package nta.engine.parser;
}

@lexer::members {
   @Override
   public void reportError(RecognitionException e) {
    System.out.println(e.getMessage());
   }
}

@members {
   @Override
   public void reportError(RecognitionException e) {
   }
}

// NQL Main
statement
  : 'session' 'clear' -> ^(SESSION_CLEAR)
  | controlStatement
  | dataStatement 
  | dataChangeStatement
  | schemaStatement
  | indexStatement
  ;
  
controlStatement
  : '\\' 't' (table)? -> ^(SHOW_TABLE table?)
  | '\\' 'd' table -> ^(DESC_TABLE table)
  | '\\' 'f' -> ^(SHOW_FUNCTION)
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
  | DROP TABLE table -> ^(DROP_TABLE table)
  ;
  
indexStatement
  : CREATE (u=UNIQUE)? INDEX n=ID ON t=table (m=method_specifier)? LEFT_PAREN s=sort_specifier_list RIGHT_PAREN p=param_clause?-> ^(CREATE_INDEX $u? $m? $p? $n $t $s)
  ;
  
createTableStatement
  : CREATE TABLE t=table AS query_expression -> ^(CREATE_TABLE $t query_expression)
  | t=table ASSIGN query_expression -> ^(CREATE_TABLE $t query_expression)
  | CREATE TABLE t=table tableElements USING s=ID LOCATION path=STRING p=param_clause? -> ^(CREATE_TABLE $t ^(TABLE_DEF tableElements) $s $path $p?)
  ;
  
tableElements
  : LEFT_PAREN fieldElement (COMMA fieldElement)* RIGHT_PAREN -> fieldElement+
  ;
  
fieldElement
  : ID fieldType -> ^(FIELD_DEF ID fieldType)
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
  ;

query_expression
  : query_expression_body
  ;
  
query_expression_body
  : non_join_query_expression
  | joined_table
  ;
  
non_join_query_expression
  : (non_join_query_term | joined_table (UNION | EXCEPT)^ (ALL|DISTINCT)? query_term) ((UNION | EXCEPT)^ (ALL|DISTINCT)? query_term)*  
  ;
  
query_term
  : non_join_query_term
  | joined_table
  ;
  
non_join_query_term
  : ( non_join_query_primary | joined_table INTERSECT^ (ALL|DISTINCT)? query_primary) (INTERSECT^ (ALL|DISTINCT)? query_primary)*
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
  : SELECT setQualifier? selectList from_clause? where_clause? groupby_clause? having_clause? orderby_clause?
  -> ^(SELECT from_clause? setQualifier? selectList where_clause? groupby_clause? having_clause? orderby_clause?)
  ;
  
insertStmt
  : INSERT 'into' table (LEFT_PAREN column_reference RIGHT_PAREN)? 'values' array
  -> ^(INSERT ^(TABLE table) ^(VALUES array) ^(TARGET_FIELDS column_reference)?)
  ;
	
selectList
  : MULTIPLY -> ^(SEL_LIST ALL)
  | derivedColumn (COMMA derivedColumn)* -> ^(SEL_LIST derivedColumn+)
  ;
  
setQualifier
  : DISTINCT -> ^(SET_QUALIFIER DISTINCT) 
  | ALL -> ^(SET_QUALIFIER ALL)
  ;
  
derivedColumn
  : bool_expr asClause? -> ^(COLUMN bool_expr asClause?)
  ;
	
fieldName
	:	(t=ID DOT)? b=ID -> ^(FIELD_NAME $b $t?)
	;
	
asClause
  : (AS)? fieldName
  ;
	
column_reference  
	:	fieldName (COMMA fieldName)* -> fieldName+
	;
  
table  
  : ID
  ;

// TODO - to be improved
funcCall
	:	ID LEFT_PAREN funcArgs? RIGHT_PAREN -> ^(FUNCTION[$ID.text] funcArgs?)
	| COUNT LEFT_PAREN funcArgs RIGHT_PAREN -> ^(COUNT_VAL funcArgs)
	| COUNT LEFT_PAREN MULTIPLY RIGHT_PAREN -> ^(COUNT_ROWS)
	;
	
funcArgs
  : bool_expr (COMMA bool_expr)* -> bool_expr+ 
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
  : cross_join 
  | l=table_primary JOIN r=table_reference s=join_specification -> ^(JOIN INNER_JOIN $l $r $s)
  | l=table_primary t=join_type JOIN r=table_reference s=join_specification -> ^(JOIN $t $l $r $s)  
  | natural_join
  ;
  
cross_join
  : l=table_primary CROSS JOIN r=table_reference -> ^(JOIN CROSS_JOIN $l $r)
  ;
  
natural_join
  : l=table_primary NATURAL t=join_type? JOIN r=table_reference -> ^(JOIN NATURAL_JOIN $t? $l $r)
  ;

join_type
  : INNER -> ^(INNER_JOIN)
  | t=outer_join_type? OUTER -> ^(OUTER_JOIN $t)
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
  : USING LEFT_PAREN f=column_reference RIGHT_PAREN -> ^(USING $f)
  ;
  
table_primary
  : table ((AS)? a=ID)? -> ^(TABLE table ($a)?)
  ;

where_clause
  : WHERE^ search_condition
  ;
  
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
  : column_reference
  | LEFT_PAREN! column_reference RIGHT_PAREN!
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
  : HAVING^ bool_expr
  ;
  
orderby_clause
  : ORDER BY sort_specifier_list -> ^(ORDER_BY sort_specifier_list)
  ;
  
sort_specifier_list
  : sort_specifier (COMMA sort_specifier)* -> ^(SORT_SPECIFIERS sort_specifier+)
  ;
  
sort_specifier
  : fn=fieldName a=order_specification? o=null_ordering? -> ^(SORT_KEY $fn $a? $o?)
  ;
  
order_specification
  : ASC -> ^(ORDER ASC)
  | DESC -> ^(ORDER DESC)
  ;
  
null_ordering
  : NULL FIRST -> ^(NULL_ORDER FIRST)
  | NULL LAST -> ^(NULL_ORDER LAST)
  ;
	
set_stmt
	:	'set' ('union'|'intersect'|'diff') table
	;
	
search_condition
	:	bool_expr
	; 
	
param_clause
  : WITH LEFT_PAREN param (COMMA param)* RIGHT_PAREN -> ^(PARAMS param+)
  ;
  
param
  : k=STRING EQUAL v=bool_expr -> ^(PARAM $k $v)
  ;
  
method_specifier
  : USING m=ID -> ^(USING[$m.text])
  ;

bool_expr
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
  | expr
  | LEFT_PAREN! bool_expr RIGHT_PAREN!
  ;

predicate 
  : comparison_predicate
  | in_predicate
  | like_predicate
  ;

in_predicate
	:	expr NOT? IN array -> ^(IN expr array NOT?)
	;
	
like_predicate
  : f=fieldName NOT? LIKE s=string_value_expr -> ^(LIKE NOT? $f $s)
  ;
	
comparison_predicate
	:	expr EQUAL^ expr
	|	expr NOT_EQUAL^ expr
	|	expr LTH^ expr
	|	expr LEQ^ expr
	|	expr GTH^ expr
	|	expr GEQ^ expr
	;

expr
	:	multExpr ((PLUS|MINUS)^ multExpr)*
	;
	
multExpr
  :	atom ((MULTIPLY|DIVIDE|MODULAR)^ atom)*
	;

array
  : LEFT_PAREN literal (COMMA literal )* RIGHT_PAREN -> literal+
  ;
	
atom
  :	literal
	| fieldName
	|	LEFT_PAREN! expr RIGHT_PAREN!
	| funcCall
	;
	
literal
  : string_value_expr
  | signed_numerical_literal
  ;
	
string_value_expr
  : STRING
  ;
  
signed_numerical_literal
  : sign? unsigned_numerical_literal
  ;
  
unsigned_numerical_literal
  : DIGIT 
  | REAL
  ;
  
sign
  : PLUS | MINUS
  ;
	
////////////////////////////////	
// Lexer Section  
////////////////////////////////
// Keywords
AS : 'as';
ALL : 'all';
AND : 'and';
ASC : 'asc';
BY : 'by';
COUNT : 'count';
CREATE : 'create';
CROSS : 'cross';
CUBE : 'cube';
DESC : 'desc';
DISTINCT : 'distinct';
DROP : 'drop';
EXCEPT : 'except';
FALSE : 'false';
FIRST : 'first';
FULL : 'full';
FROM : 'from';
GROUP : 'group';
NATURAL : 'natural';
NULL : 'null';
HAVING : 'having';
IN : 'in';
INDEX : 'index';
INNER : 'inner';
INSERT : 'insert';
INTERSECT : 'intersect';
INTO : 'into';
IS : 'is';
JOIN : 'join';
LAST : 'last';
LEFT : 'left';
LIKE : 'like';
LOCATION : 'location';
NOT : 'not';
ON : 'on';
OUTER : 'outer';
OR : 'or';
ORDER : 'order';
RIGHT : 'right';
ROLLUP : 'rollup';
SELECT : 'select';
TABLE : 'table';
TRUE : 'true';
UNION : 'union';
UNIQUE : 'unique';
UNKNOWN: 'unknown';
USING : 'using';
VALUES : 'values';
WHERE : 'where';
WITH : 'with';

// column types
BOOL : 'bool';
BYTE : 'byte';
CHAR : 'char';
INT : 'int';
LONG : 'long';
FLOAT : 'float';
DOUBLE : 'double';
TEXT : 'string';
DATE : 'date';
BYTES : 'bytes';
IPv4 : 'ipv4';

// Operators
ASSIGN  : ':=';
EQUAL  : '=';
SEMI_COLON :  ';';
COMMA : ',';
NOT_EQUAL  : '<>' | '!=' | '~='| '^=' ;
LTH : '<' ;
LEQ : '<=';
GTH   : '>';
GEQ   : '>=';
LEFT_PAREN :  '(';  
RIGHT_PAREN : ')';
PLUS  : '+';
MINUS : '-';
MULTIPLY: '*';
DIVIDE  : '/';
MODULAR : '%';
DOT : '.';

// Regular Expressions for Tokens
ID  : ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|':')*
    ;

DIGIT : '0'..'9'+
    ;

REAL
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT
    ;

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;

STRING
    :  '\'' ( ESC_SEQ | ~('\\'|'\'') )* '\'' {setText(getText().substring(1, getText().length()-1));}
    ;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;