grammar NQL;

options {
	language=Java;
	backtrack=true;
	memoize=true;
	output=AST;
	ASTLabelType=CommonTree;
}

tokens {
  FIELD_NAME;
  COLUMN;
  FUNCTION;
  GROUP_BY;
  SHOW_TABLE;
  SHOW_FUNCTION;
  CREATE_TABLE;
  DROP_TABLE;
  DESC_TABLE;
  SESSION_CLEAR;
  SEL_LIST;
  ALL;
  VALUES;
  TARGET_FIELDS;
  TABLE_DEF;
  FIELD_DEF;
  FUNC_ARGS;
  STORE_TYPE;
}

@header {
package nta.engine.parser;

import java.util.List;
import java.util.ArrayList;

import nta.engine.exception.NQLSyntaxException;
}

@lexer::header {
package nta.engine.parser;

import java.util.List;
import java.util.ArrayList;

import nta.engine.exception.NQLSyntaxException;
}

@lexer::members {
   @Override
   public void reportError(RecognitionException e) {
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
  ;
  
controlStatement
  : '\\' 't' (table)? -> ^(SHOW_TABLE table?)
  | '\\' 'd' table -> ^(DESC_TABLE table)
  | '\\' 'f' -> ^(SHOW_FUNCTION)
  ;
  
dataStatement
  : select_stmt
  | set_stmt
  ;
  
dataChangeStatement
  : insertStmt
  ;
  
schemaStatement
  : createTable
  | DROP TABLE table -> ^(DROP_TABLE table)
  ;
  
createTable
  : CREATE TABLE table AS select_stmt -> ^(CREATE_TABLE ^(TABLE table) select_stmt)
  | CREATE TABLE table tableElements (USING ID)? -> ^(CREATE_TABLE ^(TABLE table) ^(TABLE_DEF tableElements) ^(STORE_TYPE ID)?)
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
  | INT
  | LONG
  | FLOAT
  | DOUBLE
  | TEXT (LEFT_PAREN DIGIT RIGHT_PAREN)?
  | BYTES
  | IPv4
  ;
  
select_stmt
  : SELECT selectList from_clause? where_clause? groupby_clause?
  -> ^(SELECT from_clause? where_clause? groupby_clause? selectList)
  ;
  
insertStmt
  : INSERT 'into' table (LEFT_PAREN fieldList RIGHT_PAREN)? 'values' array
  -> ^(INSERT ^(TABLE table) ^(VALUES array) ^(TARGET_FIELDS fieldList)?)
  ;
	
selectList
  : MULTIPLY -> ^(SEL_LIST ALL)
  | derivedColumn (COMMA derivedColumn)* -> ^(SEL_LIST derivedColumn+)
  ;
  
derivedColumn
  : bool_expr asClause? -> ^(COLUMN bool_expr asClause?)
  ;
	
fieldName
	:	(t=ID DOT)? b=ID -> ^(FIELD_NAME $b $t?)
	;
	
asClause
  : AS? fieldName
  ;
	
fieldList  
	:	fieldName (COMMA fieldName)* -> fieldName+
	;
  
table  
  : ID
  ;

funcCall
	:	ID LEFT_PAREN funcArgs? RIGHT_PAREN -> ^(FUNCTION[$ID.text] funcArgs?)
	;
	
funcArgs
  : bool_expr (COMMA bool_expr)* -> bool_expr+ 
  ;
	
from_clause
  : FROM^ table_list
  ;
  
table_list
  :tableRef (COMMA tableRef)* -> tableRef+
  ;  
  
tableRef
  : derivedTable
  | joinedTable
  ;
  
joinedTable
  : derivedTable 'cross' 'join' derivedTable
  | derivedTable join_type? 'join' derivedTable join_condition
  | derivedTable 'natural' join_type? 'join' derivedTable
  ;
  
join_condition
  : 'on' bool_expr
  ;
  
  
join_type
  : 'inner'
  | 'outer' outer_join_type?
  ;
  
outer_join_type
  : 'left'
  | 'right'
  | 'full'
  ;
  
derivedTable
  : table (AS ID)? -> ^(TABLE table (ID)?)
  ;

where_clause
  : WHERE^ search_condition
  ;
  
groupby_clause
  : 'group' 'by' fieldList having_clause? -> ^(GROUP_BY having_clause? fieldList)
  ;
  
having_clause
  : HAVING^ bool_expr
  ;
	
set_stmt
	:	'set' ('union'|'intersect'|'diff') table
	;
	
search_condition
	:	bool_expr
	; 

bool_expr
	:	and_predicate (OR^ and_predicate)*
	;

and_predicate
  :	boolean_primary (AND^ boolean_primary)*
	;

boolean_primary
  : predicate 
  | expr
  | LEFT_PAREN! bool_expr RIGHT_PAREN!
  ;

predicate 
  : comparison_predicate
  | in_predicate
  ;

in_predicate
	:	expr NOT? IN array -> ^(IN expr array NOT?)
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
//  @init { CommonTreeAdaptor adaptor = new CommonTreeAdaptor(); }
  :	literal
	| fieldName
	|	LEFT_PAREN! expr RIGHT_PAREN!
	| funcCall
	;
	
literal
  : general_literal
  | signed_numerical_literal
  ;
	
// It represents various literals, such as string, bytes, and network addr.
general_literal
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
IN : 'in';
NOT : 'not';
AND : 'and';
OR : 'or';
AS : 'as';
CREATE : 'create';
SELECT : 'select';
INSERT : 'insert';
INTO : 'into';
VALUES : 'values';
DROP : 'drop';
TABLE : 'table';
FROM : 'from';
WHERE : 'where';
HAVING : 'having';
USING : 'using';

// column types
BOOL : 'bool';
BYTE : 'byte';
INT : 'int';
LONG : 'long';
FLOAT : 'float';
DOUBLE : 'double';
TEXT : 'string';
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
    :  '\'' ( ESC_SEQ | ~('\\'|'\'') )* '\''
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