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
  TABLE_DEF;
  TARGET_FIELDS;
  VALUES;
}

@header {
package tajo.engine.parser;

import java.util.List;
import java.util.ArrayList;
import tajo.engine.query.exception.TQLParseError;
}

@lexer::header {
package tajo.engine.parser;

import tajo.engine.query.exception.TQLParseError;
}

@lexer::members {
   @Override
   public void reportError(RecognitionException e) {
    throw new TQLParseError(getErrorHeader(e));
   }
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
  Tokens for Case Insensitive Keywords
===============================================================================
*/
fragment A
	:	'A' | 'a';

fragment B
	:	'B' | 'b';

fragment C
	:	'C' | 'c';

fragment D
	:	'D' | 'd';

fragment E
	:	'E' | 'e';

fragment F
	:	'F' | 'f';

fragment G
	:	'G' | 'g';

fragment H
	:	'H' | 'h';

fragment I
	:	'I' | 'i';

fragment J
	:	'J' | 'j';

fragment K
	:	'K' | 'k';

fragment L
	:	'L' | 'l';

fragment M
	:	'M' | 'm';

fragment N
	:	'N' | 'n';

fragment O
	:	'O' | 'o';

fragment P
	:	'P' | 'p';

fragment Q
	:	'Q' | 'q';

fragment R
	:	'R' | 'r';

fragment S
	:	'S' | 's';

fragment T
	:	'T' | 't';

fragment U
	:	'U' | 'u';

fragment V
	:	'V' | 'v';

fragment W
	:	'W' | 'w';

fragment X
	:	'X' | 'x';

fragment Y
	:	'Y' | 'y';

fragment Z
	:	'Z' | 'z';

/*
===============================================================================
  Reserved Keywords
===============================================================================
*/

AS : A S;
ALL : A L L;
AND : A N D;
ASC : A S C;

BY : B Y;

CASE : C A S E;
CHARACTER : C H A R A C T E R;
COUNT : C O U N T;
COPY : C O P Y;
CREATE : C R E A T E;
CROSS : C R O S S;
CUBE : C U B E;

DESC : D E S C;
DISTINCT : D I S T I N C T;
DROP : D R O P;

END : E N D;
ELSE : E L S E;
EXCEPT : E X C E P T;
EXTERNAL : E X T E R N A L;
FALSE : F A L S E;
FIRST : F I R S T;
FORMAT : F O R M A T;
FULL : F U L L;
FROM : F R O M;

GROUP : G R O U P;

HAVING : H A V I N G;

IN : I N;
INDEX : I N D E X;
INNER : I N N E R;
INSERT : I N S E R T;
INTERSECT : I N T E R S E C T;
INTO : I N T O;
IS : I S;

JOIN : J O I N;

LAST : L A S T;
LEFT : L E F T;
LIKE : L I K E;
LIMIT : L I M I T;
LOCATION : L O C A T I O N;

NATIONAL : N A T I O N A L;
NATURAL : N A T U R A L;
NOT : N O T;
NULL : N U L L;

ON : O N;
OUTER : O U T E R;
OR : O R;
ORDER : O R D E R;

PRECISION : P R E C I S I ON;

RIGHT : R I G H T;
ROLLUP : R O L L U P;

SET : S E T;
SELECT : S E L E C T;

TABLE : T A B L E;
THEN : T H E N;
TRUE : T R U E;

UNION : U N I O N;
UNIQUE : U N I Q U E;
UNKNOWN : U N K N O W N;
USING : U S I N G;

VALUES : V A L U E S;
VARYING : V A R Y I N G;

WHEN : W H E N;
WHERE : W H E R E;
WITH : W I T H;

ZONE : Z O N E;

/*
===============================================================================
  Data Type Tokens
===============================================================================
*/
BOOLEAN : B O O L E A N;
BOOL : B O O L;
BIT : B I T;
VARBIT : V A R B I T;

INT1 : I N T '1';
INT2 : I N T '2';
INT4 : I N T '4';
INT8 : I N T '8';

TINYINT : T I N Y I N T; // alias for INT1
SMALLINT : S M A L L I N T; // alias for INT2
INT : I N T; // alias for INT4
INTEGER : I N T E G E R; // alias - INT4
BIGINT : B I G I N T; // alias for INT8

FLOAT4 : F L O A T '4';
FLOAT8 : F L O A T '8';

REAL : R E A L; // alias for FLOAT4
FLOAT : F L O A T; // alias for FLOAT8
DOUBLE : D O U B L E; // alias for FLOAT8

NUMERIC : N U M E R I C;
DECIMAL : D E C I M A L; // alias for number

CHAR : C H A R;
VARCHAR : V A R C H A R;
NCHAR : N C H A R;
NVARCHAR : N V A R C H A R;

DATE : D A T E;
TIME : T I M E;
TIMETZ : T I M E T Z;
TIMESTAMP : T I M E S T A M P;
TIMESTAMPTZ : T I M E S T A M P T Z;

TEXT : T E X T;

BINARY : B I N A R Y;
VARBINARY : V A R B I N A R Y;
BLOB : B L O B;
BYTEA : B Y T E A; // alias for BLOB

INET4 : I N E T '4';

/*
===============================================================================
  SQL Main
===============================================================================
*/
sql
  : statement EOF
  ;

statement
  : sessionStatement
  | controlStatement
  | dataStatement
  | dataChangeStatement
  | schemaStatement
  | indexStatement
  ;

sessionStatement
  : 'session' 'clear' -> ^(SESSION_CLEAR)
  ;

controlStatement
  : '\\' 't' (table)? -> ^(SHOW_TABLE table?)
  | '\\' 'd' table -> ^(DESC_TABLE table)
  | '\\' 'f' -> ^(SHOW_FUNCTION)
  ;

dataStatement
  : query_expression
  | set_stmt
  | copyStatement
  ;

dataChangeStatement
  : insertStmt
  ;

schemaStatement
  : createTableStatement
  | DROP TABLE table -> ^(DROP_TABLE table)
  ;

indexStatement
  : CREATE (u=UNIQUE)? INDEX n=Identifier ON t=table (m=method_specifier)? LEFT_PAREN s=sort_specifier_list RIGHT_PAREN p=param_clause?-> ^(CREATE_INDEX $u? $m? $p? $n $t $s)
  ;

createTableStatement
  : CREATE EXTERNAL TABLE t=table def=tableElements USING f=Identifier p=param_clause? (LOCATION path=Character_String_Literal)
      -> ^(CREATE_TABLE $t EXTERNAL ^(TABLE_DEF $def) ^(USING $f) $p? ^(LOCATION $path))
  | CREATE TABLE t=table (def=tableElements)? (USING s=Identifier)? (p=param_clause)? (AS q=query_expression)?
      -> ^(CREATE_TABLE $t ^(TABLE_DEF $def)? ^(USING $s)? $p? ^(AS $q)?)
  ;

copyStatement
  : COPY t=table FROM path=string_value_expr FORMAT s=Identifier (p=param_clause)? -> ^(COPY $t $path $s $p?)
  ;

tableElements
  : LEFT_PAREN fieldElement (COMMA fieldElement)* RIGHT_PAREN -> fieldElement+
  ;

fieldElement
  : Identifier fieldType -> ^(FIELD_DEF Identifier fieldType)
  ;

fieldType
  : dataType
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

dataType
  : boolean_type
  | bit_type
  | varbit_type
  | int1_type
  | int2_type
  | int4_type
  | int8_type
  | float4_type
  | float_type
  | float8_type
  | number_type
  | char_type
  | varchar_type
  | nchar_type
  | nvarchar_type
  | DATE
  | TIME
  | timetz_type
  | TIMESTAMP
  | timestamptz_type
  | TEXT
  | binary_type
  | varbinary_type
  | blob_type
  | INET4
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
  : SELECT setQualifier? selectList from_clause? where_clause? groupby_clause? having_clause? orderby_clause? limit_clause?
  -> ^(SELECT from_clause? setQualifier? selectList where_clause? groupby_clause? having_clause? orderby_clause? limit_clause?)
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
	:	(t=Identifier DOT)? b=Identifier -> ^(FIELD_NAME $b $t?)
	;

asClause
  : (AS)? fieldName
  ;

column_reference
	:	fieldName (COMMA fieldName)* -> fieldName+
	;

table
  : Identifier
  ;

// TODO - to be improved
funcCall
	: Identifier LEFT_PAREN funcArgs? RIGHT_PAREN -> ^(FUNCTION[$Identifier.text] funcArgs?)
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
  : USING LEFT_PAREN f=column_reference RIGHT_PAREN -> ^(USING $f)
  ;

table_primary
  : table ((AS)? a=Identifier)? -> ^(TABLE table ($a)?)
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

limit_clause
  : LIMIT e=expr -> ^(LIMIT $e)
  ;

null_ordering
  : NULL FIRST -> ^(NULL_ORDER FIRST)
  | NULL LAST -> ^(NULL_ORDER LAST)
  ;

set_stmt
	:	SET (UNION|INTERSECT|EXCEPT) table
	;

search_condition
	:	bool_expr
	;

param_clause
  : WITH LEFT_PAREN param (COMMA param)* RIGHT_PAREN -> ^(PARAMS param+)
  ;

param
  : k=Character_String_Literal EQUAL v=bool_expr -> ^(PARAM $k $v)
  ;

method_specifier
  : USING m=Identifier -> ^(USING[$m.text])
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
  | case_expression
  ;

predicate
  : comparison_predicate
  | in_predicate
  | like_predicate
  | null_predicate
  ;

in_predicate
	:	expr NOT? IN array -> ^(IN expr array NOT?)
	;

like_predicate
  : f=fieldName NOT? LIKE s=string_value_expr -> ^(LIKE NOT? $f $s)
  ;

null_predicate
  : f=expr IS (n=NOT)? NULL -> ^(IS $f NULL $n?)
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

////////////////////////////////
// Case Statement
////////////////////////////////
case_expression
  : case_specification
  ;

case_specification
  : searched_case
  ;

searched_case
  : CASE^ (searched_when_clause)+ (else_clause)? END!
  ;

searched_when_clause
  : WHEN c=search_condition THEN r=result -> ^(WHEN $c $r)
  ;

else_clause
  : ELSE r=result -> ^(ELSE $r)
  ;

result
  : bool_expr
  ;

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

NUMBER : Digit+;

fragment
Digit : '0'..'9';

REAL_NUMBER
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT
    ;

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;

/*
===============================================================================
 Identifiers
===============================================================================
*/

// Regular Expressions for Tokens
Identifier  : ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|Digit|'_'|':')*
    ;

/*
===============================================================================
 Literal
===============================================================================
*/

// Some Unicode Character Ranges
fragment
Control_Characters                  :   '\u0001' .. '\u001F';
fragment
Extended_Control_Characters         :   '\u0080' .. '\u009F';

Character_String_Literal
    : Quote ( ESC_SEQ | ~('\\'|Quote) )* Quote {setText(getText().substring(1, getText().length()-1));}
    | Double_Quote ( ESC_SEQ | ~('\\'|Double_Quote) )* Double_Quote {setText(getText().substring(1, getText().length()-1));}
    ;

Quote
  : '\'';

Double_Quote
  : '"';

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


/*
===============================================================================
 Whitespace Tokens
===============================================================================
*/

Space : ' '
{
	$channel = HIDDEN;
};

White_Space :	( Control_Characters  | Extended_Control_Characters )+
{
	$channel = HIDDEN;
};


BAD : . {
  skip();
} ;