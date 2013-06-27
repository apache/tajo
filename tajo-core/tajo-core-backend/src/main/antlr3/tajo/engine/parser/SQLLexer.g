/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

lexer grammar SQLLexer;

@header {
package tajo.engine.parser;

import tajo.engine.query.exception.TQLParseError;
}

@members {
   @Override
   public void reportError(RecognitionException e) {
    throw new TQLParseError(getErrorHeader(e));
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
COALESCE : C O A L E S C E;
COUNT : C O U N T;
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
NULLIF : N U L L I F;

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

Identifier
  : ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|Digit|'_'|':')*
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
    : Quote ( ESC_SEQ | ~('\\'|Quote) )* Quote
      { setText(getText().substring(1, getText().length()-1)); }
    | Double_Quote ( ESC_SEQ | ~('\\'|Double_Quote) )* Double_Quote
      { setText(getText().substring(1, getText().length()-1)); }
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