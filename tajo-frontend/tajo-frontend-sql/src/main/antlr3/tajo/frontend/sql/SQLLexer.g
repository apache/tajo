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

lexer grammar SQLLexer;

options{
  language=Java;
}

@header {
package tajo.frontend.sql;

import tajo.frontend.sql.SQLParseError;
}

@members {
   @Override
   public void reportError(RecognitionException e) {
    throw new SQLParseError(getErrorHeader(e));
   }
}

/*
==============================================================================================
  Comment   Tokens
==============================================================================================
*/
fragment
Start_Comment   : '/*';

fragment
End_Comment     : '*/';

fragment
Line_Comment    : '//';

COMMENT
    :   (   Start_Comment ( options {greedy=false;} : . )* End_Comment )+
    {
      $channel=HIDDEN;
    }
    ;

LINE_COMMENT
    :   (   ( Line_Comment | '--' ) ~('\n'|'\r')* '\r'? '\n')+
    {
      $channel=HIDDEN;
    }
    ;

fragment
SQL_Terminal_Character  :  SQL_Language_Character;

fragment
SQL_Language_Character  :  Simple_Latin_Letter | Digit | SQL_Special_Character;

fragment
Simple_Latin_Letter  :  Simple_Latin_Upper_Case_Letter | Simple_Latin_Lower_Case_Letter;

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

fragment
Simple_Latin_Upper_Case_Letter :
		'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' |
		'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z';

fragment
Simple_Latin_Lower_Case_Letter :
		'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' |
		'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z';

fragment
Digit  :  '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9';

fragment
OctalDigit  :  '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7';

fragment
Hexit   :  Digit | A | B | C | D | E | F;

fragment
SQL_Special_Character :
		Space
	|	Double_Quote
	|	Percent
	|	Ampersand
	|	Quote
	|	Left_Paren
	|	Right_Paren
	|	Asterisk
	|	Plus_Sign
	|	Comma
	|	Minus_Sign
	|	Period
	|	Slash
	|	Colon
	|	Semicolon
	|	Less_Than_Operator
	|	Equals_Operator
	|	Greater_Than_Operator
	|	Question_Mark
	|	Left_Bracket
	|	Right_Bracket
	|	Circumflex
	|	Underscore
	|	Vertical_Bar
	|	Left_Brace
	|	Right_Brace
	;

fragment
Unsigned_Large_Integer
	:	;
fragment
Signed_Large_Integer
	:	;
fragment
Unsigned_Float
	:	;
fragment
Signed_Float
	:	;

/*
==============================================================================================
 Reserved Keyword Tokens
==============================================================================================
*/
ALL                         :   A L L;
AS                          :   A S;
AND                         :   A N D;

BEGIN                       :   B E G I N;
BETWEEN                     :   B E T W E E N;
BIGINT                      :   B I G I N T;
BINARY                      :   B I N A R Y;
BLOB                        :   B L O B;
BOOLEAN                     :   B O O L E A N;
BOTH                        :   B O T H;
BY                          :   B Y;
// non-standard
BYTE                        :   B Y T E;
BYTES                       :   B Y T E S;

CASE                        :   C A S E;
CHAR                        :   C H A R;
CREATE                      :   C R E A T E;
CROSS                       :   C R O S S;
CUBE                        :   C U B E;

DATE                        :   D A T E;
DAY                         :   D A Y;
DECIMAL                     :   D E C I M A L;
DECLARE                     :   D E C L A R E;
DESCRIBE                    :   D E S C R I B E;
DISCONNECT                  :   D I S C O N N E C T;
DISTINCT                    :   D I S T I N C T;
DOUBLE                      :   D O U B L E;
DROP                        :   D R O P;

ELSE                        :   E L S E;
END_EXEC                    :   E N D Hyphen E X E C;
END                         :   E N D;
ESCAPE                      :   E S C A P E;
EXCEPT                      :   E X C E P T;
// non-standard
EXTERNAL                    :   E X T E R N A L;

FALSE                       :   F A L S E;
FETCH                       :   F E T C H;
FILTER                      :   F I L T E R;
FLOAT                       :   F L O A T;
FOREIGN                     :   F O R E I G N;
FOR                         :   F O R;
FREE                        :   F R E E;
FROM                        :   F R O M;
FULL                        :   F U L L;
FUNCTION                    :   F U N C T I O N;

IDENTITY                    :   I D E N T I T Y;
IMMEDIATE                   :   I M M E D I A T E;
INDICATOR                   :   I N D I C A T O R;
INNER                       :   I N N E R;
INOUT                       :   I N O U T;
INPUT                       :   I N P U T;
INSENSITIVE                 :   I N S E N S I T I V E;
INSERT                      :   I N S E R T;
INTEGER                     :   I N T E G E R;
INTERSECT                   :   I N T E R S E C T;
INTERVAL                    :   I N T E R V A L;
INTO                        :   I N T O;
INT                         :   I N T;
IN                          :   I N;
ISOLATION                   :   I S O L A T I O N;
IS                          :   I S;
// non standards
IPV4                        :   I P V '4';
INDEX                       :   I N D E X;

JOIN                        :   J O I N;

NATIONAL                    :   N A T I O N A L;
NATURAL                     :   N A T U R A L;
NCHAR_VARYING               :   N C H A R Underscore V A R Y I N G;
NCHAR                       :   N C H A R;
NCLOB                       :   N C L O B;
NEW                         :   N E W;
NONE                        :   N O N E;
NOTFOUND                    :   N O T F O U N D;
NOT                         :   N O T;
NO                          :   N O;
NULL                        :   N U L L;
NUMERIC                     :   N U M E R I C;

LANGUAGE                    :   L A N G U A G E;
LEFT                        :   L E F T;
LIKE                        :   L I K E;
LOCALTIMESTAMP              :   L O C A L T I M E S T A M P;
LOCALTIME                   :   L O C A L T I M E;
LOCAL                       :   L O C A L;
LIMIT                       :   L I M I T;
// non standards
LOCATION                    :   L O C A T I O N;

GET                         :   G E T;
GLOBAL                      :   G L O B A L;
GRANT                       :   G R A N T;
GROUPING                    :   G R O U P I N G;
GROUP                       :   G R O U P;

HAVING                      :   H A V I N G;
HOUR                        :   H O U R;

OF                          :   O F;
ON                          :   O N;
ORDER                       :   O R D E R;
OR                          :   O R;
OUTER                       :   O U T E R;
OUTPUT                      :   O U T P U T;

RIGHT                       :   R I G H T;
ROLLBACK                    :   R O L L B A C K;
ROLLUP                      :   R O L L U P;
ROWS                        :   R O W S;
ROW                         :   R O W;

SELECT                      :   S E L E C T;
SET                         :   S E T;
SESSION                     :   S E S S I O N;
SIMILAR                     :   S I M I L A R;
SMALLINT                    :   S M A L L I N T;
SQL                         :   S Q L;
START                       :   S T A R T;
SYSTEM_USER                 :   S Y S T E M Underscore U S E R;
SYSTEM                      :   S Y S T E M;
// non standards
STRING                      :   S T R I N G;

TABLE                       :   T A B L E;
THEN                        :   T H E N;
TIMESTAMP                   :   T I M E S T A M P;
TIMEZONE_HOUR               :   T I M E Z O N E Underscore H O U R;
TIMEZONE_MINUTE             :   T I M E Z O N E Underscore M I N U T E;
TIME                        :   T I M E;
TO                          :   T O;
TRUE                        :   T R U E;

UNION                       :   U N I O N;
UNIQUE                      :   U N I Q U E;
UNKNOWN                     :   U N K N O W N;
USER                        :   U S E R;
USING                       :   U S I N G;

VALUES                      :   V A L U E S;
VALUE                       :   V A L U E;
VARCHAR                     :   V A R C H A R;
VARYING                     :   V A R Y I N G;

WHEN                        :   W H E N;
WHERE                       :   W H E R E;
WINDOW                      :   W I N D O W;
WITH                        :   W I T H;
WITHIN                      :   W I T H I N;
WITHOUT                     :   W I T H O U T;

YEAR                        :   Y E A R;

/*
==============================================================================================
 Non-Reserved Keyword Tokens
==============================================================================================
*/
ASC                         :   A S C;

COUNT                       :   C O U N T;

DESC                        :   D E S C;

FINAL                       :   F I N A L;
FIRST                       :   F I R S T;

LAST                        :   L A S T;
LONG                        :   L O N G;

// Non Standard Keyword Tokens
CLEAR : 'clear';
DIFF : 'diff';


/*
==============================================================================================
 Punctuation and Arithmetic/Logical Operators
==============================================================================================
*/
Not_Equals_Operator
  : '<>' | '!=' | '~='| '^=' ;
Greater_Or_Equals_Operator
  : '>=';
Less_Or_Equals_Operator
  : '<=';
Concatenation_Operator
  :	 '||';
Right_Arrow
  :	 '->';
Double_Colon
  :	 '::';
Double_Period
  :	 '..';
Back_Quote
  :	 '`';
Tilde
  :	 '~';
Exclamation
  :	 '!';
AT_Sign
  :	 '@';
Percent
  :	 '\%';

Circumflex
  :	 '^';
Ampersand
  :	 '&';
Asterisk
  : '*';
Left_Paren
  :  '(';
Right_Paren
  : ')';
Plus_Sign
  : '+';
Minus_Sign
  : '-';
fragment
Hyphen
	:	 '-';
Equals_Operator
	:	 '=';
Left_Brace
	:	 '{';
Right_Brace
	:	 '}';

/*
==============================================================================================
 The trigraphs are new in SQL-2003.
==============================================================================================
*/

Left_Bracket
	:	 '[';
Left_Bracket_Trigraph
	:	 '??(';
Right_Bracket
	:	 ']';
Right_Bracket_Trigraph
	:	 '??)';
Vertical_Bar
	:	 '|';
Colon
	:	 ':';
Semicolon
	:	 ';';
Double_Quote
	:	 '"';
Quote
	:	 '\'';
Less_Than_Operator
	:	 '<';
Greater_Than_Operator
	:	 '>';
Comma
  : ',';
Period
  : '.';
Question_Mark
	:	 '?';
Slash
	:	 '/';

fragment
Underscore	: '_';
fragment
Back_Slash  : '\\';
fragment
Hash_Sign   : '#';
fragment
Dollar_Sign : '$';


// Tajo Ones
ASSIGN  : ':=';

/*
==============================================================================================
 Character Set rules
==============================================================================================
*/

fragment
Unicode_Permitted_Identifier_Character  :
        Basic_Latin_Without_Quotes
    |   Latin1_Supplement
    |   Latin_ExtendedA
    |   Latin_ExtendedB
    |   IPA_Extensions
    |   Combining_Diacritical_Marks
    |   Greek_and_Coptic
    |   Cyrillic
    |   Cyrillic_Supplementary
    |   Armenian
    |   Hebrew
    |   Arabic
    |   Syriac
    |   Thaana
    |   Devanagari
    |   Bengali
    |   Gurmukhi
    |   Gujarati
    |   Oriya
    |   Tamil
    |   Telugu
    |   Kannada
    |   Malayalam
    |   Sinhala
    |   Thai
    |   Lao
    |   Tibetan
    |   Myanmar
    |   Georgian
    |   Hangul_Jamo
    |   Ethiopic
    |   Cherokee
    |   Unified_Canadian_Aboriginal
    |   Ogham
    |   Runic
    |   Tagalog
    |   Hanunoo
    |   Buhid
    |   Tagbanwa
    |   Khmer
    |   Mongolian
    |   Limbu
    |   Tai_Le
    |   Khmer_Symbols
    |   Phonetic_Extensions
    |   Latin_Extended_Additional
    |   Greek_Extended
    |   Superscripts_and_Subscripts
    |   Currency_Symbols
    |   Combining_Diacritical_Symbol_Marks
    |   Letterlike_Symbols
    |   Number_Forms
    |   Enclosed_Alphanumerics
    |   CJK_Radicals_Supplement
    |   Kangxi_Radicals
    |   Ideographic_Description_Characters
    |   CJK_Symbols_and_Punctuation
    |   Hiragana
    |   Katakana
    |   Bopomofo
    |   Hangul_Compatibility_Jamo
    |   Kanbun
    |   Bopomofo_Extended
    |   Katakana_Phonetic_Extensions
    |   Enclosed_CJK_Letters_and_Months
    |   CJK_Compatibility
    |   CJK_Unified_Ideographs_ExtensionA
    |   CJK_Unified_Ideographs
    |   Yi_Syllables
    |   Yi_Radicals
    |   Hangul_Syllables
    |   High_Surrogates
    |   High_Private_Use_Surrogates
    |   Low_Surrogates
    |   Private_Use_Area
    |   CJK_Compatibility_Ideographs
    |   Alphabetic_Presentation_Forms
    |   Arabic_Presentation_FormsA
    |   Variation_Selectors
    |   Combining_Half_Marks
    |   CJK_Compatibility_Forms
    |   Small_Form_Variants
    |   Arabic_Presentation_FormsB
    |   Halfwidth_and_Fullwidth_Forms
    ;

//  Generate an Unexpected Token Error if any forbidden characters are used in a Unicode Identifier
Unicode_Forbidden_Identifier_Characters  :  ( Unicode_Forbidden_Identifier_Character )+;

fragment
Unicode_Forbidden_Identifier_Character  :
        Control_Characters
    |   Spacing_Modifier_Letters
    |   General_Punctuation
    |   Arrows
    |   Mathematical_Operators
    |   Miscellaneous_Technical
    |   Control_Pictures
    |   Optical_Character_Recognition
    |   Box_Drawing
    |   Block_Elements
    |   Geometric_Shapes
    |   Miscellaneous_Symbols
    |   Dingbats
    |   Miscellaneous_Mathematical_SymbolsA
    |   Supplemental_ArrowsA
    |   Braille_Patterns
    |   Supplemental_ArrowsB
    |   Miscellaneous_Mathematical_SymbolsB
    |   Supplemental_Mathematical_Operators
    |   Miscellaneous_Symbols_and_Arrows
    |   Yijing_Hexagram_Symbols
    |   Specials
    ;

// Unicode Character Ranges
fragment
Unicode_Character_Without_Quotes    :   Basic_Latin_Without_Quotes
                                    |   '\u00A0' .. '\uFFFF';
fragment
Extended_Latin_Without_Quotes       :   '\u0001' .. '!' | '#' .. '&' | '(' .. '\u00FF';
fragment
Control_Characters                  :   '\u0001' .. '\u001F';
fragment
Basic_Latin                         :   '\u0020' .. '\u007F';
fragment
Basic_Latin_Without_Quotes          :   ' ' .. '!' | '#' .. '&' | '(' .. '~';
fragment
Regex_Non_Escaped_Unicode           :   ~( '|' | '*' | '+' | '-' | '?' | '\%' | '_' | '^' | ':' | '{' | '}' | '(' | ')' | '[' | '\\' ) ;
fragment
Regex_Escaped_Unicode               :   ' ' .. '[' | ']' .. '~' | '\u00A0' .. '\uFFFF';
fragment
Unicode_Allowed_Escape_Chracter		:	'!' | '#' .. '&' | '(' .. '/' | ':' .. '@' | '[' .. '`' | '{' .. '~' | '\u0080' .. '\u00BF';
fragment
Full_Unicode						:	'\u0001' .. '\uFFFF';
fragment
Extended_Control_Characters         :   '\u0080' .. '\u009F';
fragment
Latin1_Supplement                   :   '\u00A0' .. '\u00FF';
fragment
Latin_ExtendedA                     :   '\u0100' .. '\u017F';
fragment
Latin_ExtendedB                     :   '\u0180' .. '\u024F';
fragment
IPA_Extensions                      :   '\u0250' .. '\u02AF';
fragment
Spacing_Modifier_Letters            :   '\u02B0' .. '\u02FF';
fragment
Combining_Diacritical_Marks         :   '\u0300' .. '\u036F';
fragment
Greek_and_Coptic                    :   '\u0370' .. '\u03FF';
fragment
Cyrillic                            :   '\u0400' .. '\u04FF';
fragment
Cyrillic_Supplementary              :   '\u0500' .. '\u052F';
fragment
Armenian                            :   '\u0530' .. '\u058F';
fragment
Hebrew                              :   '\u0590' .. '\u05FF';
fragment
Arabic                              :   '\u0600' .. '\u06FF';
fragment
Syriac                              :   '\u0700' .. '\u074F';
fragment
Thaana                              :   '\u0780' .. '\u07BF';
fragment
Devanagari                          :   '\u0900' .. '\u097F';
fragment
Bengali                             :   '\u0980' .. '\u09FF';
fragment
Gurmukhi                            :   '\u0A00' .. '\u0A7F';
fragment
Gujarati                            :   '\u0A80' .. '\u0AFF';
fragment
Oriya                               :   '\u0B00' .. '\u0B7F';
fragment
Tamil                               :   '\u0B80' .. '\u0BFF';
fragment
Telugu                              :   '\u0C00' .. '\u0C7F';
fragment
Kannada                             :   '\u0C80' .. '\u0CFF';
fragment
Malayalam                           :   '\u0D00' .. '\u0D7F';
fragment
Sinhala                             :   '\u0D80' .. '\u0DFF';
fragment
Thai                                :   '\u0E00' .. '\u0E7F';
fragment
Lao                                 :   '\u0E80' .. '\u0EFF';
fragment
Tibetan                             :   '\u0F00' .. '\u0FFF';
fragment
Myanmar                             :   '\u1000' .. '\u109F';
fragment
Georgian                            :   '\u10A0' .. '\u10FF';
fragment
Hangul_Jamo                         :   '\u1100' .. '\u11FF';
fragment
Ethiopic                            :   '\u1200' .. '\u137F';
fragment
Cherokee                            :   '\u13A0' .. '\u13FF';
fragment
Unified_Canadian_Aboriginal         :   '\u1400' .. '\u167F';
fragment
Ogham                               :   '\u1680' .. '\u169F';
fragment
Runic                               :   '\u16A0' .. '\u16FF';
fragment
Tagalog                             :   '\u1700' .. '\u171F';
fragment
Hanunoo                             :   '\u1720' .. '\u173F';
fragment
Buhid                               :   '\u1740' .. '\u175F';
fragment
Tagbanwa                            :   '\u1760' .. '\u177F';
fragment
Khmer                               :   '\u1780' .. '\u17FF';
fragment
Mongolian                           :   '\u1800' .. '\u18AF';
fragment
Limbu                               :   '\u1900' .. '\u194F';
fragment
Tai_Le                              :   '\u1950' .. '\u197F';
fragment
Khmer_Symbols                       :   '\u19E0' .. '\u19FF';
fragment
Phonetic_Extensions                 :   '\u1D00' .. '\u1D7F';
fragment
Latin_Extended_Additional           :   '\u1E00' .. '\u1EFF';
fragment
Greek_Extended                      :   '\u1F00' .. '\u1FFF';
fragment
General_Punctuation                 :   '\u2000' .. '\u206F';
fragment
Superscripts_and_Subscripts         :   '\u2070' .. '\u209F';
fragment
Currency_Symbols                    :   '\u20A0' .. '\u20CF';
fragment
Combining_Diacritical_Symbol_Marks  :   '\u20D0' .. '\u20FF';
fragment
Letterlike_Symbols                  :   '\u2100' .. '\u214F';
fragment
Number_Forms                        :   '\u2150' .. '\u218F';
fragment
Arrows                              :   '\u2190' .. '\u21FF';
fragment
Mathematical_Operators              :   '\u2200' .. '\u22FF';
fragment
Miscellaneous_Technical             :   '\u2300' .. '\u23FF';
fragment
Control_Pictures                    :   '\u2400' .. '\u243F';
fragment
Optical_Character_Recognition       :   '\u2440' .. '\u245F';
fragment
Enclosed_Alphanumerics              :   '\u2460' .. '\u24FF';
fragment
Box_Drawing                         :   '\u2500' .. '\u257F';
fragment
Block_Elements                      :   '\u2580' .. '\u259F';
fragment
Geometric_Shapes                    :   '\u25A0' .. '\u25FF';
fragment
Miscellaneous_Symbols               :   '\u2600' .. '\u26FF';
fragment
Dingbats                            :   '\u2700' .. '\u27BF';
fragment
Miscellaneous_Mathematical_SymbolsA  :    '\u27C0' .. '\u27EF';
fragment
Supplemental_ArrowsA                :   '\u27F0' .. '\u27FF';
fragment
Braille_Patterns                    :   '\u2800' .. '\u28FF';
fragment
Supplemental_ArrowsB                :   '\u2900' .. '\u297F';
fragment
Miscellaneous_Mathematical_SymbolsB  :    '\u2980' .. '\u29FF';
fragment
Supplemental_Mathematical_Operators  :    '\u2A00' .. '\u2AFF';
fragment
Miscellaneous_Symbols_and_Arrows    :   '\u2B00' .. '\u2BFF';
fragment
CJK_Radicals_Supplement             :   '\u2E80' .. '\u2EFF';
fragment
Kangxi_Radicals                     :   '\u2F00' .. '\u2FDF';
fragment
Ideographic_Description_Characters  :   '\u2FF0' .. '\u2FFF';
fragment
CJK_Symbols_and_Punctuation         :   '\u3000' .. '\u303F';
fragment
Hiragana                            :   '\u3040' .. '\u309F';
fragment
Katakana                            :   '\u30A0' .. '\u30FF';
fragment
Bopomofo                            :   '\u3100' .. '\u312F';
fragment
Hangul_Compatibility_Jamo           :   '\u3130' .. '\u318F';
fragment
Kanbun                              :   '\u3190' .. '\u319F';
fragment
Bopomofo_Extended                   :   '\u31A0' .. '\u31BF';
fragment
Katakana_Phonetic_Extensions        :   '\u31F0' .. '\u31FF';
fragment
Enclosed_CJK_Letters_and_Months     :   '\u3200' .. '\u32FF';
fragment
CJK_Compatibility                   :   '\u3300' .. '\u33FF';
fragment
CJK_Unified_Ideographs_ExtensionA   :   '\u3400' .. '\u4DBF';
fragment
CJK_Unified_Ideographs              :   '\u4E00' .. '\u9FFF';
fragment
Yijing_Hexagram_Symbols             :   '\u4DC0' .. '\u4DFF';
fragment
Yi_Syllables                        :   '\uA000' .. '\uA48F';
fragment
Yi_Radicals                         :   '\uA490' .. '\uA4CF';
fragment
Hangul_Syllables                    :   '\uAC00' .. '\uD7AF';
fragment
High_Surrogates                     :   '\uD800' .. '\uDB7F';
fragment
High_Private_Use_Surrogates         :   '\uDB80' .. '\uDBFF';
fragment
Low_Surrogates                      :   '\uDC00' .. '\uDFFF';
fragment
Private_Use_Area                    :   '\uE000' .. '\uF8FF';
fragment
CJK_Compatibility_Ideographs        :   '\uF900' .. '\uFAFF';
fragment
Alphabetic_Presentation_Forms       :   '\uFB00' .. '\uFB4F';
fragment
Arabic_Presentation_FormsA          :   '\uFB50' .. '\uFDFF';
fragment
Variation_Selectors                 :   '\uFE00' .. '\uFE0F';
fragment
Combining_Half_Marks                :   '\uFE20' .. '\uFE2F';
fragment
CJK_Compatibility_Forms             :   '\uFE30' .. '\uFE4F';
fragment
Small_Form_Variants                 :   '\uFE50' .. '\uFE6F';
fragment
Arabic_Presentation_FormsB          :   '\uFE70' .. '\uFEFF';
fragment
Halfwidth_and_Fullwidth_Forms       :   '\uFF00' .. '\uFFEF';
fragment
Specials                            :   '\uFFF0' .. '\uFFFF';




fragment
Unicode_Identifier_Part          :  Unicode_Escape_Value | Unicode_Permitted_Identifier_Character;

fragment
Unicode_Escape_Value             :  Unicode_4_Digit_Escape_Value  | Unicode_6_Digit_Escape_Value ;

fragment
Unicode_4_Digit_Escape_Value     :  Escape_Character  Hexit  Hexit  Hexit  Hexit ;

fragment
Unicode_6_Digit_Escape_Value     :  Escape_Character  Plus_Sign Hexit  Hexit  Hexit  Hexit  Hexit  Hexit ;

Escape_Character                 :  '\\' /* Unicode_Allowed_Escape_Chracter */ /*!! See the Syntax Rules*/;


/*
==============================================================================================
 5.3 <literal> (p143)
==============================================================================================
*/

fragment
HexPair     :   Hexit Hexit;

fragment
HexQuad     :   Hexit Hexit Hexit Hexit;

fragment
Unsigned_Integer	:
	(	'0'
			(	( 'x' | 'X' )
				HexPair ( HexPair ( HexQuad (HexQuad HexQuad)? )? )?
			|	OctalDigit ( OctalDigit )*
			|	{true}?
				( '0' )*
			)
	|	( '1'..'9' ) ( Digit )*
	);

fragment
Signed_Integer	:
	( Plus_Sign | Minus_Sign ) ( Digit )+
	;

Number	:
	(	'0'
			(	( 'x' | 'X' )
				HexPair ( HexPair ( HexQuad (HexQuad HexQuad)? )? )?
				(	('K' | 'M' |'G')
					{
						_type  =  Unsigned_Large_Integer;
					}
				|	{true}?
					{
						_type  =  Unsigned_Integer;
					}
				)

			|	( OctalDigit )*
				(	('K' | 'M' |'G')
					{
						_type  =  Unsigned_Large_Integer;
					}
				|	{true}?
					{
						_type  =  Unsigned_Integer;
					}
				)

			|	Period
				( Digit )+ ( ( 'f' | 'F' | 'd' | 'D' | 'e' | 'E' ) ( Plus_Sign | Minus_Sign )? Digit ( Digit ( Digit )? )? )?
				{
					_type  =  Unsigned_Float;
				}

			|	'8'..'9' ( Digit )*
				(	('K' | 'M' |'G')
					{
						_type  =  Unsigned_Large_Integer;
					}
				|	{true}?
					{
						_type  =  Unsigned_Integer;
					}
				)
			)
	|	( Plus_Sign | Minus_Sign ) ( Digit )+
			(	Period ( Digit )+ ( ( 'f' | 'F' | 'd' | 'D' | 'e' | 'E' ) ( Plus_Sign | Minus_Sign )? Digit ( Digit ( Digit )? )? )?
				{
					_type  =  Signed_Float;
				}

			|	(	('K' | 'M' |'G')
					{
						_type  =  Signed_Large_Integer;
					}
				|	{true}?
					{
						_type  =  Signed_Integer;
					}
				)
			)
	|	( '1'..'9' ) ( Digit )*
			(	Period ( Digit )+ ( ( 'f' | 'F' | 'd' | 'D' | 'e' | 'E' ) ( Plus_Sign | Minus_Sign )? Digit ( Digit ( Digit )? )? )?
				{
					_type  =  Unsigned_Float;
				}

			|	(	('K' | 'M' |'G')
					{
						_type  =  Unsigned_Large_Integer;
					}
				|	{true}?
					{
						_type  =  Unsigned_Integer;
					}
				)
			)

	|	Period
			( Digit )+ ( ( 'f' | 'F' | 'd' | 'D' | 'e' | 'E' ) ( Plus_Sign | Minus_Sign )? Digit ( Digit ( Digit )? )? )?
				{
					_type  =  Unsigned_Float;
				}
	);

fragment
Character_Set_Name  : ( ( ( Regular_Identifier )  Period )?
                          ( Regular_Identifier )  Period )?
                            Regular_Identifier ;

fragment
Character_Set		:	Underscore  Character_Set_Name;

fragment
Character_String_Literal :
		  Quote ( Extended_Latin_Without_Quotes  )* Quote ( Quote ( Extended_Latin_Without_Quotes  )* Quote )* {setText(getText().substring(1, getText().length()-1));}
		;

fragment
National_Character_String_Literal :
		'N' Quote ( Extended_Latin_Without_Quotes  )* Quote ( Quote ( Extended_Latin_Without_Quotes  )* Quote )*
		;

fragment
Unicode_Character_String_Literal  :
		'U' Ampersand Quote ( Unicode_Character_Without_Quotes  )* Quote ( Quote ( Unicode_Character_Without_Quotes  )* Quote )*
		;

fragment
Bit :   ('0' | '1');

fragment
Bit_String_Literal  :
		'B' Quote ( Bit Bit Bit Bit  Bit Bit Bit Bit  )* Quote ( Quote ( Bit Bit Bit Bit  Bit Bit Bit Bit  )* Quote )*
		;

fragment
Hex_String_Literal  :
		'X' Quote ( Hexit  Hexit  )* Quote ( Quote ( Hexit  Hexit  )* Quote )*
		;

String_Literal	:
	(	Character_Set
		(
			Unicode_Character_String_Literal
				{
					_type  =  Unicode_Character_String_Literal;
				}
		|	Character_String_Literal
				{
					_type  =  Character_String_Literal;
				}
		)
	|	Bit_String_Literal
			{
				_type  =  Bit_String_Literal;
			}
	|	Hex_String_Literal
			{
				_type  =  Hex_String_Literal;
			}
	|	National_Character_String_Literal
			{
				_type  =  National_Character_String_Literal;
			}
	|	Unicode_Character_String_Literal
			{
				_type  =  Unicode_Character_String_Literal;
			}
	|	Character_String_Literal
			{
				_type  =  Character_String_Literal;
			}
	);

/*
==============================================================================================
 5.4 Names and identifiers (p151)

 Refer to the comparison table among various commercial DBMSs.

 Database        1st Character           Sunsequent Characters           Case Sensitive  Max Length
 ============    ====================    ============================    ==============  ==========
 Sybase ASA      Latin,'_','@','#','$'   Latin,Digit,'_','@','#','$'     no              128
 Sybase ASE      Latin,'_','@','#'       Latin,Digit,'_','@','#','$'     no              255
 SQL Server      Latin,'_','@','#'       Latin,Digit,'_','@','#','$'     no              128
 Teradata        Latin,'_','#','$'       Latin,Digit,'_','#','$'         no              30
 MySQL           Latin,Digit             Latin,Digit,'_','$'             yes             64
 Informix        Latin,'_'               Latin,Digit,'_','$'             no              128
 PostgreSQL      Latin,'_'               Latin,Digit,'_','$'             no              63
 Oracle          Latin                   Latin,Digit,'_','#','$'         no              30
 Interbase       Latin                   Latin,Digit,'_','$'             no              67
 ANSI SQL92      Latin                   Latin,Digit,'_'                 yes             128
 IBM DB2         same as ANSI SQL92                                      no              128
 HP Neoview      same as ANSI SQL92                                      no              128
 ------------    --------------------    ----------------------------    --------------  ----------
 Tajo SQL        Latin                   Latin,Digit,'_',':'             no              128
==============================================================================================
*/

Regular_Identifier     :  Tajo_Identifier;

fragment
Tajo_Identifier        :  Tajo_Identifier_Start ( Tajo_Identifier_Part )*;
fragment
Tajo_Identifier_Start  :  Simple_Latin_Letter;
fragment
Tajo_Identifier_Part   :  Simple_Latin_Letter | Digit | Underscore | Colon;

/*
==============================================================================================
 Whitespace Tokens
 ignore all control characters and non-Ascii characters that are not enclosed in quotes
==============================================================================================
*/

Space    :  ' '
{
	$channel=HIDDEN;
};

White_Space  :	( Control_Characters  | Extended_Control_Characters )+
{
	$channel=HIDDEN;
};


BAD : . {
  //error(UNKNOWN_CHARACTER, $text);
  skip();
} ;