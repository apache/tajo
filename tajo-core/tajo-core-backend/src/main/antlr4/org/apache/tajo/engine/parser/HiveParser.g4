/**
   Licensed to the Apache Software Foundation (ASF) under one or more 
   contributor license agreements.  See the NOTICE file distributed with 
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with 
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
parser grammar HiveParser;

options
{
tokenVocab=HiveLexer;
language=Java;
}

tokens {
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_SELECTDI;
TOK_SELEXPR;
TOK_FROM;
TOK_TAB;
TOK_PARTSPEC;
TOK_PARTVAL;
TOK_DIR;
TOK_LOCAL_DIR;
TOK_TABREF;
TOK_SUBQUERY;
TOK_INSERT_INTO;
TOK_DESTINATION;
TOK_ALLCOLREF;
TOK_TABLE_OR_COL;
TOK_FUNCTION;
TOK_FUNCTIONDI;
TOK_FUNCTIONSTAR;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_OP_DIV;
TOK_OP_ADD;
TOK_OP_SUB;
TOK_OP_MUL;
TOK_OP_MOD;
TOK_OP_BITAND;
TOK_OP_BITNOT;
TOK_OP_BITOR;
TOK_OP_BITXOR;
TOK_OP_AND;
TOK_OP_OR;
TOK_OP_NOT;
TOK_OP_LIKE;
TOK_TRUE;
TOK_FALSE;
TOK_TRANSFORM;
TOK_SERDE;
TOK_SERDENAME;
TOK_SERDEPROPS;
TOK_EXPLIST;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ROLLUP_GROUPBY;
TOK_CUBE_GROUPBY;
TOK_GROUPING_SETS;
TOK_GROUPING_SETS_EXPRESSION;
TOK_HAVING;
TOK_ORDERBY;
TOK_CLUSTERBY;
TOK_DISTRIBUTEBY;
TOK_SORTBY;
TOK_UNION;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_UNIQUEJOIN;
TOK_CROSSJOIN;
TOK_LOAD;
TOK_EXPORT;
TOK_IMPORT;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_TINYINT;
TOK_SMALLINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_DOUBLE;
TOK_DATE;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_STRING;
TOK_BINARY;
TOK_DECIMAL;
TOK_LIST;
TOK_STRUCT;
TOK_MAP;
TOK_UNIONTYPE;
TOK_COLTYPELIST;
TOK_CREATEDATABASE;
TOK_CREATETABLE;
TOK_TRUNCATETABLE;
TOK_CREATEINDEX;
TOK_CREATEINDEX_INDEXTBLNAME;
TOK_DEFERRED_REBUILDINDEX;
TOK_DROPINDEX;
TOK_DROPTABLE_PROPERTIES;
TOK_LIKETABLE;
TOK_DESCTABLE;
TOK_DESCFUNCTION;
TOK_ALTERTABLE_PARTITION;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;
TOK_ALTERTABLE_RENAMECOL;
TOK_ALTERTABLE_RENAMEPART;
TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_ADDPARTS;
TOK_ALTERTABLE_DROPPARTS;
TOK_ALTERTABLE_ALTERPARTS;
TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE;
TOK_ALTERTABLE_TOUCH;
TOK_ALTERTABLE_ARCHIVE;
TOK_ALTERTABLE_UNARCHIVE;
TOK_ALTERTABLE_SERDEPROPERTIES;
TOK_ALTERTABLE_SERIALIZER;
TOK_TABLE_PARTITION;
TOK_ALTERTABLE_FILEFORMAT;
TOK_ALTERTABLE_LOCATION;
TOK_ALTERTABLE_PROPERTIES;
TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION;
TOK_ALTERINDEX_REBUILD;
TOK_ALTERINDEX_PROPERTIES;
TOK_MSCK;
TOK_SHOWDATABASES;
TOK_SHOWTABLES;
TOK_SHOWCOLUMNS;
TOK_SHOWFUNCTIONS;
TOK_SHOWPARTITIONS;
TOK_SHOW_CREATETABLE;
TOK_SHOW_TABLESTATUS;
TOK_SHOW_TBLPROPERTIES;
TOK_SHOWLOCKS;
TOK_LOCKTABLE;
TOK_UNLOCKTABLE;
TOK_SWITCHDATABASE;
TOK_DROPDATABASE;
TOK_DROPTABLE;
TOK_DATABASECOMMENT;
TOK_TABCOLLIST;
TOK_TABCOL;
TOK_TABLECOMMENT;
TOK_TABLEPARTCOLS;
TOK_TABLEBUCKETS;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TBLORCFILE;
TOK_TBLSEQUENCEFILE;
TOK_TBLTEXTFILE;
TOK_TBLRCFILE;
TOK_TABLEFILEFORMAT;
TOK_FILEFORMAT_GENERIC;
TOK_OFFLINE;
TOK_ENABLE;
TOK_DISABLE;
TOK_READONLY;
TOK_NO_DROP;
TOK_STORAGEHANDLER;
TOK_ALTERTABLE_CLUSTER_SORT;
TOK_NOT_CLUSTERED;
TOK_NOT_SORTED;
TOK_TABCOLNAME;
TOK_TABLELOCATION;
TOK_PARTITIONLOCATION;
TOK_TABLEBUCKETSAMPLE;
TOK_TABLESPLITSAMPLE;
TOK_PERCENT;
TOK_LENGTH;
TOK_ROWCOUNT;
TOK_TMP_FILE;
TOK_TABSORTCOLNAMEASC;
TOK_TABSORTCOLNAMEDESC;
TOK_STRINGLITERALSEQUENCE;
TOK_CHARSETLITERAL;
TOK_CREATEFUNCTION;
TOK_DROPFUNCTION;
TOK_CREATEVIEW;
TOK_DROPVIEW;
TOK_ALTERVIEW_AS;
TOK_ALTERVIEW_PROPERTIES;
TOK_DROPVIEW_PROPERTIES;
TOK_ALTERVIEW_ADDPARTS;
TOK_ALTERVIEW_DROPPARTS;
TOK_ALTERVIEW_RENAME;
TOK_VIEWPARTCOLS;
TOK_EXPLAIN;
TOK_TABLESERIALIZER;
TOK_TABLEPROPERTIES;
TOK_TABLEPROPLIST;
TOK_INDEXPROPERTIES;
TOK_INDEXPROPLIST;
TOK_TABTYPE;
TOK_LIMIT;
TOK_TABLEPROPERTY;
TOK_IFEXISTS;
TOK_IFNOTEXISTS;
TOK_ORREPLACE;
TOK_HINTLIST;
TOK_HINT;
TOK_MAPJOIN;
TOK_STREAMTABLE;
TOK_HOLD_DDLTIME;
TOK_HINTARGLIST;
TOK_USERSCRIPTCOLNAMES;
TOK_USERSCRIPTCOLSCHEMA;
TOK_RECORDREADER;
TOK_RECORDWRITER;
TOK_LEFTSEMIJOIN;
TOK_LATERAL_VIEW;
TOK_TABALIAS;
TOK_ANALYZE;
TOK_CREATEROLE;
TOK_DROPROLE;
TOK_GRANT;
TOK_REVOKE;
TOK_SHOW_GRANT;
TOK_PRIVILEGE_LIST;
TOK_PRIVILEGE;
TOK_PRINCIPAL_NAME;
TOK_USER;
TOK_GROUP;
TOK_ROLE;
TOK_GRANT_WITH_OPTION;
TOK_PRIV_ALL;
TOK_PRIV_ALTER_METADATA;
TOK_PRIV_ALTER_DATA;
TOK_PRIV_DROP;
TOK_PRIV_INDEX;
TOK_PRIV_LOCK;
TOK_PRIV_SELECT;
TOK_PRIV_SHOW_DATABASE;
TOK_PRIV_CREATE;
TOK_PRIV_OBJECT;
TOK_PRIV_OBJECT_COL;
TOK_GRANT_ROLE;
TOK_REVOKE_ROLE;
TOK_SHOW_ROLE_GRANT;
TOK_SHOWINDEXES;
TOK_INDEXCOMMENT;
TOK_DESCDATABASE;
TOK_DATABASEPROPERTIES;
TOK_DATABASELOCATION;
TOK_DBPROPLIST;
TOK_ALTERDATABASE_PROPERTIES;
TOK_ALTERTABLE_ALTERPARTS_MERGEFILES;
TOK_TABNAME;
TOK_TABSRC;
TOK_RESTRICT;
TOK_CASCADE;
TOK_TABLESKEWED;
TOK_TABCOLVALUE;
TOK_TABCOLVALUE_PAIR;
TOK_TABCOLVALUES;
TOK_ALTERTABLE_SKEWED;
TOK_ALTERTBLPART_SKEWED_LOCATION;
TOK_SKEWED_LOCATIONS;
TOK_SKEWED_LOCATION_LIST;
TOK_SKEWED_LOCATION_MAP;
TOK_STOREDASDIRS;
TOK_PARTITIONINGSPEC;
TOK_PTBLFUNCTION;
TOK_WINDOWDEF;
TOK_WINDOWSPEC;
TOK_WINDOWVALUES;
TOK_WINDOWRANGE;
TOK_IGNOREPROTECTION;
}




// Package headers
@header {

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

}


@members {
  private static final Log LOG = LogFactory.getLog(HiveParser.class.getName());

  Stack msgs = new Stack<String>();

  private static HashMap<String, String> xlateMap;
  static {
    xlateMap = new HashMap<String, String>();

    // Keywords
    xlateMap.put("KW_TRUE", "TRUE");
    xlateMap.put("KW_FALSE", "FALSE");
    xlateMap.put("KW_ALL", "ALL");
    xlateMap.put("KW_AND", "AND");
    xlateMap.put("KW_OR", "OR");
    xlateMap.put("KW_NOT", "NOT");
    xlateMap.put("KW_LIKE", "LIKE");

    xlateMap.put("KW_ASC", "ASC");
    xlateMap.put("KW_DESC", "DESC");
    xlateMap.put("KW_ORDER", "ORDER");
    xlateMap.put("KW_BY", "BY");
    xlateMap.put("KW_GROUP", "GROUP");
    xlateMap.put("KW_WHERE", "WHERE");
    xlateMap.put("KW_FROM", "FROM");
    xlateMap.put("KW_AS", "AS");
    xlateMap.put("KW_SELECT", "SELECT");
    xlateMap.put("KW_DISTINCT", "DISTINCT");
    xlateMap.put("KW_INSERT", "INSERT");
    xlateMap.put("KW_OVERWRITE", "OVERWRITE");
    xlateMap.put("KW_OUTER", "OUTER");
    xlateMap.put("KW_JOIN", "JOIN");
    xlateMap.put("KW_LEFT", "LEFT");
    xlateMap.put("KW_RIGHT", "RIGHT");
    xlateMap.put("KW_FULL", "FULL");
    xlateMap.put("KW_ON", "ON");
    xlateMap.put("KW_PARTITION", "PARTITION");
    xlateMap.put("KW_PARTITIONS", "PARTITIONS");
    xlateMap.put("KW_TABLE", "TABLE");
    xlateMap.put("KW_TABLES", "TABLES");
    xlateMap.put("KW_TBLPROPERTIES", "TBLPROPERTIES");
    xlateMap.put("KW_SHOW", "SHOW");
    xlateMap.put("KW_MSCK", "MSCK");
    xlateMap.put("KW_DIRECTORY", "DIRECTORY");
    xlateMap.put("KW_LOCAL", "LOCAL");
    xlateMap.put("KW_TRANSFORM", "TRANSFORM");
    xlateMap.put("KW_USING", "USING");
    xlateMap.put("KW_CLUSTER", "CLUSTER");
    xlateMap.put("KW_DISTRIBUTE", "DISTRIBUTE");
    xlateMap.put("KW_SORT", "SORT");
    xlateMap.put("KW_UNION", "UNION");
    xlateMap.put("KW_LOAD", "LOAD");
    xlateMap.put("KW_DATA", "DATA");
    xlateMap.put("KW_INPATH", "INPATH");
    xlateMap.put("KW_IS", "IS");
    xlateMap.put("KW_NULL", "NULL");
    xlateMap.put("KW_CREATE", "CREATE");
    xlateMap.put("KW_EXTERNAL", "EXTERNAL");
    xlateMap.put("KW_ALTER", "ALTER");
    xlateMap.put("KW_DESCRIBE", "DESCRIBE");
    xlateMap.put("KW_DROP", "DROP");
    xlateMap.put("KW_REANME", "REANME");
    xlateMap.put("KW_TO", "TO");
    xlateMap.put("KW_COMMENT", "COMMENT");
    xlateMap.put("KW_BOOLEAN", "BOOLEAN");
    xlateMap.put("KW_TINYINT", "TINYINT");
    xlateMap.put("KW_SMALLINT", "SMALLINT");
    xlateMap.put("KW_INT", "INT");
    xlateMap.put("KW_BIGINT", "BIGINT");
    xlateMap.put("KW_FLOAT", "FLOAT");
    xlateMap.put("KW_DOUBLE", "DOUBLE");
    xlateMap.put("KW_DATE", "DATE");
    xlateMap.put("KW_DATETIME", "DATETIME");
    xlateMap.put("KW_TIMESTAMP", "TIMESTAMP");
    xlateMap.put("KW_STRING", "STRING");
    xlateMap.put("KW_BINARY", "BINARY");
    xlateMap.put("KW_ARRAY", "ARRAY");
    xlateMap.put("KW_MAP", "MAP");
    xlateMap.put("KW_REDUCE", "REDUCE");
    xlateMap.put("KW_PARTITIONED", "PARTITIONED");
    xlateMap.put("KW_CLUSTERED", "CLUSTERED");
    xlateMap.put("KW_SORTED", "SORTED");
    xlateMap.put("KW_INTO", "INTO");
    xlateMap.put("KW_BUCKETS", "BUCKETS");
    xlateMap.put("KW_ROW", "ROW");
    xlateMap.put("KW_FORMAT", "FORMAT");
    xlateMap.put("KW_DELIMITED", "DELIMITED");
    xlateMap.put("KW_FIELDS", "FIELDS");
    xlateMap.put("KW_TERMINATED", "TERMINATED");
    xlateMap.put("KW_COLLECTION", "COLLECTION");
    xlateMap.put("KW_ITEMS", "ITEMS");
    xlateMap.put("KW_KEYS", "KEYS");
    xlateMap.put("KW_KEY_TYPE", "\$KEY\$");
    xlateMap.put("KW_LINES", "LINES");
    xlateMap.put("KW_STORED", "STORED");
    xlateMap.put("KW_SEQUENCEFILE", "SEQUENCEFILE");
    xlateMap.put("KW_TEXTFILE", "TEXTFILE");
    xlateMap.put("KW_INPUTFORMAT", "INPUTFORMAT");
    xlateMap.put("KW_OUTPUTFORMAT", "OUTPUTFORMAT");
    xlateMap.put("KW_LOCATION", "LOCATION");
    xlateMap.put("KW_TABLESAMPLE", "TABLESAMPLE");
    xlateMap.put("KW_BUCKET", "BUCKET");
    xlateMap.put("KW_OUT", "OUT");
    xlateMap.put("KW_OF", "OF");
    xlateMap.put("KW_CAST", "CAST");
    xlateMap.put("KW_ADD", "ADD");
    xlateMap.put("KW_REPLACE", "REPLACE");
    xlateMap.put("KW_COLUMNS", "COLUMNS");
    xlateMap.put("KW_RLIKE", "RLIKE");
    xlateMap.put("KW_REGEXP", "REGEXP");
    xlateMap.put("KW_TEMPORARY", "TEMPORARY");
    xlateMap.put("KW_FUNCTION", "FUNCTION");
    xlateMap.put("KW_EXPLAIN", "EXPLAIN");
    xlateMap.put("KW_EXTENDED", "EXTENDED");
    xlateMap.put("KW_SERDE", "SERDE");
    xlateMap.put("KW_WITH", "WITH");
    xlateMap.put("KW_SERDEPROPERTIES", "SERDEPROPERTIES");
    xlateMap.put("KW_LIMIT", "LIMIT");
    xlateMap.put("KW_SET", "SET");
    xlateMap.put("KW_PROPERTIES", "TBLPROPERTIES");
    xlateMap.put("KW_VALUE_TYPE", "\$VALUE\$");
    xlateMap.put("KW_ELEM_TYPE", "\$ELEM\$");

    // Operators
    xlateMap.put("DOT", ".");
    xlateMap.put("COLON", ":");
    xlateMap.put("COMMA", ",");
    xlateMap.put("SEMICOLON", ");");

    xlateMap.put("LPAREN", "(");
    xlateMap.put("RPAREN", ")");
    xlateMap.put("LSQUARE", "[");
    xlateMap.put("RSQUARE", "]");

    xlateMap.put("EQUAL", "=");
    xlateMap.put("NOTEQUAL", "<>");
    xlateMap.put("EQUAL_NS", "<=>");
    xlateMap.put("LESSTHANOREQUALTO", "<=");
    xlateMap.put("LESSTHAN", "<");
    xlateMap.put("GREATERTHANOREQUALTO", ">=");
    xlateMap.put("GREATERTHAN", ">");

    xlateMap.put("DIVIDE", "/");
    xlateMap.put("PLUS", "+");
    xlateMap.put("MINUS", "-");
    xlateMap.put("STAR", "*");
xlateMap.put("MOD", "%");


    xlateMap.put("AMPERSAND", "&");
    xlateMap.put("TILDE", "~");
    xlateMap.put("BITWISEOR", "|");
    xlateMap.put("BITWISEXOR", "^");
    xlateMap.put("CharSetLiteral", "\\'");
  }

  public static Collection<String> getKeywords() {
    return xlateMap.values();
  }

  private static String xlate(String name) {

    String ret = xlateMap.get(name);
    if (ret == null) {
      ret = name;
    }

    return ret;
  }

}

@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...
selectClause
@init { msgs.push("select clause"); }
@after { msgs.pop(); }
    :
    KW_SELECT hintClause? (((KW_ALL | dist=KW_DISTINCT)? selectList)
                          | (transform=KW_TRANSFORM selectTrfmClause))
    |
    trfmClause
    ;

selectList
@init { msgs.push("select list"); }
@after { msgs.pop(); }
    :
    selectItem ( COMMA  selectItem )* 
    ;

selectTrfmClause
@init { msgs.push("transform clause"); }
@after { msgs.pop(); }
    :
    LPAREN selectExpressionList RPAREN
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    ;

hintClause
@init { msgs.push("hint clause"); }
@after { msgs.pop(); }
    :
    DIVIDE STAR PLUS hintList STAR DIVIDE 
    ;

hintList
@init { msgs.push("hint list"); }
@after { msgs.pop(); }
    :
    hintItem (COMMA hintItem)* 
    ;

hintItem
@init { msgs.push("hint item"); }
@after { msgs.pop(); }
    :
    hintName (LPAREN hintArgs RPAREN)? 
    ;

hintName
@init { msgs.push("hint name"); }
@after { msgs.pop(); }
    :
    KW_MAPJOIN 
    | KW_STREAMTABLE 
    | KW_HOLD_DDLTIME 
    ;

hintArgs
@init { msgs.push("hint arguments"); }
@after { msgs.pop(); }
    :
    hintArgName (COMMA hintArgName)* 
    ;

hintArgName
@init { msgs.push("hint argument name"); }
@after { msgs.pop(); }
    :
    identifier
    ;

selectItem
@init { msgs.push("selection target"); }
@after { msgs.pop(); }
    :
    ( selectExpression (KW_OVER ws=window_specification )?
      ((KW_AS? identifier) | (KW_AS LPAREN identifier (COMMA identifier)* RPAREN))?
    ) 
    ;

trfmClause
@init { msgs.push("transform clause"); }
@after { msgs.pop(); }
    :
    (   KW_MAP    selectExpressionList
      | KW_REDUCE selectExpressionList )
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    ;

selectExpression
@init { msgs.push("select expression"); }
@after { msgs.pop(); }
    :
    expression | tableAllColumns
    ;

selectExpressionList
@init { msgs.push("select expression list"); }
@after { msgs.pop(); }
    :
    selectExpression (COMMA selectExpression)* 
    ;


//---------------------- Rules for windowing clauses -------------------------------
window_clause 
@init { msgs.push("window_clause"); }
@after { msgs.pop(); } 
:
  KW_WINDOW window_defn (COMMA window_defn)* 
;  

window_defn 
@init { msgs.push("window_defn"); }
@after { msgs.pop(); } 
:
  Identifier KW_AS window_specification 
;  

window_specification 
@init { msgs.push("window_specification"); }
@after { msgs.pop(); } 
:
  (Identifier | ( LPAREN Identifier? partitioningSpec? window_frame? RPAREN)) 
;

window_frame :
 window_range_expression |
 window_value_expression
;

window_range_expression 
@init { msgs.push("window_range_expression"); }
@after { msgs.pop(); } 
:
 KW_ROWS sb=window_frame_start_boundary 
 KW_ROWS KW_BETWEEN s=window_frame_boundary KW_AND end=window_frame_boundary 
;

window_value_expression 
@init { msgs.push("window_value_expression"); }
@after { msgs.pop(); } 
:
 KW_RANGE sb=window_frame_start_boundary 
 KW_RANGE KW_BETWEEN s=window_frame_boundary KW_AND end=window_frame_boundary 
;

window_frame_start_boundary 
@init { msgs.push("windowframestartboundary"); }
@after { msgs.pop(); } 
:
  KW_UNBOUNDED KW_PRECEDING  
  KW_CURRENT KW_ROW  
  Number KW_PRECEDING 
;

window_frame_boundary 
@init { msgs.push("windowframeboundary"); }
@after { msgs.pop(); } 
:
  KW_UNBOUNDED (r=KW_PRECEDING|r=KW_FOLLOWING)  
  KW_CURRENT KW_ROW  
  Number (d=KW_PRECEDING | d=KW_FOLLOWING ) 
;   


tableAllColumns
    : STAR
    | tableName DOT STAR
    ;

// (table|column)
tableOrColumn
@init { msgs.push("table or column identifier"); }
@after { msgs.pop(); }
    :
    identifier 
    ;

expressionList
@init { msgs.push("expression list"); }
@after { msgs.pop(); }
    :
    expression (COMMA expression)* 
    ;

aliasList
@init { msgs.push("alias list"); }
@after { msgs.pop(); }
    :
    identifier (COMMA identifier)* 
    ;


//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
@init { msgs.push("from clause"); }
@after { msgs.pop(); }
    :
    KW_FROM joinSource 
    ;

joinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }
    : fromSource ( joinToken fromSource (KW_ON expression)? 
    )*
    | uniqueJoinToken uniqueJoinSource (COMMA uniqueJoinSource)+
    ;

uniqueJoinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }
    : KW_PRESERVE? fromSource uniqueJoinExpr
    ;

uniqueJoinExpr
@init { msgs.push("unique join expression list"); }
@after { msgs.pop(); }
    : LPAREN e1+=expression (COMMA e1+=expression)* RPAREN
    ;

uniqueJoinToken
@init { msgs.push("unique join"); }
@after { msgs.pop(); }
    : KW_UNIQUEJOIN 
;

joinToken
@init { msgs.push("join type specifier"); }
@after { msgs.pop(); }
    :
      KW_JOIN                    
    | KW_INNER  KW_JOIN            
    | KW_CROSS KW_JOIN            
    | KW_LEFT  KW_OUTER KW_JOIN   
    | KW_RIGHT KW_OUTER KW_JOIN  
    | KW_FULL  KW_OUTER KW_JOIN  
    | KW_LEFT  KW_SEMI  KW_JOIN  
    ;

lateralView
@init {msgs.push("lateral view"); }
@after {msgs.pop(); }
	:
	KW_LATERAL KW_VIEW function tableAlias KW_AS identifier (COMMA identifier)* 
	;

tableAlias
@init {msgs.push("table alias"); }
@after {msgs.pop(); }
    :
    identifier 
    ;

fromSource
@init { msgs.push("from source"); }
@after { msgs.pop(); }
    :
    ((Identifier LPAREN) | tableSource | subQuerySource) (lateralView)*
    ;

tableBucketSample
@init { msgs.push("table bucket sample specification"); }
@after { msgs.pop(); }
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON expr+=expression (COMMA expr+=expression)*)? RPAREN 
    ;

splitSample
@init { msgs.push("table split sample specification"); }
@after { msgs.pop(); }
    :
    KW_TABLESAMPLE LPAREN  (numerator=Number) (percent=KW_PERCENT|KW_ROWS) RPAREN
    |
    KW_TABLESAMPLE LPAREN  (numerator=ByteLengthLiteral) RPAREN
    ;

tableSample
@init { msgs.push("table sample specification"); }
@after { msgs.pop(); }
    :
    tableBucketSample |
    splitSample
    ;

tableSource
@init { msgs.push("table source"); }
@after { msgs.pop(); }
    : tabname=tableName (ts=tableSample)? (alias=identifier)?
    ;

tableName
@init { msgs.push("table name"); }
@after { msgs.pop(); }
    :
    db=identifier DOT tab=identifier
    |
    tab=identifier
    ;

viewName
@init { msgs.push("view name"); }
@after { msgs.pop(); }
    :
    (db=identifier DOT)? view=identifier
    ;

subQuerySource
@init { msgs.push("subquery source"); }
@after { msgs.pop(); }
    :
    LPAREN queryStatementExpression RPAREN identifier 
    ;

//---------------------- Rules for parsing PTF clauses -----------------------------
partitioningSpec
@init { msgs.push("partitioningSpec clause"); }
@after { msgs.pop(); } 
   :
   partitionByClause orderByClause? 
   orderByClause 
   distributeByClause sortByClause? 
   sortByClause 
   clusterByClause 
   ;

partitionTableFunctionSource
@init { msgs.push("partitionTableFunctionSource clause"); }
@after { msgs.pop(); } 
   :
   subQuerySource |
   tableSource |
   partitionedTableFunction
   ;

partitionedTableFunction
@init { msgs.push("ptf clause"); }
@after { msgs.pop(); } 
   :
   name=Identifier
   LPAREN KW_ON ptfsrc=partitionTableFunctionSource partitioningSpec?
     ((Identifier LPAREN expression RPAREN ) )? 
   RPAREN alias=Identifier? 
   ;

//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
@init { msgs.push("where clause"); }
@after { msgs.pop(); }
    :
    KW_WHERE searchCondition 
    ;

searchCondition
@init { msgs.push("search condition"); }
@after { msgs.pop(); }
    :
    expression
    ;

//-----------------------------------------------------------------------------------


// group by a,b
groupByClause
@init { msgs.push("group by clause"); }
@after { msgs.pop(); }
    :
    KW_GROUP KW_BY
    groupByExpression
    ( COMMA groupByExpression )*
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE)) ?
    (sets=KW_GROUPING KW_SETS 
    LPAREN groupingSetExpression ( COMMA groupingSetExpression)*  RPAREN ) ?
    ;

groupingSetExpression
@init {msgs.push("grouping set expression"); }
@after {msgs.pop(); }
   :
   groupByExpression
   |
   LPAREN 
   groupByExpression (COMMA groupByExpression)*
   RPAREN
   |
   LPAREN
   RPAREN
   ;


groupByExpression
@init { msgs.push("group by expression"); }
@after { msgs.pop(); }
    :
    expression
    ;

havingClause
@init { msgs.push("having clause"); }
@after { msgs.pop(); }
    :
    KW_HAVING havingCondition 
    ;

havingCondition
@init { msgs.push("having condition"); }
@after { msgs.pop(); }
    :
    expression
    ;

// order by a,b
orderByClause
@init { msgs.push("order by clause"); }
@after { msgs.pop(); }
    :
    KW_ORDER KW_BY
    LPAREN columnRefOrder
    ( COMMA columnRefOrder)* RPAREN 
    |
    KW_ORDER KW_BY
    columnRefOrder
    ( COMMA columnRefOrder)* 
    ;

clusterByClause
@init { msgs.push("cluster by clause"); }
@after { msgs.pop(); }
    :
    KW_CLUSTER KW_BY
    LPAREN expression (COMMA expression)* RPAREN
    |
    KW_CLUSTER KW_BY
    expression
    ((COMMA))*
    ;

partitionByClause
@init  { msgs.push("partition by clause"); }
@after { msgs.pop(); }
    :
    KW_PARTITION KW_BY
    LPAREN expression (COMMA expression)* RPAREN
    |
    KW_PARTITION KW_BY
    expression ((COMMA))*
    ;

distributeByClause
@init { msgs.push("distribute by clause"); }
@after { msgs.pop(); }
    :
    KW_DISTRIBUTE KW_BY
    LPAREN expression (COMMA expression)* RPAREN
    |
    KW_DISTRIBUTE KW_BY
    expression ((COMMA))*
    ;

sortByClause
@init { msgs.push("sort by clause"); }
@after { msgs.pop(); }
    :
    KW_SORT KW_BY
    LPAREN columnRefOrder
    ( COMMA columnRefOrder)* RPAREN 
    |
    KW_SORT KW_BY
    columnRefOrder
    ( (COMMA))*
    ;

// fun(par1, par2, par3)
function
@init { msgs.push("function specification"); }
@after { msgs.pop(); }
    :
    functionName
    LPAREN
      (
        (star=STAR)
        | (dist=KW_DISTINCT)? (selectExpression (COMMA selectExpression)*)?
      )
    RPAREN 
    ;

functionName
@init { msgs.push("function name"); }
@after { msgs.pop(); }
    : // Keyword IF is also a function name
    KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT | KW_UNIONTYPE | identifier
    ;

castExpression
@init { msgs.push("cast expression"); }
@after { msgs.pop(); }
    :
    KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN 
    ;

caseExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END 
    ;

whenExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE
     ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END 
    ;

constant
@init { msgs.push("constant"); }
@after { msgs.pop(); }
    :
    Number
    | StringLiteral
    | stringLiteralSequence
    | BigintLiteral
    | SmallintLiteral
    | TinyintLiteral
    | DecimalLiteral
    | charSetStringLiteral
    | booleanValue
    ;

stringLiteralSequence
    :
    StringLiteral StringLiteral+ 
    ;

charSetStringLiteral
@init { msgs.push("character string literal"); }
@after { msgs.pop(); }
    :
    csName=CharSetName csLiteral=CharSetLiteral 
    ;

expression
@init { msgs.push("expression specification"); }
@after { msgs.pop(); }
    :
    precedenceOrExpression
    ;

atomExpression
    :
    KW_NULL 
    | constant
    | function
    | castExpression
    | caseExpression
    | whenExpression
    | tableOrColumn
    | LPAREN expression RPAREN
    ;


precedenceFieldExpression
    :
    atomExpression ((LSQUARE expression RSQUARE) | (DOT identifier))*
    ;

precedenceUnaryOperator
    :
    PLUS | MINUS | TILDE
    ;

nullCondition
    :
    KW_NULL     
    | KW_NOT KW_NULL     
    ;

precedenceUnaryPrefixExpression
    :
    (precedenceUnaryOperator)* precedenceFieldExpression
    ;

precedenceUnarySuffixExpression
    : precedenceUnaryPrefixExpression (a=KW_IS nullCondition)?
    ;


precedenceBitwiseXorOperator
    :
    BITWISEXOR
    ;

precedenceBitwiseXorExpression
    :
    precedenceUnarySuffixExpression (precedenceBitwiseXorOperator precedenceUnarySuffixExpression)*
    ;


precedenceStarOperator
    :
    STAR | DIVIDE | MOD | DIV
    ;

precedenceStarExpression
    :
    precedenceBitwiseXorExpression (precedenceStarOperator precedenceBitwiseXorExpression)*
    ;


precedencePlusOperator
    :
    PLUS | MINUS
    ;

precedencePlusExpression
    :
    precedenceStarExpression (precedencePlusOperator precedenceStarExpression)*
    ;


precedenceAmpersandOperator
    :
    AMPERSAND
    ;

precedenceAmpersandExpression
    :
    precedencePlusExpression (precedenceAmpersandOperator precedencePlusExpression)*
    ;


precedenceBitwiseOrOperator
    :
    BITWISEOR
    ;

precedenceBitwiseOrExpression
    :
    precedenceAmpersandExpression (precedenceBitwiseOrOperator precedenceAmpersandExpression)*
    ;


// Equal operators supporting NOT prefix
precedenceEqualNegatableOperator
    :
    KW_LIKE | KW_RLIKE | KW_REGEXP
    ;

precedenceEqualOperator
    :
    precedenceEqualNegatableOperator | EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;

precedenceEqualExpression
    :
    (left=precedenceBitwiseOrExpression     
    )
    (
       (KW_NOT precedenceEqualNegatableOperator notExpr=precedenceBitwiseOrExpression) 
    | (precedenceEqualOperator equalExpr=precedenceBitwiseOrExpression)
    | (KW_NOT KW_IN expressions) 
    | (KW_IN expressions) 
    | ( KW_NOT KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) ) 
    | ( KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
    )*
    ;

expressions
    :
    LPAREN expression (COMMA expression)* RPAREN 
    ;

precedenceNotOperator
    :
    KW_NOT
    ;

precedenceNotExpression
    :
    (precedenceNotOperator)* precedenceEqualExpression
    ;


precedenceAndOperator
    :
    KW_AND
    ;

precedenceAndExpression
    :
    precedenceNotExpression (precedenceAndOperator precedenceNotExpression)*
    ;


precedenceOrOperator
    :
    KW_OR
    ;

precedenceOrExpression
    :
    precedenceAndExpression (precedenceOrOperator precedenceAndExpression)*
    ;


booleanValue
    :
    KW_TRUE | KW_FALSE
    ;

tableOrPartition
   :
   tableName partitionSpec? 
   ;

partitionSpec
    :
    KW_PARTITION
     LPAREN partitionVal (COMMA  partitionVal )* RPAREN 
    ;

partitionVal
    :
    identifier (EQUAL constant)? 
    ;

dropPartitionSpec
    :
    KW_PARTITION
     LPAREN dropPartitionVal (COMMA  dropPartitionVal )* RPAREN 
    ;

dropPartitionVal
    :
    identifier dropPartitionOperator constant 
    ;

dropPartitionOperator
    :
    EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;

sysFuncNames
    :
      KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_FLOAT
    | KW_DOUBLE
    | KW_BOOLEAN
    | KW_STRING
    | KW_BINARY
    | KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | KW_UNIONTYPE
    | EQUAL
    | EQUAL_NS
    | NOTEQUAL
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    | DIVIDE
    | PLUS
    | MINUS
    | STAR
    | MOD
    | DIV
    | AMPERSAND
    | TILDE
    | BITWISEOR
    | BITWISEXOR
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
    | KW_BETWEEN
    ;

descFuncNames
    :
      sysFuncNames
    | StringLiteral
    | identifier
    ;

identifier
    :
    Identifier
    | nonReserved 
    ;
    
nonReserved
    :
    KW_TRUE | KW_FALSE | KW_LIKE | KW_EXISTS | KW_ASC | KW_DESC | KW_ORDER | KW_GROUP | KW_BY | KW_AS | KW_INSERT | KW_OVERWRITE | KW_OUTER | KW_LEFT | KW_RIGHT | KW_FULL | KW_PARTITION | KW_PARTITIONS | KW_TABLE | KW_TABLES | KW_COLUMNS | KW_INDEX | KW_INDEXES | KW_REBUILD | KW_FUNCTIONS | KW_SHOW | KW_MSCK | KW_REPAIR | KW_DIRECTORY | KW_LOCAL | KW_USING | KW_CLUSTER | KW_DISTRIBUTE | KW_SORT | KW_UNION | KW_LOAD | KW_EXPORT | KW_IMPORT | KW_DATA | KW_INPATH | KW_IS | KW_NULL | KW_CREATE | KW_EXTERNAL | KW_ALTER | KW_CHANGE | KW_FIRST | KW_AFTER | KW_DESCRIBE | KW_DROP | KW_RENAME | KW_IGNORE | KW_PROTECTION | KW_TO | KW_COMMENT | KW_BOOLEAN | KW_TINYINT | KW_SMALLINT | KW_INT | KW_BIGINT | KW_FLOAT | KW_DOUBLE | KW_DATE | KW_DATETIME | KW_TIMESTAMP | KW_DECIMAL | KW_STRING | KW_ARRAY | KW_STRUCT | KW_UNIONTYPE | KW_PARTITIONED | KW_CLUSTERED | KW_SORTED | KW_INTO | KW_BUCKETS | KW_ROW | KW_ROWS | KW_FORMAT | KW_DELIMITED | KW_FIELDS | KW_TERMINATED | KW_ESCAPED | KW_COLLECTION | KW_ITEMS | KW_KEYS | KW_KEY_TYPE | KW_LINES | KW_STORED | KW_FILEFORMAT | KW_SEQUENCEFILE | KW_TEXTFILE | KW_RCFILE | KW_ORCFILE | KW_INPUTFORMAT | KW_OUTPUTFORMAT | KW_INPUTDRIVER | KW_OUTPUTDRIVER | KW_OFFLINE | KW_ENABLE | KW_DISABLE | KW_READONLY | KW_NO_DROP | KW_LOCATION | KW_BUCKET | KW_OUT | KW_OF | KW_PERCENT | KW_ADD | KW_REPLACE | KW_RLIKE | KW_REGEXP | KW_TEMPORARY | KW_EXPLAIN | KW_FORMATTED | KW_PRETTY | KW_DEPENDENCY | KW_SERDE | KW_WITH | KW_DEFERRED | KW_SERDEPROPERTIES | KW_DBPROPERTIES | KW_LIMIT | KW_SET | KW_UNSET | KW_TBLPROPERTIES | KW_IDXPROPERTIES | KW_VALUE_TYPE | KW_ELEM_TYPE | KW_MAPJOIN | KW_STREAMTABLE | KW_HOLD_DDLTIME | KW_CLUSTERSTATUS | KW_UTC | KW_UTCTIMESTAMP | KW_LONG | KW_DELETE | KW_PLUS | KW_MINUS | KW_FETCH | KW_INTERSECT | KW_VIEW | KW_IN | KW_DATABASES | KW_MATERIALIZED | KW_SCHEMA | KW_SCHEMAS | KW_GRANT | KW_REVOKE | KW_SSL | KW_UNDO | KW_LOCK | KW_LOCKS | KW_UNLOCK | KW_SHARED | KW_EXCLUSIVE | KW_PROCEDURE | KW_UNSIGNED | KW_WHILE | KW_READ | KW_READS | KW_PURGE | KW_RANGE | KW_ANALYZE | KW_BEFORE | KW_BETWEEN | KW_BOTH | KW_BINARY | KW_CONTINUE | KW_CURSOR | KW_TRIGGER | KW_RECORDREADER | KW_RECORDWRITER | KW_SEMI | KW_LATERAL | KW_TOUCH | KW_ARCHIVE | KW_UNARCHIVE | KW_COMPUTE | KW_STATISTICS | KW_USE | KW_OPTION | KW_CONCATENATE | KW_SHOW_DATABASE | KW_UPDATE | KW_RESTRICT | KW_CASCADE | KW_SKEWED | KW_ROLLUP | KW_CUBE | KW_DIRECTORIES | KW_FOR | KW_GROUPING | KW_SETS | KW_TRUNCATE | KW_NOSCAN | KW_USER | KW_ROLE | KW_INNER
    ;

//-----------------------------------------------------------------------------------

// starting rule
statement
	: explainStatement EOF
	| execStatement EOF
	;

explainStatement
@init { msgs.push("explain statement"); }
@after { msgs.pop(); }
	: KW_EXPLAIN (explainOptions=KW_EXTENDED|explainOptions=KW_FORMATTED|explainOptions=KW_DEPENDENCY)? execStatement
	;

execStatement
@init { msgs.push("statement"); }
@after { msgs.pop(); }
    : queryStatementExpression
    | loadStatement
    | exportStatement
    | importStatement
    | ddlStatement
    ;

loadStatement
@init { msgs.push("load statement"); }
@after { msgs.pop(); }
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tableOrPartition)
    ;

exportStatement
@init { msgs.push("export statement"); }
@after { msgs.pop(); }
    : KW_EXPORT KW_TABLE (tab=tableOrPartition) KW_TO (path=StringLiteral)
    ;

importStatement
@init { msgs.push("import statement"); }
@after { msgs.pop(); }
	: KW_IMPORT ((ext=KW_EXTERNAL)? KW_TABLE (tab=tableOrPartition))? KW_FROM (path=StringLiteral) tableLocation?
    ;

ddlStatement
@init { msgs.push("ddl statement"); }
@after { msgs.pop(); }
    : createDatabaseStatement
    | switchDatabaseStatement
    | dropDatabaseStatement
    | createTableStatement
    | dropTableStatement
    | truncateTableStatement
    | alterStatement
    | descStatement
    | showStatement
    | metastoreCheck
    | createViewStatement
    | dropViewStatement
    | createFunctionStatement
    | createIndexStatement
    | dropIndexStatement
    | dropFunctionStatement
    | analyzeStatement
    | lockStatement
    | unlockStatement
    | createRoleStatement
    | dropRoleStatement
    | grantPrivileges
    | revokePrivileges
    | showGrants
    | showRoleGrants
    | grantRole
    | revokeRole
    ;

ifExists
@init { msgs.push("if exists clause"); }
@after { msgs.pop(); }
    : KW_IF KW_EXISTS
    ;

restrictOrCascade
@init { msgs.push("restrict or cascade clause"); }
@after { msgs.pop(); }
    : KW_RESTRICT
    | KW_CASCADE
    ;

ifNotExists
@init { msgs.push("if not exists clause"); }
@after { msgs.pop(); }
    : KW_IF KW_NOT KW_EXISTS
    ;

storedAsDirs
@init { msgs.push("stored as directories"); }
@after { msgs.pop(); }
    : KW_STORED KW_AS KW_DIRECTORIES
    ;

orReplace
@init { msgs.push("or replace clause"); }
@after { msgs.pop(); }
    : KW_OR KW_REPLACE
    ;

ignoreProtection
@init { msgs.push("ignore protection clause"); }
@after { msgs.pop(); }
        : KW_IGNORE KW_PROTECTION
        ;

createDatabaseStatement
@init { msgs.push("create database statement"); }
@after { msgs.pop(); }
    : KW_CREATE (KW_DATABASE|KW_SCHEMA)
        ifNotExists?
        name=identifier
        databaseComment?
        dbLocation?
        (KW_WITH KW_DBPROPERTIES dbprops=dbProperties)?
    ;

dbLocation
@init { msgs.push("database location specification"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral 
    ;

dbProperties
@init { msgs.push("dbproperties"); }
@after { msgs.pop(); }
    :
      LPAREN dbPropertiesList RPAREN 
    ;

dbPropertiesList
@init { msgs.push("database properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* 
    ;


switchDatabaseStatement
@init { msgs.push("switch database statement"); }
@after { msgs.pop(); }
    : KW_USE identifier
    ;

dropDatabaseStatement
@init { msgs.push("drop database statement"); }
@after { msgs.pop(); }
    : KW_DROP (KW_DATABASE|KW_SCHEMA) ifExists? identifier restrictOrCascade?
    ;

databaseComment
@init { msgs.push("database's comment"); }
@after { msgs.pop(); }
    : KW_COMMENT comment=StringLiteral
    ;

createTableStatement
@init { msgs.push("create table statement"); }
@after { msgs.pop(); }
    : KW_CREATE (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=tableName
      (  like=KW_LIKE likeName=tableName
         tableLocation?
         tablePropertiesPrefixed?
       | (LPAREN columnNameTypeList RPAREN)?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatement)?
      )
    ;

truncateTableStatement
@init { msgs.push("truncate table statement"); }
@after { msgs.pop(); }
    : KW_TRUNCATE KW_TABLE tablePartitionPrefix 
;

createIndexStatement
@init { msgs.push("create index statement");}
@after {msgs.pop();}
    : KW_CREATE KW_INDEX indexName=identifier
      KW_ON KW_TABLE tab=tableName LPAREN indexedCols=columnNameList RPAREN
      KW_AS typeName=StringLiteral
      autoRebuild?
      indexPropertiesPrefixed?
      indexTblName?
      tableRowFormat?
      tableFileFormat?
      tableLocation?
      tablePropertiesPrefixed?
      indexComment?
    ;

indexComment
@init { msgs.push("comment on an index");}
@after {msgs.pop();}
        :
                KW_COMMENT comment=StringLiteral  
        ;

autoRebuild
@init { msgs.push("auto rebuild index");}
@after {msgs.pop();}
    : KW_WITH KW_DEFERRED KW_REBUILD
    ;

indexTblName
@init { msgs.push("index table name");}
@after {msgs.pop();}
    : KW_IN KW_TABLE indexTbl=tableName
    ;

indexPropertiesPrefixed
@init { msgs.push("table properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_IDXPROPERTIES indexProperties
    ;

indexProperties
@init { msgs.push("index properties"); }
@after { msgs.pop(); }
    :
      LPAREN indexPropertiesList RPAREN 
    ;

indexPropertiesList
@init { msgs.push("index properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* 
    ;

dropIndexStatement
@init { msgs.push("drop index statement");}
@after {msgs.pop();}
    : KW_DROP KW_INDEX ifExists? indexName=identifier KW_ON tab=tableName
    ;

dropTableStatement
@init { msgs.push("drop statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TABLE ifExists? tableName 
    ;

alterStatement
@init { msgs.push("alter statement"); }
@after { msgs.pop(); }
    : 
    KW_ALTER
        (
            KW_TABLE alterTableStatementSuffix
        |
            KW_VIEW alterViewStatementSuffix
        |
            KW_INDEX alterIndexStatementSuffix
        |
            KW_DATABASE alterDatabaseStatementSuffix
        )
    ;

alterTableStatementSuffix
@init { msgs.push("alter table statement"); }
@after { msgs.pop(); }
    : alterStatementSuffixRename
    | alterStatementSuffixAddCol
    | alterStatementSuffixRenameCol
    | alterStatementSuffixDropPartitions
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterTblPartitionStatement
    | alterStatementSuffixSkewedby
    ;

alterViewStatementSuffix
@init { msgs.push("alter view statement"); }
@after { msgs.pop(); }
    : alterViewSuffixProperties
    | alterStatementSuffixRename
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixDropPartitions
    | name=tableName KW_AS selectStatement
    ;

alterIndexStatementSuffix
@init { msgs.push("alter index statement"); }
@after { msgs.pop(); }
    : indexName=identifier
      (KW_ON tableNameId=identifier)
      partitionSpec?
    (
      KW_REBUILD
    |
      KW_SET KW_IDXPROPERTIES
      indexProperties
    )
    ;

alterDatabaseStatementSuffix
@init { msgs.push("alter database statement"); }
@after { msgs.pop(); }
    : alterDatabaseSuffixProperties
    ;

alterDatabaseSuffixProperties
@init { msgs.push("alter database properties statement"); }
@after { msgs.pop(); }
    : name=identifier KW_SET KW_DBPROPERTIES dbProperties
    ;

alterStatementSuffixRename
@init { msgs.push("rename statement"); }
@after { msgs.pop(); }
    : oldName=identifier KW_RENAME KW_TO newName=identifier
    ;

alterStatementSuffixAddCol
@init { msgs.push("add column statement"); }
@after { msgs.pop(); }
    : identifier (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    ;

alterStatementSuffixRenameCol
@init { msgs.push("rename column name"); }
@after { msgs.pop(); }
    : identifier KW_CHANGE KW_COLUMN? oldName=identifier newName=identifier colType (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition?
    ;

alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=identifier
    ;

alterStatementSuffixAddPartitions
@init { msgs.push("add partition statement"); }
@after { msgs.pop(); }
    : identifier KW_ADD ifNotExists? partitionSpec partitionLocation? (partitionSpec partitionLocation?)*
    ;

alterStatementSuffixTouch
@init { msgs.push("touch statement"); }
@after { msgs.pop(); }
    : identifier KW_TOUCH (partitionSpec)*
    ;

alterStatementSuffixArchive
@init { msgs.push("archive statement"); }
@after { msgs.pop(); }
    : identifier KW_ARCHIVE (partitionSpec)*
    ;

alterStatementSuffixUnArchive
@init { msgs.push("unarchive statement"); }
@after { msgs.pop(); }
    : identifier KW_UNARCHIVE (partitionSpec)*
    ;

partitionLocation
@init { msgs.push("partition location"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral 
    ;

alterStatementSuffixDropPartitions
@init { msgs.push("drop partition statement"); }
@after { msgs.pop(); }
    : identifier KW_DROP ifExists? dropPartitionSpec (COMMA dropPartitionSpec)* ignoreProtection?
    ;

alterStatementSuffixProperties
@init { msgs.push("alter properties statement"); }
@after { msgs.pop(); }
    : name=identifier KW_SET KW_TBLPROPERTIES tableProperties
    | name=identifier KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    ;

alterViewSuffixProperties
@init { msgs.push("alter view properties statement"); }
@after { msgs.pop(); }
    : name=identifier KW_SET KW_TBLPROPERTIES tableProperties
    | name=identifier KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    ;

alterStatementSuffixSerdeProperties
@init { msgs.push("alter serdes statement"); }
@after { msgs.pop(); }
    : KW_SET KW_SERDE serdeName=StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    | KW_SET KW_SERDEPROPERTIES tableProperties
    ;

tablePartitionPrefix
@init {msgs.push("table partition prefix");}
@after {msgs.pop();}
  :name=identifier partitionSpec?
  ;

alterTblPartitionStatement
@init {msgs.push("alter table partition statement");}
@after {msgs.pop();}
  : tablePartitionPrefix alterTblPartitionStatementSuffix
  |Identifier KW_PARTITION KW_COLUMN LPAREN columnNameType RPAREN
  ;

alterTblPartitionStatementSuffix
@init {msgs.push("alter table partition statement suffix");}
@after {msgs.pop();}
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixProtectMode
  | alterStatementSuffixMergeFiles
  | alterStatementSuffixSerdeProperties
  | alterStatementSuffixRenamePart
  | alterStatementSuffixBucketNum
  | alterTblPartitionStatementSuffixSkewedLocation
  | alterStatementSuffixClusterbySortby
  ;

alterStatementSuffixFileFormat
@init {msgs.push("alter fileformat statement"); }
@after {msgs.pop();}
	: KW_SET KW_FILEFORMAT fileFormat
	;

alterStatementSuffixClusterbySortby
@init {msgs.push("alter partition cluster by sort by statement");}
@after {msgs.pop();}
  : KW_NOT KW_CLUSTERED 
  | KW_NOT KW_SORTED 
  | tableBuckets 
  ;

alterTblPartitionStatementSuffixSkewedLocation
@init {msgs.push("alter partition skewed location");}
@after {msgs.pop();}
  : KW_SET KW_SKEWED KW_LOCATION skewedLocations
  ;
  
skewedLocations
@init { msgs.push("skewed locations"); }
@after { msgs.pop(); }
    :
      LPAREN skewedLocationsList RPAREN 
    ;

skewedLocationsList
@init { msgs.push("skewed locations list"); }
@after { msgs.pop(); }
    :
      skewedLocationMap (COMMA skewedLocationMap)* 
    ;

skewedLocationMap
@init { msgs.push("specifying skewed location map"); }
@after { msgs.pop(); }
    :
      key=skewedValueLocationElement EQUAL value=StringLiteral 
    ;

alterStatementSuffixLocation
@init {msgs.push("alter location");}
@after {msgs.pop();}
  : KW_SET KW_LOCATION newLoc=StringLiteral
  ;

	
alterStatementSuffixSkewedby
@init {msgs.push("alter skewed by statement");}
@after{msgs.pop();}
	:name=identifier tableSkewed
	|
	name=identifier KW_NOT KW_SKEWED
	|
	name=identifier KW_NOT storedAsDirs
	;

alterStatementSuffixProtectMode
@init { msgs.push("alter partition protect mode statement"); }
@after { msgs.pop(); }
    : alterProtectMode
    ;

alterStatementSuffixRenamePart
@init { msgs.push("alter table rename partition statement"); }
@after { msgs.pop(); }
    : KW_RENAME KW_TO partitionSpec
    ;

alterStatementSuffixMergeFiles
@init { msgs.push(""); }
@after { msgs.pop(); }
    : KW_CONCATENATE
    ;

alterProtectMode
@init { msgs.push("protect mode specification enable"); }
@after { msgs.pop(); }
    : KW_ENABLE alterProtectModeMode  
    | KW_DISABLE alterProtectModeMode  
    ;

alterProtectModeMode
@init { msgs.push("protect mode specification enable"); }
@after { msgs.pop(); }
    : KW_OFFLINE  
    | KW_NO_DROP KW_CASCADE? 
    | KW_READONLY  
    ;

alterStatementSuffixBucketNum
@init { msgs.push(""); }
@after { msgs.pop(); }
    : KW_INTO num=Number KW_BUCKETS
    ;

fileFormat
@init { msgs.push("file format specification"); }
@after { msgs.pop(); }
    : KW_SEQUENCEFILE  
    | KW_TEXTFILE  
    | KW_RCFILE  
    | KW_ORCFILE 
    | KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
    | genericSpec=identifier 
    ;

tabTypeExpr
@init { msgs.push("specifying table types"); }
@after { msgs.pop(); }

   : 
   identifier (DOT (KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE | identifier))*
   ;

descTabTypeExpr
@init { msgs.push("specifying describe table types"); }
@after { msgs.pop(); }

   : 
   identifier (DOT (KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE | identifier))* identifier?
   ;

partTypeExpr
@init { msgs.push("specifying table partitions"); }
@after { msgs.pop(); }
    :  tabTypeExpr partitionSpec? 
    ;

descPartTypeExpr
@init { msgs.push("specifying describe table partitions"); }
@after { msgs.pop(); }
    :  descTabTypeExpr partitionSpec? 
    ;

descStatement
@init { msgs.push("describe statement"); }
@after { msgs.pop(); }
    : (KW_DESCRIBE|KW_DESC) (descOptions=KW_FORMATTED|descOptions=KW_EXTENDED|descOptions=KW_PRETTY)? (parttype=descPartTypeExpr) 
    | (KW_DESCRIBE|KW_DESC) KW_FUNCTION KW_EXTENDED? (name=descFuncNames) 
    | (KW_DESCRIBE|KW_DESC) KW_DATABASE KW_EXTENDED? (dbName=identifier) 
    ;

analyzeStatement
@init { msgs.push("analyze statement"); }
@after { msgs.pop(); }
    : KW_ANALYZE KW_TABLE (parttype=tableOrPartition) KW_COMPUTE KW_STATISTICS ((noscan=KW_NOSCAN) | (partialscan=KW_PARTIALSCAN) | (KW_FOR KW_COLUMNS statsColumnName=columnNameList))? 
    ;

showStatement
@init { msgs.push("show statement"); }
@after { msgs.pop(); }
    : KW_SHOW (KW_DATABASES|KW_SCHEMAS) (KW_LIKE showStmtIdentifier)? 
    | KW_SHOW KW_TABLES ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  
    | KW_SHOW KW_COLUMNS (KW_FROM|KW_IN) tabname=tableName ((KW_FROM|KW_IN) db_name=identifier)? 
    | KW_SHOW KW_FUNCTIONS showStmtIdentifier?  
    | KW_SHOW KW_PARTITIONS identifier partitionSpec? 
    | KW_SHOW KW_CREATE KW_TABLE tabName=tableName 
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    | KW_SHOW KW_TBLPROPERTIES tblName=identifier (LPAREN prptyName=StringLiteral RPAREN)? 
    | KW_SHOW KW_LOCKS (parttype=partTypeExpr)? (isExtended=KW_EXTENDED)? 
    | KW_SHOW (showOptions=KW_FORMATTED)? (KW_INDEX|KW_INDEXES) KW_ON showStmtIdentifier ((KW_FROM|KW_IN) db_name=identifier)?
    ;

lockStatement
@init { msgs.push("lock statement"); }
@after { msgs.pop(); }
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode 
    ;

lockMode
@init { msgs.push("lock mode"); }
@after { msgs.pop(); }
    : KW_SHARED | KW_EXCLUSIVE
    ;

unlockStatement
@init { msgs.push("unlock statement"); }
@after { msgs.pop(); }
    : KW_UNLOCK KW_TABLE tableName partitionSpec?  
    ;

createRoleStatement
@init { msgs.push("create role"); }
@after { msgs.pop(); }
    : KW_CREATE KW_ROLE roleName=identifier
    ;

dropRoleStatement
@init {msgs.push("drop role");}
@after {msgs.pop();}
    : KW_DROP KW_ROLE roleName=identifier
    ;

grantPrivileges
@init {msgs.push("grant privileges");}
@after {msgs.pop();}
    : KW_GRANT privList=privilegeList
      privilegeObject?
      KW_TO principalSpecification
      (KW_WITH withOption)?
    ;

revokePrivileges
@init {msgs.push("revoke privileges");}
@afer {msgs.pop();}
    : KW_REVOKE privilegeList privilegeObject? KW_FROM principalSpecification
    ;

grantRole
@init {msgs.push("grant role");}
@after {msgs.pop();}
    : KW_GRANT KW_ROLE identifier (COMMA identifier)* KW_TO principalSpecification
    ;

revokeRole
@init {msgs.push("revoke role");}
@after {msgs.pop();}
    : KW_REVOKE KW_ROLE identifier (COMMA identifier)* KW_FROM principalSpecification
    ;

showRoleGrants
@init {msgs.push("show role grants");}
@after {msgs.pop();}
    : KW_SHOW KW_ROLE KW_GRANT principalName
    ;

showGrants
@init {msgs.push("show grants");}
@after {msgs.pop();}
    : KW_SHOW KW_GRANT principalName privilegeIncludeColObject?
    ;

privilegeIncludeColObject
@init {msgs.push("privilege object including columns");}
@after {msgs.pop();}
    : KW_ON (table=KW_TABLE|KW_DATABASE) identifier (LPAREN cols=columnNameList RPAREN)? partitionSpec?
    ;

privilegeObject
@init {msgs.push("privilege subject");}
@after {msgs.pop();}
    : KW_ON (table=KW_TABLE|KW_DATABASE) identifier partitionSpec?
    ;

privilegeList
@init {msgs.push("grant privilege list");}
@after {msgs.pop();}
    : privlegeDef (COMMA privlegeDef)*
    ;

privlegeDef
@init {msgs.push("grant privilege");}
@after {msgs.pop();}
    : privilegeType (LPAREN cols=columnNameList RPAREN)?
    ;

privilegeType
@init {msgs.push("privilege type");}
@after {msgs.pop();}
    : KW_ALL 
    | KW_ALTER 
    | KW_UPDATE 
    | KW_CREATE 
    | KW_DROP 
    | KW_INDEX 
    | KW_LOCK 
    | KW_SELECT 
    | KW_SHOW_DATABASE 
    ;

principalSpecification
@init { msgs.push("user/group/role name list"); }
@after { msgs.pop(); }
    : principalName (COMMA principalName)* 
    ;

principalName
@init {msgs.push("user|group|role name");}
@after {msgs.pop();}
    : KW_USER identifier 
    | KW_GROUP identifier 
    | KW_ROLE identifier 
    ;

withOption
@init {msgs.push("grant with option");}
@after {msgs.pop();}
    : KW_GRANT KW_OPTION
    ;

metastoreCheck
@init { msgs.push("metastore check statement"); }
@after { msgs.pop(); }
    : KW_MSCK (repair=KW_REPAIR)? (KW_TABLE table=identifier partitionSpec? (COMMA partitionSpec)*)?
    ;

createFunctionStatement
@init { msgs.push("create function statement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_TEMPORARY KW_FUNCTION identifier KW_AS StringLiteral
    ;

dropFunctionStatement
@init { msgs.push("drop temporary function statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TEMPORARY KW_FUNCTION ifExists? identifier
    ;

createViewStatement
@init {
    msgs.push("create view statement");
}
@after { msgs.pop(); }
    : KW_CREATE (orReplace)? KW_VIEW (ifNotExists)? name=tableName
        (LPAREN columnNameCommentList RPAREN)? tableComment? viewPartition?
        tablePropertiesPrefixed?
        KW_AS
        selectStatement
    ;

viewPartition
@init { msgs.push("view partition specification"); }
@after { msgs.pop(); }
    : KW_PARTITIONED KW_ON LPAREN columnNameList RPAREN
    ;

dropViewStatement
@init { msgs.push("drop view statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_VIEW ifExists? viewName 
    ;

showStmtIdentifier
@init { msgs.push("identifier for show statement"); }
@after { msgs.pop(); }
    : identifier
    | StringLiteral
    ;

tableComment
@init { msgs.push("table's comment"); }
@after { msgs.pop(); }
    :
      KW_COMMENT comment=StringLiteral  
    ;

tablePartition
@init { msgs.push("table partition specification"); }
@after { msgs.pop(); }
    : KW_PARTITIONED KW_BY LPAREN columnNameTypeList RPAREN
    ;

tableBuckets
@init { msgs.push("table buckets specification"); }
@after { msgs.pop(); }
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS
    ;

tableSkewed
@init { msgs.push("table skewed specification"); }
@after { msgs.pop(); }
    :
     KW_SKEWED KW_BY LPAREN skewedCols=columnNameList RPAREN KW_ON LPAREN (skewedValues=skewedValueElement) RPAREN (storedAsDirs)?
    ;

rowFormat
@init { msgs.push("serde specification"); }
@after { msgs.pop(); }
    : rowFormatSerde 
    | rowFormatDelimited 
    ;

recordReader
@init { msgs.push("record reader specification"); }
@after { msgs.pop(); }
    : KW_RECORDREADER StringLiteral 
    ;

recordWriter
@init { msgs.push("record writer specification"); }
@after { msgs.pop(); }
    : KW_RECORDWRITER StringLiteral 
    ;

rowFormatSerde
@init { msgs.push("serde format specification"); }
@after { msgs.pop(); }
    : KW_ROW KW_FORMAT KW_SERDE name=StringLiteral (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
    ;

rowFormatDelimited
@init { msgs.push("serde properties specification"); }
@after { msgs.pop(); }
    :
      KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier?
    ;

tableRowFormat
@init { msgs.push("table row format specification"); }
@after { msgs.pop(); }
    :
      rowFormatDelimited
    | rowFormatSerde
    ;

tablePropertiesPrefixed
@init { msgs.push("table properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_TBLPROPERTIES tableProperties
    ;

tableProperties
@init { msgs.push("table properties"); }
@after { msgs.pop(); }
    :
      LPAREN tablePropertiesList RPAREN 
    ;

tablePropertiesList
@init { msgs.push("table properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* 
    |
      keyProperty (COMMA keyProperty)* 
    ;

keyValueProperty
@init { msgs.push("specifying key/value property"); }
@after { msgs.pop(); }
    :
      key=StringLiteral EQUAL value=StringLiteral 
    ;

keyProperty
@init { msgs.push("specifying key property"); }
@after { msgs.pop(); }
    :
      key=StringLiteral 
    ;

tableRowFormatFieldIdentifier
@init { msgs.push("table row format's field separator"); }
@after { msgs.pop(); }
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=StringLiteral (KW_ESCAPED KW_BY fldEscape=StringLiteral)?
    ;

tableRowFormatCollItemsIdentifier
@init { msgs.push("table row format's column separator"); }
@after { msgs.pop(); }
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=StringLiteral
    ;

tableRowFormatMapKeysIdentifier
@init { msgs.push("table row format's map key separator"); }
@after { msgs.pop(); }
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=StringLiteral
    ;

tableRowFormatLinesIdentifier
@init { msgs.push("table row format's line separator"); }
@after { msgs.pop(); }
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=StringLiteral
    ;

tableFileFormat
@init { msgs.push("table file format specification"); }
@after { msgs.pop(); }
    :
      KW_STORED KW_AS KW_SEQUENCEFILE  
      | KW_STORED KW_AS KW_TEXTFILE  
      | KW_STORED KW_AS KW_RCFILE  
      | KW_STORED KW_AS KW_ORCFILE 
      | KW_STORED KW_AS KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      | KW_STORED KW_BY storageHandler=StringLiteral
         (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
      | KW_STORED KW_AS genericSpec=identifier
    ;

tableLocation
@init { msgs.push("table location specification"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral 
    ;

columnNameTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameType (COMMA columnNameType)* 
    ;

columnNameColonTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameColonType (COMMA columnNameColonType)* 
    ;

columnNameList
@init { msgs.push("column name list"); }
@after { msgs.pop(); }
    : columnName (COMMA columnName)* 
    ;

columnName
@init { msgs.push("column name"); }
@after { msgs.pop(); }
    :
      identifier
    ;

columnNameOrderList
@init { msgs.push("column name order list"); }
@after { msgs.pop(); }
    : columnNameOrder (COMMA columnNameOrder)* 
    ;

skewedValueElement
@init { msgs.push("skewed value element"); }
@after { msgs.pop(); }
    : 
      skewedColumnValues
     | skewedColumnValuePairList
    ;

skewedColumnValuePairList
@init { msgs.push("column value pair list"); }
@after { msgs.pop(); }
    : skewedColumnValuePair (COMMA skewedColumnValuePair)* 
    ;

skewedColumnValuePair
@init { msgs.push("column value pair"); }
@after { msgs.pop(); }
    : 
      LPAREN colValues=skewedColumnValues RPAREN 
    ;

skewedColumnValues
@init { msgs.push("column values"); }
@after { msgs.pop(); }
    : skewedColumnValue (COMMA skewedColumnValue)* 
    ;

skewedColumnValue
@init { msgs.push("column value"); }
@after { msgs.pop(); }
    :
      constant
    ;

skewedValueLocationElement
@init { msgs.push("skewed value location element"); }
@after { msgs.pop(); }
    : 
      skewedColumnValue
     | skewedColumnValuePair
    ;
    
columnNameOrder
@init { msgs.push("column name order"); }
@after { msgs.pop(); }
    : identifier (asc=KW_ASC | desc=KW_DESC)?
    ;

columnNameCommentList
@init { msgs.push("column name comment list"); }
@after { msgs.pop(); }
    : columnNameComment (COMMA columnNameComment)* 
    ;

columnNameComment
@init { msgs.push("column name comment"); }
@after { msgs.pop(); }
    : colName=identifier (KW_COMMENT comment=StringLiteral)?
    ;

columnRefOrder
@init { msgs.push("column order"); }
@after { msgs.pop(); }
    : expression (asc=KW_ASC | desc=KW_DESC)?
    ;

columnNameType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=identifier colType (KW_COMMENT comment=StringLiteral)?
    ;

columnNameColonType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=identifier COLON colType (KW_COMMENT comment=StringLiteral)?
    ;

colType
@init { msgs.push("column type"); }
@after { msgs.pop(); }
    : type
    ;

colTypeList
@init { msgs.push("column type list"); }
@after { msgs.pop(); }
    : colType (COMMA colType)* 
    ;

type
    : primitiveType
    | listType
    | structType
    | mapType
    | unionType;

primitiveType
@init { msgs.push("primitive type specification"); }
@after { msgs.pop(); }
    : KW_TINYINT       
    | KW_SMALLINT      
    | KW_INT           
    | KW_BIGINT        
    | KW_BOOLEAN       
    | KW_FLOAT         
    | KW_DOUBLE        
    | KW_DATE          
    | KW_DATETIME      
    | KW_TIMESTAMP     
    | KW_STRING        
    | KW_BINARY        
    | KW_DECIMAL       
    ;

listType
@init { msgs.push("list type"); }
@after { msgs.pop(); }
    : KW_ARRAY LESSTHAN type GREATERTHAN   
    ;

structType
@init { msgs.push("struct type"); }
@after { msgs.pop(); }
    : KW_STRUCT LESSTHAN columnNameColonTypeList GREATERTHAN 
    ;

mapType
@init { msgs.push("map type"); }
@after { msgs.pop(); }
    : KW_MAP LESSTHAN left=primitiveType COMMA right=type GREATERTHAN
    ;

unionType
@init { msgs.push("uniontype type"); }
@after { msgs.pop(); }
    : KW_UNIONTYPE LESSTHAN colTypeList GREATERTHAN 
    ;

queryOperator
@init { msgs.push("query operator"); }
@after { msgs.pop(); }
    : KW_UNION KW_ALL 
    ;

// select statement select ... from ... where ... group by ... order by ...
queryStatementExpression
    : 
    queryStatement (queryOperator queryStatement)*
    ;

queryStatement
    :
    fromClause
    ( b+=body )+ 
    | regular_body
    ;

regular_body
   :
   insertClause
   selectClause
   fromClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? 
   |
   selectStatement
   ;

selectStatement
   :
   selectClause
   fromClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? 
   ;


body
   :
   insertClause
   selectClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? 
   |
   selectClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   window_clause?
   limitClause? 
   ;

insertClause
@init { msgs.push("insert clause"); }
@after { msgs.pop(); }
   :
     KW_INSERT KW_OVERWRITE destination ifNotExists? 
   | KW_INSERT KW_INTO KW_TABLE tableOrPartition
   ;

destination
@init { msgs.push("destination specification"); }
@after { msgs.pop(); }
   :
     KW_LOCAL KW_DIRECTORY StringLiteral tableRowFormat? tableFileFormat? 
   | KW_DIRECTORY StringLiteral 
   | KW_TABLE tableOrPartition 
   ;

limitClause
@init { msgs.push("limit clause"); }
@after { msgs.pop(); }
   :
   KW_LIMIT num=Number 
   ;

    