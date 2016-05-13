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
package org.apache.tajo.jdbc;

import com.google.common.collect.Lists;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.client.CatalogAdminClient;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.common.type.TajoTypeUtil;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.util.VersionInfo;

import java.sql.*;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_SCHEMA_NAME;

/**
 * TajoDatabaseMetaData.
 */
public class TajoDatabaseMetaData implements DatabaseMetaData {
  private static final char SEARCH_STRING_ESCAPE = '\\';

  private static final String KEYWORDS = "add,binary,boolean,explain,index,rename";
  private static final String NUMERIC_FUNCTIONS =
      "abs,acos,asin,atan,atan2,ceiling,cos,degrees,exp,,floor,mod,pi,pow," +
      "radians,round,sign,sin,sqrt,tan";
  private static final String STRING_FUNCTIONS = "ascii,chr,concat,left,length,ltrim,repeat,rtrim,substring";
  private static final String PROCEDURE_TERM = "UDF";

  private final JdbcConnection conn;

  public TajoDatabaseMetaData(JdbcConnection conn) {
    this.conn = conn;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return true;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    return conn.getUri();
  }

  @Override
  public String getUserName() throws SQLException {
    return "tajo";
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return "Tajo";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return VersionInfo.getVersion();
  }

  @Override
  public String getDriverName() throws SQLException {
    return "tajo";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return TajoDriver.MAJOR_VERSION + "." + TajoDriver.MINOR_VERSION;
  }

  @Override
  public int getDriverMajorVersion() {
    return TajoDriver.MAJOR_VERSION;
  }

  @Override
  public int getDriverMinorVersion() {
    return TajoDriver.MINOR_VERSION;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return IdentifierUtil.IDENTIFIER_QUOTE_STRING;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return KEYWORDS;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return NUMERIC_FUNCTIONS;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return STRING_FUNCTIONS;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getTimeDateFunctions()
      throws SQLException {
    return "";
  }

  @Override
  public String getSearchStringEscape()
      throws SQLException {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "";
  }

  @Override
  public String getProcedureTerm()  throws SQLException {
    return PROCEDURE_TERM;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return IdentifierUtil.IDENTIFIER_DELIMITER;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return IdentifierUtil.MAX_IDENTIFIER_LENGTH;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return CatalogConstants.MAX_CONNECTION_LENGTH;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return IdentifierUtil.MAX_IDENTIFIER_LENGTH;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return IdentifierUtil.MAX_IDENTIFIER_LENGTH;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return IdentifierUtil.MAX_IDENTIFIER_LENGTH;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return IdentifierUtil.MAX_IDENTIFIER_LENGTH;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 0; // no limit
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return CatalogConstants.MAX_STATEMENT_LENGTH;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return IdentifierUtil.MAX_IDENTIFIER_LENGTH;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0; // no limit
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return CatalogConstants.MAX_USERNAME_LENGTH;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit()
      throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions()
      throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return new TajoMetaDataResultSet(Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM",
        "PROCEDURE_NAME", "NUM_INPUT_PARAMS", "NUM_OUTPUT_PARAMS", "NUM_RESULT_SETS", "REMARKS",
        "PROCEDURE_TYPE"),
        Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.INT4, Type.INT4, Type.INT4,
            Type.VARCHAR, Type.INT2),
            new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                       String columnNamePattern)
      throws SQLException {
    return new TajoMetaDataResultSet(Arrays.asList(
        "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE",
        "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE",
        "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
        Arrays.asList(
            Type.VARCHAR, Type.VARCHAR, Type.VARCHAR,Type.VARCHAR, Type.INT2,
            Type.INT2, Type.VARCHAR, Type.INT4, Type.INT4, Type.INT2,
            Type.INT2, Type.INT2, Type.VARCHAR, Type.VARCHAR, Type.INT2,
            Type.INT2, Type.INT2, Type.INT1, Type.VARCHAR, Type.VARCHAR),
            new ArrayList<MetaDataTuple>());
  }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   * these characters escaped using {@link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   * characters.
   */
  private String convertPattern(final String pattern) {
    if (pattern == null) {
      return ".*";
    } else {
      StringBuilder result = new StringBuilder(pattern.length());

      boolean escaped = false;
      for (int i = 0, len = pattern.length(); i < len; i++) {
        char c = pattern.charAt(i);
        if (escaped) {
          if (c != SEARCH_STRING_ESCAPE) {
            escaped = false;
          }
          result.append(c);
        } else {
          if (c == SEARCH_STRING_ESCAPE) {
            escaped = true;
            continue;
          } else if (c == '%') {
            result.append(".*");
          } else if (c == '_') {
            result.append('.');
          } else {
            result.append(c);
          }
        }
      }

      return result.toString();
    }
  }

  @Override
  public ResultSet getTables(@Nullable String catalog, @Nullable String schemaPattern,
                             @Nullable String tableNamePattern, @Nullable String [] types) throws SQLException {
    final List<MetaDataTuple> resultTables = new ArrayList<>();
    String regtableNamePattern = convertPattern(tableNamePattern == null ? null : tableNamePattern);

    List<String> targetCatalogs = Lists.newArrayList();
    if (catalog != null) {
      targetCatalogs.add(catalog);
    }

    try {
      CatalogAdminClient catalogAdmin = conn.getCatalogAdminClient();

      // if catalog is null, all databases are targets.
      if (targetCatalogs.isEmpty()) {
        targetCatalogs.addAll(catalogAdmin.getAllDatabaseNames());
      }

      for (String databaseName : targetCatalogs) {
        List<String> tableNames = catalogAdmin.getTableList(databaseName);
        for (String eachTableName: tableNames) {
          if (eachTableName.matches(regtableNamePattern)) {
            MetaDataTuple tuple = new MetaDataTuple(5);

            int index = 0;
            tuple.put(index++, new TextDatum(databaseName));         // TABLE_CAT
            tuple.put(index++, new TextDatum(DEFAULT_SCHEMA_NAME));   // TABLE_SCHEM
            tuple.put(index++, new TextDatum(eachTableName));         // TABLE_NAME
            tuple.put(index++, new TextDatum("TABLE"));               // TABLE_TYPE
            tuple.put(index++, NullDatum.get());                      // REMARKS

            resultTables.add(tuple);
          }
        }
      }
      Collections.sort(resultTables, new Comparator<MetaDataTuple> () {
        @Override
        public int compare(MetaDataTuple table1, MetaDataTuple table2) {
          int compVal = table1.getText(1).compareTo(table2.getText(1));
          if (compVal == 0) {
            compVal = table1.getText(2).compareTo(table2.getText(2));
          }
          return compVal;
        }
      });
    } catch (Throwable e) {
      throw new SQLException(e);
    }
    TajoMetaDataResultSet result = new TajoMetaDataResultSet(
        Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"),
        Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR),
        resultTables);

    return result;
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    String databaseName;

    databaseName = conn.getQueryClient().getCurrentDatabase();

    MetaDataTuple tuple = new MetaDataTuple(2);
    tuple.put(0, new TextDatum(DEFAULT_SCHEMA_NAME));
    tuple.put(1, new TextDatum(databaseName));

    return new TajoMetaDataResultSet(
        Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
        Arrays.asList(Type.VARCHAR, Type.VARCHAR),
        Collections.singletonList(tuple));
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    Collection<String> databaseNames;

    databaseNames = conn.getCatalogAdminClient().getAllDatabaseNames();

    List<MetaDataTuple> tuples = new ArrayList<>();
    for (String databaseName : databaseNames) {
      MetaDataTuple tuple = new MetaDataTuple(1);
      tuple.put(0, new TextDatum(databaseName));
      tuples.add(tuple);
    }

    return new TajoMetaDataResultSet(
        Collections.singletonList("TABLE_CAT"),
        Collections.singletonList(Type.VARCHAR),
        tuples);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    List<MetaDataTuple> columns = new ArrayList<>();
    MetaDataTuple tuple = new MetaDataTuple(2);
    tuple.put(0, new TextDatum("TABLE"));
    columns.add(tuple);

    ResultSet result = new TajoMetaDataResultSet(
        Collections.singletonList("TABLE_TYPE")
        , Collections.singletonList(Type.VARCHAR)
        , columns);

    return result;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    List<MetaDataTuple> columns = new ArrayList<>();

    return new TajoMetaDataResultSet(
        Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE"
            , "REMARKS", "BASE_TYPE")
        , Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.INT4, Type.VARCHAR, Type.INT4)
        , columns);
  }

  @Override
  public ResultSet getColumns(@Nullable String catalog, @Nullable String schemaPattern,
                              @Nullable String tableNamePattern, @Nullable String columnNamePattern)
      throws SQLException {

    List<String> targetCatalogs = Lists.newArrayList();
    if (catalog != null) {
      targetCatalogs.add(catalog);
    }

    List<MetaDataTuple> columns = new ArrayList<>();
    try {
      if (targetCatalogs.isEmpty()) {
        targetCatalogs.addAll(conn.getCatalogAdminClient().getAllDatabaseNames());
      }
      for (String databaseName : targetCatalogs) {
        String regtableNamePattern = convertPattern(tableNamePattern == null ? null : tableNamePattern);
        String regcolumnNamePattern = convertPattern(columnNamePattern == null ? null : columnNamePattern);

        List<String> tables = conn.getCatalogAdminClient().getTableList(databaseName);
        for (String table: tables) {
          if (table.matches(regtableNamePattern)) {
            TableDesc tableDesc = conn.getCatalogAdminClient().getTableDesc(
                IdentifierUtil.buildFQName(databaseName, table));
            int pos = 0;

            for (Column column: tableDesc.getLogicalSchema().getRootColumns()) {
              if (column.getSimpleName().matches(regcolumnNamePattern)) {
                MetaDataTuple tuple = new MetaDataTuple(22);

                int index = 0;
                tuple.put(index++, new TextDatum(databaseName));            // TABLE_CAT
                tuple.put(index++, new TextDatum(DEFAULT_SCHEMA_NAME));     // TABLE_SCHEM
                tuple.put(index++, new TextDatum(table));                   // TABLE_NAME
                tuple.put(index++, new TextDatum(column.getSimpleName()));  // COLUMN_NAME
                // TODO - DATA_TYPE
                tuple.put(index++, new TextDatum("" + ResultSetUtil.tajoTypeToSqlType(column.getDataType())));
                tuple.put(index++, new TextDatum(ResultSetUtil.toSqlType(column.getDataType())));  //TYPE_NAME
                tuple.put(index++, new TextDatum("0"));                     // COLUMN_SIZE
                tuple.put(index++, new TextDatum("0"));                     // BUFFER_LENGTH
                tuple.put(index++, new TextDatum("0"));                     // DECIMAL_DIGITS
                tuple.put(index++, new TextDatum("0"));                     // NUM_PREC_RADIX
                tuple.put(index++, new TextDatum("" + DatabaseMetaData.columnNullable));  // NULLABLE
                tuple.put(index++, NullDatum.get());                        // REMARKS
                tuple.put(index++, NullDatum.get());                        // COLUMN_DEF
                tuple.put(index++, NullDatum.get());                        // SQL_DATA_TYPE
                tuple.put(index++, NullDatum.get());                        // SQL_DATETIME_SUB
                tuple.put(index++, new TextDatum("0"));                     // CHAR_OCTET_LENGTH
                tuple.put(index++, new TextDatum("" + pos));                // ORDINAL_POSITION
                tuple.put(index++, new TextDatum("YES"));                   // IS_NULLABLE
                tuple.put(index++, NullDatum.get());                        // SCOPE_CATLOG
                tuple.put(index++, NullDatum.get());                        // SCOPE_SCHEMA
                tuple.put(index++, NullDatum.get());                        // SCOPE_TABLE
                tuple.put(index++, new TextDatum("0"));                     // SOURCE_DATA_TYPE
                columns.add(tuple);
              }
              pos++;
            }
          }
        }
      }

      return new TajoMetaDataResultSet(
          Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"
              , "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX"
              , "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB"
              , "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA"
              , "SCOPE_TABLE", "SOURCE_DATA_TYPE")
          , Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.INT4
              , Type.VARCHAR, Type.INT4, Type.INT4, Type.INT4, Type.INT4
              , Type.INT4, Type.VARCHAR, Type.VARCHAR, Type.INT4, Type.INT4
              , Type.INT4, Type.INT4, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR
              , Type.VARCHAR, Type.INT4)
          , columns);
    } catch (Throwable e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
      throws SQLException {
    return new TajoMetaDataResultSet(Arrays.asList("TABLE_CAT", "TABLE_SCHEM",
        "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
        "IS_GRANTABLE"),
        Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR,
            Type.VARCHAR, Type.VARCHAR),
            new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return new TajoMetaDataResultSet(Arrays.asList("TABLE_CAT", "TABLE_SCHEM",
        "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
        Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR),
            new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    return new TajoMetaDataResultSet(Arrays.asList("SCOPE", "COLUMN_NAME",
        "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
        Arrays.asList(Type.INT2, Type.VARCHAR, Type.INT2, Type.VARCHAR, Type.INT4, Type.INT4, Type.INT2, Type.INT2),
            new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    return new TajoMetaDataResultSet(Arrays.asList("SCOPE", "COLUMN_NAME",
        "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
        Arrays.asList(Type.INT2, Type.VARCHAR, Type.INT2, Type.VARCHAR, Type.INT4, Type.INT4, Type.INT2, Type.INT2),
            new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return new TajoMetaDataResultSet(
        Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME")
        , Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.INT4, Type.VARCHAR)
        , new ArrayList<MetaDataTuple>());
  }

  private final static Schema importedExportedSchema = SchemaBuilder.builder()
      .add("PKTABLE_CAT", Type.VARCHAR)   // 0
      .add("PKTABLE_SCHEM", Type.VARCHAR) // 1
      .add("PKTABLE_NAME", Type.VARCHAR)  // 2
      .add("PKCOLUMN_NAME", Type.VARCHAR) // 3
      .add("FKTABLE_CAT", Type.VARCHAR)   // 4
      .add("FKTABLE_SCHEM", Type.VARCHAR) // 5
      .add("FKTABLE_NAME", Type.VARCHAR)  // 6
      .add("FKCOLUMN_NAME", Type.VARCHAR) // 7
      .add("KEY_SEQ", Type.INT2)          // 8
      .add("UPDATE_RULE", Type.INT2)      // 9
      .add("DELETE_RULE", Type.INT2)      // 10
      .add("FK_NAME", Type.VARCHAR)       // 11
      .add("PK_NAME", Type.VARCHAR)       // 12
      .add("DEFERRABILITY", Type.INT2)    // 13
      .build();

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return new TajoMetaDataResultSet(importedExportedSchema, new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return new TajoMetaDataResultSet(importedExportedSchema, new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                     String foreignCatalog, String foreignSchema, String foreignTable)
      throws SQLException {
    return new TajoMetaDataResultSet(importedExportedSchema, new ArrayList<MetaDataTuple>());
  }


  @Override
  public ResultSet getTypeInfo() throws SQLException {
    List<MetaDataTuple> tuples = new ArrayList<>();
    for (Datum[] eachDatums: TajoTypeUtil.getTypeInfos()) {
      MetaDataTuple tuple = new MetaDataTuple(eachDatums.length);
      for (int i = 0; i < eachDatums.length; i++) {
        tuple.put(i, eachDatums[i]);
      }
      tuples.add(tuple);
    }

    return new TajoMetaDataResultSet(
        Arrays.asList(
            "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
            "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
            "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"),
        Arrays.asList(
            Type.VARCHAR, Type.INT2, Type.INT4, Type.VARCHAR, Type.VARCHAR,
            Type.VARCHAR, Type.INT2, Type.BOOLEAN, Type.INT2, Type.BOOLEAN,
            Type.BOOLEAN, Type.BOOLEAN, Type.VARCHAR, Type.INT2, Type.INT2,
            Type.INT4, Type.INT4, Type.INT4)
        , tuples);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return new TajoMetaDataResultSet(
        Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
            "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
            "CARDINALITY", "PAGES", "FILTER_CONDITION"),
        Arrays.asList(
            Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.BOOLEAN, Type.VARCHAR,
            Type.VARCHAR, Type.INT2, Type.INT2, Type.VARCHAR, Type.VARCHAR,
            Type.INT4, Type.INT4, Type.VARCHAR)
        , new ArrayList<MetaDataTuple>());
  }

  @Override
  public boolean deletesAreDetected(int type)
      throws SQLException {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("type hierarchies not supported");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("type hierarchies not supported");
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("user-defined types not supported");
  }

  @Override
  public int getResultSetHoldability()
      throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion()
      throws SQLException {
    return TajoDriver.MAJOR_VERSION;
  }

  @Override
  public int getDatabaseMinorVersion()
      throws SQLException {
    return TajoDriver.MINOR_VERSION;
  }

  @Override
  public int getJDBCMajorVersion()
      throws SQLException {
    return TajoDriver.JDBC_VERSION_MAJOR;
  }

  @Override
  public int getJDBCMinorVersion()
      throws SQLException {
    return TajoDriver.JDBC_VERSION_MINOR;
  }

  @Override
  public int getSQLStateType()
      throws SQLException {
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public RowIdLifetime getRowIdLifetime()
      throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    String databaseName;

    databaseName = conn.getQueryClient().getCurrentDatabase();

    MetaDataTuple tuple = new MetaDataTuple(2);
    tuple.put(0, new TextDatum(DEFAULT_SCHEMA_NAME));
    tuple.put(1, new TextDatum(databaseName));

    return new TajoMetaDataResultSet(
        Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
        Arrays.asList(Type.VARCHAR, Type.VARCHAR),
        Collections.singletonList(tuple));
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets()
      throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties()
      throws SQLException {
    return new TajoMetaDataResultSet(Arrays.asList("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"),
        Arrays.asList(Type.VARCHAR, Type.INT4, Type.VARCHAR, Type.VARCHAR),
            new ArrayList<MetaDataTuple>());
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("getFunctions not supported");
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                      String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("getFunctionColumns not supported");
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return true;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return (T) this;
    }
    throw new SQLFeatureNotSupportedException("No wrapper for " + iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                    String tableNamePattern, String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("getPseudoColumns not supported");
  }
}

