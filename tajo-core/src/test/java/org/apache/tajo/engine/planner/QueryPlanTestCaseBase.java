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

package org.apache.tajo.engine.planner;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.cli.tsql.ParsedResult;
import org.apache.tajo.cli.tsql.SimpleParser;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class QueryPlanTestCaseBase {
  private static final Log LOG = LogFactory.getLog(QueryPlanTestCaseBase.class);
  protected static final TpchPlanTestBase testBase;
  protected static final CatalogService catalog;
  protected static TajoConf conf;

  /** the base path of dataset directories */
  protected static final Path datasetBasePath;
  /** the base path of query directories */
  protected static final Path queryBasePath;
  /** the base path of result directories */
  protected static final Path resultBasePath;

  static {
    testBase = TpchPlanTestBase.getInstance();
    conf = testBase.getUtilility().getConf();
    catalog = testBase.getUtilility().getCatalog();

    URL datasetBaseURL = ClassLoader.getSystemResource("dataset");
    datasetBasePath = new Path(datasetBaseURL.toString());
    URL queryBaseURL = ClassLoader.getSystemResource("queries");
    queryBasePath = new Path(queryBaseURL.toString());
    URL resultBaseURL = ClassLoader.getSystemResource("plans");
    resultBasePath = new Path(resultBaseURL.toString());
  }

  /** It transiently contains created tables for the running test class. */
  private static String currentDatabase;
  private static Set<String> createdTableGlobalSet = new HashSet<String>();
  // queries and results directory corresponding to subclass class.
  protected Path currentQueryPath;
  protected Path currentResultPath;
  protected Path currentDatasetPath;

  // for getting a method name
  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = testBase.getUtilility().getConf();
  }

  @AfterClass
  public static void tearDownClass() throws ServiceException {
    for (String tableName : createdTableGlobalSet) {
      String denormalizedName = CatalogUtil.denormalizeIdentifier(tableName);
      if (catalog.existsTable(denormalizedName)) {
        catalog.dropTable(denormalizedName);
      }
    }
    createdTableGlobalSet.clear();

    // if the current database is "default", shouldn't drop it.
    if (!currentDatabase.equals(TajoConstants.DEFAULT_DATABASE_NAME)) {
      for (String tableName : catalog.getAllTableNames(currentDatabase)) {
        if (catalog.existsTable(tableName)) {
          catalog.dropTable(tableName);
        }
      }

      catalog.dropDatabase(currentDatabase);
    }
  }

  @Before
  public void printTestName() {
    /* protect a travis stalled build */
    System.out.println("Run: " + name.getMethodName());
  }

  public QueryPlanTestCaseBase() {
    this.currentDatabase = getClass().getSimpleName();
    init();
  }

  public QueryPlanTestCaseBase(String currentDatabase) {
    this.currentDatabase = currentDatabase;
    init();
  }

  private void init() {
    String className = getClass().getSimpleName();
    currentQueryPath = new Path(queryBasePath, className);
    currentResultPath = new Path(resultBasePath, className);
    currentDatasetPath = new Path(datasetBasePath, className);

    // if the current database is "default", we don't need create it because it is already prepated at startup time.
    if (!currentDatabase.equals(TajoConstants.DEFAULT_DATABASE_NAME)) {
//      String denormalizedDatabaseName = CatalogUtil.denormalizeIdentifier(currentDatabase);
      if (!catalog.existDatabase(currentDatabase)) {
        catalog.createDatabase(currentDatabase, TajoConstants.DEFAULT_TABLESPACE_NAME);
      }
    }

    conf.set(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
  }

  public String getCurrentDatabase() {
    return currentDatabase;
  }

  protected String executeString(String sql) throws Exception {
    return testBase.execute(sql);
  }

  /**
   * Execute a query contained in the file located in src/test/resources/results/<i>ClassName</i>/<i>MethodName</i>.
   * <i>ClassName</i> and <i>MethodName</i> will be replaced by actual executed class and methods.
   *
   * @return ResultSet of query execution.
   */
  public String executeQuery() throws Exception {
    return executeFile(getMethodName() + ".sql");
  }

  protected String getMethodName() {
    String methodName = name.getMethodName();
    // In the case of parameter execution name's pattern is methodName[0]
    if (methodName.endsWith("]")) {
      methodName = methodName.substring(0, methodName.length() - 3);
    }
    return methodName;
  }

  /**
   * Execute a query contained in the given named file. This methods tries to find the given file within the directory
   * src/test/resources/results/<i>ClassName</i>.
   *
   * @param queryFileName The file name to be used to execute a query.
   * @return ResultSet of query execution.
   */
  public String executeFile(String queryFileName) throws Exception {
    Path queryFilePath = getQueryFilePath(queryFileName);

    List<ParsedResult> parsedResults = SimpleParser.parseScript(FileUtil.readTextFile(new File(queryFilePath.toUri())));
    if (parsedResults.size() > 1) {
      assertNotNull("This script \"" + queryFileName + "\" includes two or more queries");
    }

    int idx = 0;
    for (; idx < parsedResults.size() - 1; idx++) {
      testBase.execute(parsedResults.get(idx).getHistoryStatement());
    }

    String result = testBase.execute(parsedResults.get(idx).getHistoryStatement());
    assertNotNull("Query succeeded test", result);
    return result;
  }

  private Path getQueryFilePath(String fileName) throws IOException {
    Path queryFilePath = StorageUtil.concatPath(currentQueryPath, fileName);
    FileSystem fs = currentQueryPath.getFileSystem(testBase.getUtilility().getConf());
    assertTrue(queryFilePath.toString() + " existence check", fs.exists(queryFilePath));
    return queryFilePath;
  }

  private Path getResultFile(String fileName) throws IOException {
    Path resultPath = StorageUtil.concatPath(currentResultPath, fileName);
    FileSystem fs = currentResultPath.getFileSystem(testBase.getUtilility().getConf());
    assertTrue(resultPath.toString() + " existence check", fs.exists(resultPath));
    return resultPath;
  }

  private Path getDataSetFile(String fileName) throws IOException {
    Path dataFilePath = StorageUtil.concatPath(currentDatasetPath, fileName);
    FileSystem fs = currentDatasetPath.getFileSystem(testBase.getUtilility().getConf());
    assertTrue(dataFilePath.toString() + " existence check", fs.exists(dataFilePath));
    return dataFilePath;
  }

  /**
   * Assert the equivalence between the expected result and an actual query result.
   * If it isn't it throws an AssertionError.
   *
   * @param result Query result to be compared.
   */
  public final void assertPlan(String result) throws IOException {
    assertPlan("Result Verification", result, getMethodName() + ".plan");
  }

  /**
   * Assert the equivalence between the expected result and an actual query result.
   * If it isn't it throws an AssertionError.
   *
   * @param result Query result to be compared.
   * @param resultFileName The file name containing the result to be compared
   */
  public final void assertPlan(String result, String resultFileName) throws IOException {
    assertPlan("Result Verification", result, resultFileName);
  }

  public final void assertPlan(String message, String result, String resultFileName) throws IOException {
    FileSystem fs = currentQueryPath.getFileSystem(testBase.getUtilility().getConf());
    Path resultFile = getResultFile(resultFileName);
    try {
      verifyPlan(message, result, resultFile);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void verifyPlan(String message, String res, Path resultFile) throws SQLException, IOException {
    String actualResult = res;
    String expectedResult = FileUtil.readTextFile(new File(resultFile.toUri()));
    assertEquals(message, expectedResult.trim(), actualResult.trim());
  }

  public void executeDDLString(String ddl) throws IOException, PlanningException {
    testBase.util.executeDDL(ddl);
  }

  public List<String> executeDDL(String ddlFileName, @Nullable String [] args) throws Exception {
    return executeDDL(ddlFileName, null, true, args);
  }

  /**
   *
   * Execute a data definition language (DDL) template. A general SQL DDL statement can be included in this file. But,
   * for user-specified table name or exact external table path, you must use some format string to indicate them.
   * The format string will be replaced by the corresponding arguments.
   *
   * The below is predefined format strings:
   * <ul>
   *   <li>${table.path} - It is replaced by the absolute file path that <code>dataFileName</code> points. </li>
   *   <li>${i} - It is replaced by the corresponding element of <code>args</code>. For example, ${0} and ${1} are
   *   replaced by the first and second elements of <code>args</code> respectively</li>. It uses zero-based index.
   * </ul>
   *
   * Example ddl
   * <pre>
   *   CREATE EXTERNAL TABLE ${0} (
   *     t_timestamp  TIMESTAMP,
   *     t_date    DATE
   *   ) USING CSV LOCATION ${table.path}
   * </pre>
   *
   * @param ddlFileName A file name, containing a data definition statement.
   * @param dataFileName A file name, containing data rows, which columns have to be separated by vertical bar '|'.
   *                     This file name is used for replacing some format string indicating an external table location.
   * @param args A list of arguments, each of which is used to replace corresponding variable which has a form of ${i}.
   * @return The table names created
   */
  public List<String> executeDDL(String ddlFileName, @Nullable String dataFileName, @Nullable String ... args)
      throws Exception {

    return executeDDL(ddlFileName, dataFileName, true, args);
  }

  private List<String> executeDDL(String ddlFileName, @Nullable String dataFileName, boolean isLocalTable,
                                  @Nullable String[] args) throws Exception {

    Path ddlFilePath = new Path(currentQueryPath, ddlFileName);
    FileSystem fs = ddlFilePath.getFileSystem(conf);
    assertTrue(ddlFilePath + " existence check", fs.exists(ddlFilePath));

    String template = FileUtil.readTextFile(new File(ddlFilePath.toUri()));
    String dataFilePath = null;
    if (dataFileName != null) {
      dataFilePath = getDataSetFile(dataFileName).toString();
    }
    String compiled = compileTemplate(template, dataFilePath, args);

    List<ParsedResult> parsedResults = SimpleParser.parseScript(compiled);
    List<String> createdTableNames = new ArrayList<String>();

    for (ParsedResult parsedResult : parsedResults) {
      // parse a statement
      Expr expr = testBase.util.getSQLAnalyzer().parse(parsedResult.getHistoryStatement());
      assertNotNull(ddlFilePath + " cannot be parsed", expr);

      if (expr.getType() == OpType.CreateTable) {
        CreateTable createTable = (CreateTable) expr;
        String tableName = createTable.getTableName();
        assertTrue("Table [" + tableName + "] creation is failed.",
            testBase.util.executeDDL(parsedResult.getHistoryStatement()));

        TableDesc createdTable = catalog.getTableDesc(tableName);
        String createdTableName = createdTable.getName();

        assertTrue("table '" + createdTableName + "' creation check", catalog.existsTable(createdTableName));
        if (isLocalTable) {
          createdTableGlobalSet.add(createdTableName);
          createdTableNames.add(tableName);
        }
      } else if (expr.getType() == OpType.DropTable) {
        DropTable dropTable = (DropTable) expr;
        String tableName = dropTable.getTableName();
        assertTrue("table '" + tableName + "' existence check",
            catalog.existsTable(CatalogUtil.buildFQName(currentDatabase, tableName)));
        assertTrue("table drop is failed.", testBase.util.executeDDL(parsedResult.getHistoryStatement()));
        assertFalse("table '" + tableName + "' dropped check",
            catalog.existsTable(CatalogUtil.buildFQName(currentDatabase, tableName)));
        if (isLocalTable) {
          createdTableGlobalSet.remove(tableName);
        }
      } else if (expr.getType() == OpType.AlterTable) {
        assertTrue(ddlFilePath + ": ALTER TABLE is not supported yet.", false);
      } else {
        assertTrue(ddlFilePath + " is not a Create or Drop Table statement", false);
      }
    }

    return createdTableNames;
  }

  /**
   * Replace format strings by a given parameters.
   *
   * @param template
   * @param dataFileName The data file name to replace <code>${table.path}</code>
   * @param args The list argument to replace each corresponding format string ${i}. ${i} uses zero-based index.
   * @return A string compiled
   */
  private String compileTemplate(String template, @Nullable String dataFileName, @Nullable String ... args) {
    String result;
    if (dataFileName != null) {
      result = template.replace("${table.path}", "\'" + dataFileName + "'");
    } else {
      result = template;
    }

    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        result = result.replace("${" + i + "}", args[i]);
      }
    }
    return result;
  }

  public void createTable(String ddl, String location, String[] dataRows) throws IOException, PlanningException {
    Path tablePath = new Path(location);
    FileSystem fs = tablePath.getFileSystem(testBase.util.getConf());
    FSDataOutputStream out = fs.create(tablePath);
    for (String row : dataRows) {
      out.writeUTF(row);
    }
    out.close();

    executeDDLString(ddl);
  }

  /**
   * Reads data file from Test Cluster's HDFS
   * @param path data parent path
   * @return data file's contents
   * @throws Exception
   */
  public String getTableFileContents(Path path) throws Exception {
    FileSystem fs = path.getFileSystem(conf);

    FileStatus[] files = fs.listStatus(path);

    if (files == null || files.length == 0) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    byte[] buf = new byte[1024];

    for (FileStatus file: files) {
      if (file.isDirectory()) {
        sb.append(getTableFileContents(file.getPath()));
        continue;
      }

      InputStream in = fs.open(file.getPath());
      try {
        while (true) {
          int readBytes = in.read(buf);
          if (readBytes <= 0) {
            break;
          }

          sb.append(new String(buf, 0, readBytes));
        }
      } finally {
        in.close();
      }
    }

    return sb.toString();
  }

//  /**
//   * Reads data file from Test Cluster's HDFS
//   * @param tableName
//   * @return data file's contents
//   * @throws Exception
//   */
//  public String getTableFileContents(String tableName) throws Exception {
//    TableDesc tableDesc = testingCluster.getMaster().getCatalog().getTableDesc(getCurrentDatabase(), tableName);
//    if (tableDesc == null) {
//      return null;
//    }
//
//    Path path = new Path(tableDesc.getPath());
//    return getTableFileContents(path);
//  }
//
//  public List<Path> listTableFiles(String tableName) throws Exception {
//    TableDesc tableDesc = testingCluster.getMaster().getCatalog().getTableDesc(getCurrentDatabase(), tableName);
//    if (tableDesc == null) {
//      return null;
//    }
//
//    Path path = new Path(tableDesc.getPath());
//    FileSystem fs = path.getFileSystem(conf);
//
//    return listFiles(fs, path);
//  }

  private List<Path> listFiles(FileSystem fs, Path path) throws Exception {
    List<Path> result = new ArrayList<Path>();
    FileStatus[] files = fs.listStatus(path);
    if (files == null || files.length == 0) {
      return result;
    }

    for (FileStatus eachFile: files) {
      if (eachFile.isDirectory()) {
        result.addAll(listFiles(fs, eachFile.getPath()));
      } else {
        result.add(eachFile.getPath());
      }
    }
    return result;
  }
}
