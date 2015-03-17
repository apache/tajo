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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.cli.tsql.ParsedResult;
import org.apache.tajo.cli.tsql.SimpleParser;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
}
