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

package org.apache.tajo;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.cli.tsql.ParsedResult;
import org.apache.tajo.cli.tsql.SimpleParser;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.GlobalEngine;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * (Note that this class is not thread safe. Do not execute maven test in any parallel mode.)
 * <br />
 * <code>QueryTestCaseBase</code> provides useful methods to easily execute queries and verify their results.
 *
 * This class basically uses four resource directories:
 * <ul>
 *   <li>src/test/resources/dataset - contains a set of data files. It contains sub directories, each of which
 *   corresponds each test class. All data files in each sub directory can be used in the corresponding test class.</li>
 *
 *   <li>src/test/resources/queries - This is the query directory. It contains sub directories, each of which
 *   corresponds each test class. All query files in each sub directory can be used in the corresponding test
 *   class.</li>
 *
 *   <li>src/test/resources/results - This is the result directory. It contains sub directories, each of which
 *   corresponds each test class. All result files in each sub directory can be used in the corresponding test class.
 *   </li>
 * </ul>
 *
 * For example, if you create a test class named <code>TestJoinQuery</code>, you should create a pair of query and
 * result set directories as follows:
 *
 * <pre>
 *   src-|
 *       |- resources
 *             |- dataset
 *             |     |- TestJoinQuery
 *             |              |- table1.tbl
 *             |              |- table2.tbl
 *             |
 *             |- queries
 *             |     |- TestJoinQuery
 *             |              |- TestInnerJoin.sql
 *             |              |- table1_ddl.sql
 *             |              |- table2_ddl.sql
 *             |
 *             |- results
 *                   |- TestJoinQuery
 *                            |- TestInnerJoin.result
 * </pre>
 *
 * <code>QueryTestCaseBase</code> basically provides the following methods:
 * <ul>
 *  <li><code>{@link #executeQuery()}</code> - executes a corresponding query and returns an ResultSet instance</li>
 *  <li><code>{@link #executeFile(String)}</code> - executes a given query file included in the corresponding query
 *  file in the current class's query directory</li>
 *  <li><code>assertResultSet()</code> - check if the query result is equivalent to the expected result included
 *  in the corresponding result file in the current class's result directory.</li>
 *  <li><code>cleanQuery()</code> - clean up all resources</li>
 *  <li><code>executeDDL()</code> - execute a DDL query like create or drop table.</li>
 * </ul>
 *
 * In order to make use of the above methods, query files and results file must be as follows:
 * <ul>
 *  <li>Each query file must be located on the subdirectory whose structure must be src/resources/queries/${ClassName},
 *  where ${ClassName} indicates an actual test class's simple name.</li>
 *  <li>Each result file must be located on the subdirectory whose structure must be src/resources/results/${ClassName},
 *  where ${ClassName} indicates an actual test class's simple name.</li>
 * </ul>
 *
 * Especially, {@link #executeQuery() and {@link #assertResultSet(java.sql.ResultSet)} methods automatically finds
 * a query file to be executed and a result to be compared, which are corresponding to the running class and method.
 * For them, query and result files additionally must be follows as:
 * <ul>
 *  <li>Each result file must have the file extension '.result'</li>
 *  <li>Each query file must have the file extension '.sql'.</li>
 * </ul>
 */
public class QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(QueryTestCaseBase.class);
  protected static final TpchTestBase testBase;
  protected static final TajoTestingCluster testingCluster;
  protected static TajoConf conf;
  protected static TajoClient client;
  protected static final CatalogService catalog;
  protected static final SQLAnalyzer sqlParser;
  protected static PreLogicalPlanVerifier verifier;
  protected static LogicalPlanner planner;
  protected static LogicalOptimizer optimizer;
  protected static LogicalPlanVerifier postVerifier;

  /** the base path of dataset directories */
  protected static final Path datasetBasePath;
  /** the base path of query directories */
  protected static final Path queryBasePath;
  /** the base path of result directories */
  protected static final Path resultBasePath;

  static {
    testBase = TpchTestBase.getInstance();
    testingCluster = testBase.getTestingCluster();
    conf = testBase.getTestingCluster().getConfiguration();
    catalog = testBase.getTestingCluster().getMaster().getCatalog();
    URL datasetBaseURL = ClassLoader.getSystemResource("dataset");
    datasetBasePath = new Path(datasetBaseURL.toString());
    URL queryBaseURL = ClassLoader.getSystemResource("queries");
    queryBasePath = new Path(queryBaseURL.toString());
    URL resultBaseURL = ClassLoader.getSystemResource("results");
    resultBasePath = new Path(resultBaseURL.toString());

    GlobalEngine engine = testingCluster.getMaster().getContext().getGlobalEngine();
    sqlParser = engine.getAnalyzer();
    verifier = engine.getPreLogicalPlanVerifier();
    planner = engine.getLogicalPlanner();
    optimizer = engine.getLogicalOptimizer();
    postVerifier = engine.getLogicalPlanVerifier();
  }

  /** It transiently contains created tables for the running test class. */
  private static String currentDatabase;
  private static Set<String> createdTableGlobalSet = new HashSet<String>();
  // queries and results directory corresponding to subclass class.
  protected Path currentQueryPath;
  protected Path namedQueryPath;
  protected Path currentResultPath;
  protected Path currentDatasetPath;
  protected Path namedDatasetPath;

  protected FileSystem currentResultFS;

  protected final String testParameter;

  // for getting a method name
  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = testBase.getTestingCluster().getConfiguration();
    client = testBase.getTestingCluster().newTajoClient();
  }

  @AfterClass
  public static void tearDownClass() throws ServiceException {
    for (String tableName : createdTableGlobalSet) {
      client.updateQuery("DROP TABLE IF EXISTS " + CatalogUtil.denormalizeIdentifier(tableName));
    }
    createdTableGlobalSet.clear();

    // if the current database is "default", shouldn't drop it.
    if (!currentDatabase.equals(TajoConstants.DEFAULT_DATABASE_NAME)) {
      for (String tableName : catalog.getAllTableNames(currentDatabase)) {
        client.updateQuery("DROP TABLE IF EXISTS " + tableName);
      }

      client.selectDatabase(TajoConstants.DEFAULT_DATABASE_NAME);
      client.dropDatabase(currentDatabase);
    }
    client.close();
  }

  @Before
  public void printTestName() {
    /* protect a travis stalled build */
    System.out.println("Run: " + name.getMethodName() +
        " Used memory: " + ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
        / (1024 * 1024)) + "MBytes");
  }

  public QueryTestCaseBase() {
    // hive 0.12 does not support quoted identifier.
    // So, we use lower case database names when Tajo uses HiveCatalogStore.
    if (testingCluster.isHiveCatalogStoreRunning()) {
      this.currentDatabase = getClass().getSimpleName().toLowerCase();
    } else {
      this.currentDatabase = getClass().getSimpleName();
    }
    testParameter = null;
    init();
  }

  public QueryTestCaseBase(String currentDatabase) {
    this(currentDatabase, null);
  }

  public QueryTestCaseBase(String currentDatabase, String testParameter) {
    this.currentDatabase = currentDatabase;
    this.testParameter = testParameter;
    init();
  }

  private void init() {
    String className = getClass().getSimpleName();
    currentQueryPath = new Path(queryBasePath, className);
    currentResultPath = new Path(resultBasePath, className);
    currentDatasetPath = new Path(datasetBasePath, className);
    NamedTest namedTest = getClass().getAnnotation(NamedTest.class);
    if (namedTest != null) {
      namedQueryPath = new Path(queryBasePath, namedTest.value());
      namedDatasetPath = new Path(datasetBasePath, namedTest.value());
    }

    try {
      // if the current database is "default", we don't need create it because it is already prepated at startup time.
      if (!currentDatabase.equals(TajoConstants.DEFAULT_DATABASE_NAME)) {
        client.updateQuery("CREATE DATABASE IF NOT EXISTS " + CatalogUtil.denormalizeIdentifier(currentDatabase));
      }
      client.selectDatabase(currentDatabase);
      currentResultFS = currentResultPath.getFileSystem(testBase.getTestingCluster().getConfiguration());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
  }

  protected TajoClient getClient() {
    return client;
  }

  public String getCurrentDatabase() {
    return currentDatabase;
  }

  private static VerificationState verify(String query) throws PlanningException {

    VerificationState state = new VerificationState();
    QueryContext context = LocalTajoTestingUtility.createDummyContext(conf);

    Expr expr = sqlParser.parse(query);
    verifier.verify(context, state, expr);
    if (state.getErrorMessages().size() > 0) {
      return state;
    }
    LogicalPlan plan = planner.createPlan(context, expr);
    optimizer.optimize(plan);
    postVerifier.verify(context, state, plan);

    return state;
  }

  public void assertValidSQL(String query) throws PlanningException, IOException {
    VerificationState state = verify(query);
    if (state.getErrorMessages().size() > 0) {
      fail(state.getErrorMessages().get(0));
    }
  }

  public void assertValidSQLFromFile(String fileName) throws PlanningException, IOException {
    Path queryFilePath = getQueryFilePath(fileName);
    String query = FileUtil.readTextFile(new File(queryFilePath.toUri()));
    assertValidSQL(query);
  }

  public void assertInvalidSQL(String query) throws PlanningException, IOException {
    VerificationState state = verify(query);
    if (state.getErrorMessages().size() == 0) {
      fail(PreLogicalPlanVerifier.class.getSimpleName() + " cannot catch any verification error: " + query);
    }
  }

  public void assertInvalidSQLFromFile(String fileName) throws PlanningException, IOException {
    Path queryFilePath = getQueryFilePath(fileName);
    String query = FileUtil.readTextFile(new File(queryFilePath.toUri()));
    assertInvalidSQL(query);
  }

  public void assertPlanError(String fileName) throws PlanningException, IOException {
    Path queryFilePath = getQueryFilePath(fileName);
    String query = FileUtil.readTextFile(new File(queryFilePath.toUri()));
    try {
      verify(query);
    } catch (PlanningException e) {
      return;
    }
    fail("Cannot catch any planning error from: " + query);
  }

  protected ResultSet executeString(String sql) throws Exception {
    return client.executeQueryAndGetResult(sql);
  }

  /**
   * It executes the query file and compare the result against the the result file.
   *
   * @throws Exception
   */
  public void assertQuery() throws Exception {
    ResultSet res = null;
    try {
      res = executeQuery();
      assertResultSet(res);
    } finally {
      if (res != null) {
        res.close();
      }
    }
  }

  /**
   * It executes a given query statement and verifies the result against the the result file.
   *
   * @param query A query statement
   * @throws Exception
   */
  public void assertQueryStr(String query) throws Exception {
    ResultSet res = null;
    try {
      res = executeString(query);
      assertResultSet(res);
    } finally {
      if (res != null) {
        res.close();
      }
    }
  }

  /**
   * Execute a query contained in the file located in src/test/resources/results/<i>ClassName</i>/<i>MethodName</i>.
   * <i>ClassName</i> and <i>MethodName</i> will be replaced by actual executed class and methods.
   *
   * @return ResultSet of query execution.
   */
  public ResultSet executeQuery() throws Exception {
    return executeFile(getMethodName() + ".sql");
  }

  private volatile Description current;

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      QueryTestCaseBase.this.current = description;
    }
  };

  @Target({ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  protected @interface SimpleTest {
    String[] prepare() default {};
    QuerySpec[] queries() default {};
    String[] cleanup() default {};
  }

  @Target({ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  protected @interface QuerySpec {
    String value();
    boolean override() default false;
    Option option() default @Option;
  }

  @Target({ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  protected @interface Option {
    boolean withExplain() default false;
    boolean withExplainGlobal() default false;
    boolean parameterized() default false;
    boolean sort() default false;
  }

  private static class DummyQuerySpec implements QuerySpec {
    private final String value;
    private final Option option;
    public DummyQuerySpec(String value, Option option) {
      this.value = value;
      this.option = option;
    }
    public Class<? extends Annotation> annotationType() { return QuerySpec.class; }
    public String value() { return value; }
    public boolean override() { return option != null; }
    public Option option() { return option; }
  }

  private static class DummyOption implements Option {
    private final boolean explain;
    private final boolean withExplainGlobal;
    private final boolean parameterized;
    private final boolean sort;
    public DummyOption(boolean explain, boolean withExplainGlobal, boolean parameterized, boolean sort) {
      this.explain = explain;
      this.withExplainGlobal = withExplainGlobal;
      this.parameterized = parameterized;
      this.sort = sort;
    }
    public Class<? extends Annotation> annotationType() { return Option.class; }
    public boolean withExplain() { return explain;}
    public boolean withExplainGlobal() { return withExplainGlobal;}
    public boolean parameterized() { return parameterized;}
    public boolean sort() { return sort;}
  }

  protected void runSimpleTests() throws Exception {
    String methodName = getMethodName();
    Method method = current.getTestClass().getMethod(methodName);
    SimpleTest annotation = method.getAnnotation(SimpleTest.class);
    if (annotation == null) {
      throw new IllegalStateException("Cannot find test annotation");
    }

    List<String> prepares = new ArrayList<String>(Arrays.asList(annotation.prepare()));
    QuerySpec[] queries = annotation.queries();
    Option defaultOption = method.getAnnotation(Option.class);
    if (defaultOption == null) {
      defaultOption = new DummyOption(false, false, false, false);
    }

    boolean fromFile = false;
    if (queries.length == 0) {
      Path queryFilePath = getQueryFilePath(getMethodName() + ".sql");
      List<ParsedResult> parsedResults = SimpleParser.parseScript(FileUtil.readTextFile(new File(queryFilePath.toUri())));
      int i = 0;
      for (; i < parsedResults.size() - 1; i++) {
        prepares.add(parsedResults.get(i).getStatement());
      }
      queries = new QuerySpec[] {new DummyQuerySpec(parsedResults.get(i).getHistoryStatement(), null)};
      fromFile = true;  // do not append query index to result file
    }
    
    try {
      for (String prepare : prepares) {
        client.executeQueryAndGetResult(prepare).close();
      }
      for (int i = 0; i < queries.length; i++) {
        QuerySpec spec = queries[i];
        Option option = spec.override() ? spec.option() : defaultOption;
        String prefix = "";
        testingCluster.getConfiguration().set(TajoConf.ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "true");
        if (option.withExplain()) {// Enable this option to fix the shape of the generated plans.
          prefix += resultSetToString(executeString("explain " + spec.value()));
        }
        if (option.withExplainGlobal()) {
          // Enable this option to fix the shape of the generated plans.
          prefix += resultSetToString(executeString("explain global " + spec.value()));
        }

        // plan test
        if (prefix.length() > 0) {
          String planResultName = methodName + (fromFile ? "" : "." + (i + 1)) +
              ((option.parameterized() && testParameter != null) ? "." + testParameter : "") + ".plan";
          Path resultPath = StorageUtil.concatPath(currentResultPath, planResultName);
          if (currentResultFS.exists(resultPath)) {
            assertEquals("Plan Verification for: " + (i + 1) + " th test",
                FileUtil.readTextFromStream(currentResultFS.open(resultPath)), prefix);
          } else if (prefix.length() > 0) {
            // If there is no result file expected, create gold files for new tests.
            FileUtil.writeTextToStream(prefix, currentResultFS.create(resultPath));
            LOG.info("New test output for " + current.getDisplayName() + " is written to " + resultPath);
            // should be copied to src directory
          }
        }

        testingCluster.getConfiguration().set(TajoConf.ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "false");
        ResultSet result = client.executeQueryAndGetResult(spec.value());

        // result test
        String fileName = methodName + (fromFile ? "" : "." + (i + 1)) + ".result";
        Path resultPath = StorageUtil.concatPath(currentResultPath, fileName);
        if (currentResultFS.exists(resultPath)) {
          assertEquals("Result Verification for: " + (i + 1) + " th test",
              FileUtil.readTextFromStream(currentResultFS.open(resultPath)), resultSetToString(result, option.sort()));
        } else if (!isNull(result)) {
          // If there is no result file expected, create gold files for new tests.
          FileUtil.writeTextToStream(resultSetToString(result, option.sort()), currentResultFS.create(resultPath));
          LOG.info("New test output for " + current.getDisplayName() + " is written to " + resultPath);
          // should be copied to src directory
        }
        result.close();
      }
    } finally {
      for (String cleanup : annotation.cleanup()) {
        try {
          client.executeQueryAndGetResult(cleanup).close();
        } catch (ServiceException e) {
          // ignore
        }
      }
    }
  }

  private boolean isNull(ResultSet result) throws SQLException {
    return result.getMetaData().getColumnCount() == 0;
  }

  protected String getMethodName() {
    String methodName = name.getMethodName();
    // In the case of parameter execution name's pattern is methodName[0]
    if (methodName.endsWith("]")) {
      methodName = methodName.substring(0, methodName.length() - 3);
    }
    return methodName;
  }

  public ResultSet executeJsonQuery() throws Exception {
    return executeJsonFile(getMethodName() + ".json");
  }

  /**
   * Execute a query contained in the given named file. This methods tries to find the given file within the directory
   * src/test/resources/results/<i>ClassName</i>.
   *
   * @param queryFileName The file name to be used to execute a query.
   * @return ResultSet of query execution.
   */
  public ResultSet executeFile(String queryFileName) throws Exception {
    Path queryFilePath = getQueryFilePath(queryFileName);

    List<ParsedResult> parsedResults = SimpleParser.parseScript(FileUtil.readTextFile(new File(queryFilePath.toUri())));
    if (parsedResults.size() > 1) {
      assertNotNull("This script \"" + queryFileName + "\" includes two or more queries");
    }

    int idx = 0;
    for (; idx < parsedResults.size() - 1; idx++) {
      client.executeQueryAndGetResult(parsedResults.get(idx).getHistoryStatement()).close();
    }

    ResultSet result = client.executeQueryAndGetResult(parsedResults.get(idx).getHistoryStatement());
    assertNotNull("Query succeeded test", result);
    return result;
  }

  public ResultSet executeJsonFile(String jsonFileName) throws Exception {
    Path queryFilePath = getQueryFilePath(jsonFileName);

    ResultSet result = client.executeJsonQueryAndGetResult(FileUtil.readTextFile(new File(queryFilePath.toUri())));
    assertNotNull("Query succeeded test", result);
    return result;
  }

  /**
   * Assert the equivalence between the expected result and an actual query result.
   * If it isn't it throws an AssertionError.
   *
   * @param result Query result to be compared.
   */
  public final void assertResultSet(ResultSet result) throws IOException {
    assertResultSet("Result Verification", result, getMethodName() + ".result");
  }

  /**
   * Assert the equivalence between the expected result and an actual query result.
   * If it isn't it throws an AssertionError.
   *
   * @param result Query result to be compared.
   * @param resultFileName The file name containing the result to be compared
   */
  public final void assertResultSet(ResultSet result, String resultFileName) throws IOException {
    assertResultSet("Result Verification", result, resultFileName);
  }

  /**
   * Assert the equivalence between the expected result and an actual query result.
   * If it isn't it throws an AssertionError with the given message.
   *
   * @param message message The message to printed if the assertion is failed.
   * @param result Query result to be compared.
   */
  public final void assertResultSet(String message, ResultSet result, String resultFileName) throws IOException {
    FileSystem fs = currentQueryPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    Path resultFile = getResultFile(resultFileName);
    try {
      verifyResultText(message, result, resultFile);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  public final void assertStrings(String actual) throws IOException {
    assertStrings(actual, getMethodName() + ".result");
  }

  public final void assertStrings(String actual, String resultFileName) throws IOException {
    assertStrings("Result Verification", actual, resultFileName);
  }

  public final void assertStrings(String message, String actual, String resultFileName) throws IOException {
    Path resultFile = getResultFile(resultFileName);
    String expectedResult = FileUtil.readTextFile(new File(resultFile.toUri()));
    assertEquals(message, expectedResult, actual);
  }

  /**
   * Release all resources
   *
   * @param resultSet ResultSet
   */
  public final void cleanupQuery(ResultSet resultSet) throws IOException {
    if (resultSet == null) {
      return;
    }
    try {
      resultSet.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  /**
   * Assert that the database exists.
   * @param databaseName The database name to be checked. This name is case sensitive.
   */
  public void assertDatabaseExists(String databaseName) throws ServiceException {
    assertTrue(client.existDatabase(databaseName));
  }

  /**
   * Assert that the database does not exists.
   * @param databaseName The database name to be checked. This name is case sensitive.
   */
  public void assertDatabaseNotExists(String databaseName) throws ServiceException {
    assertTrue(!client.existDatabase(databaseName));
  }

  /**
   * Assert that the table exists.
   *
   * @param tableName The table name to be checked. This name is case sensitive.
   * @throws ServiceException
   */
  public void assertTableExists(String tableName) throws ServiceException {
    assertTrue(client.existTable(tableName));
  }

  /**
   * Assert that the table does not exist.
   *
   * @param tableName The table name to be checked. This name is case sensitive.
   */
  public void assertTableNotExists(String tableName) throws ServiceException {
    assertTrue(!client.existTable(tableName));
  }

  public void assertColumnExists(String tableName,String columnName) throws ServiceException {
    TableDesc tableDesc = fetchTableMetaData(tableName);
    assertTrue(tableDesc.getSchema().containsByName(columnName));
  }

  private TableDesc fetchTableMetaData(String tableName) throws ServiceException {
    return client.getTableDesc(tableName);
  }

  public void assertTablePropertyEquals(String tableName, String key, String expectedValue) throws ServiceException {
    TableDesc tableDesc = fetchTableMetaData(tableName);
    assertEquals(expectedValue, tableDesc.getMeta().getOption(key));
  }

  public String resultSetToString(ResultSet resultSet) throws SQLException {
    return resultSetToString(resultSet, false);
  }

  /**
   * It transforms a ResultSet instance to rows represented as strings.
   *
   * @param resultSet ResultSet that contains a query result
   * @return String
   * @throws SQLException
   */
  public String resultSetToString(ResultSet resultSet, boolean sort) throws SQLException {
    StringBuilder sb = new StringBuilder();
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int numOfColumns = rsmd.getColumnCount();

    for (int i = 1; i <= numOfColumns; i++) {
      if (i > 1) sb.append(",");
      String columnName = rsmd.getColumnName(i);
      sb.append(columnName);
    }
    sb.append("\n-------------------------------\n");

    List<String> results = new ArrayList<String>();
    while (resultSet.next()) {
      StringBuilder line = new StringBuilder();
      for (int i = 1; i <= numOfColumns; i++) {
        if (i > 1) line.append(",");
        String columnValue = resultSet.getString(i);
        if (resultSet.wasNull()) {
          columnValue = "null";
        }
        line.append(columnValue);
      }
      results.add(line.toString());
    }
    if (sort) {
      Collections.sort(results);
    }
    for (String line : results) {
      sb.append(line).append('\n');
    }
    return sb.toString();
  }

  private void verifyResultText(String message, ResultSet res, Path resultFile) throws SQLException, IOException {
    String actualResult = resultSetToString(res);
    String expectedResult = FileUtil.readTextFile(new File(resultFile.toUri()));
    assertEquals(message, expectedResult.trim(), actualResult.trim());
  }

  private Path getQueryFilePath(String fileName) throws IOException {
    Path queryFilePath = StorageUtil.concatPath(currentQueryPath, fileName);
    FileSystem fs = currentQueryPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    if (!fs.exists(queryFilePath)) {
      if (namedQueryPath != null) {
        queryFilePath = StorageUtil.concatPath(namedQueryPath, fileName);
        fs = namedQueryPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
        if (!fs.exists(queryFilePath)) {
          throw new IOException("Cannot find " + fileName + " at " + currentQueryPath + " and " + namedQueryPath);
        }
      } else {
        throw new IOException("Cannot find " + fileName + " at " + currentQueryPath);
      }
    }
    return queryFilePath;
  }

  private Path getResultFile(String fileName) throws IOException {
    Path resultPath = StorageUtil.concatPath(currentResultPath, fileName);
    FileSystem fs = currentResultPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    assertTrue(resultPath.toString() + " existence check", fs.exists(resultPath));
    return resultPath;
  }

  private Path getDataSetFile(String fileName) throws IOException {
    Path dataFilePath = StorageUtil.concatPath(currentDatasetPath, fileName);
    FileSystem fs = currentDatasetPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    if (!fs.exists(dataFilePath)) {
      if (namedDatasetPath != null) {
        dataFilePath = StorageUtil.concatPath(namedDatasetPath, fileName);
        fs = namedDatasetPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
        if (!fs.exists(dataFilePath)) {
          throw new IOException("Cannot find " + fileName + " at " + currentQueryPath + " and " + namedQueryPath);
        }
      } else {
        throw new IOException("Cannot find " + fileName + " at " + currentQueryPath + " and " + namedQueryPath);
      }
    }
    return dataFilePath;
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

    Path ddlFilePath = getQueryFilePath(ddlFileName);

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
      Expr expr = sqlParser.parse(parsedResult.getHistoryStatement());
      assertNotNull(ddlFilePath + " cannot be parsed", expr);

      if (expr.getType() == OpType.CreateTable) {
        CreateTable createTable = (CreateTable) expr;
        String tableName = createTable.getTableName();
        assertTrue("Table [" + tableName + "] creation is failed.", client.updateQuery(parsedResult.getHistoryStatement()));

        TableDesc createdTable = client.getTableDesc(tableName);
        String createdTableName = createdTable.getName();

        assertTrue("table '" + createdTableName + "' creation check", client.existTable(createdTableName));
        if (isLocalTable) {
          createdTableGlobalSet.add(createdTableName);
          createdTableNames.add(tableName);
        }
      } else if (expr.getType() == OpType.DropTable) {
        DropTable dropTable = (DropTable) expr;
        String tableName = dropTable.getTableName();
        assertTrue("table '" + tableName + "' existence check",
            client.existTable(CatalogUtil.buildFQName(currentDatabase, tableName)));
        assertTrue("table drop is failed.", client.updateQuery(parsedResult.getHistoryStatement()));
        assertFalse("table '" + tableName + "' dropped check",
            client.existTable(CatalogUtil.buildFQName(currentDatabase, tableName)));
        if (isLocalTable) {
          createdTableGlobalSet.remove(tableName);
        }
      } else if (expr.getType() == OpType.AlterTable) {
        AlterTable alterTable = (AlterTable) expr;
        String tableName = alterTable.getTableName();
        assertTrue("table '" + tableName + "' existence check", client.existTable(tableName));
        client.updateQuery(compiled);
        if (isLocalTable) {
          createdTableGlobalSet.remove(tableName);
        }
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

  /**
   * Reads data file from Test Cluster's HDFS
   * @param tableName
   * @return data file's contents
   * @throws Exception
   */
  public String getTableFileContents(String tableName) throws Exception {
    TableDesc tableDesc = testingCluster.getMaster().getCatalog().getTableDesc(getCurrentDatabase(), tableName);
    if (tableDesc == null) {
      return null;
    }

    Path path = new Path(tableDesc.getUri());
    return getTableFileContents(path);
  }

  public List<Path> listTableFiles(String tableName) throws Exception {
    TableDesc tableDesc = testingCluster.getMaster().getCatalog().getTableDesc(getCurrentDatabase(), tableName);
    if (tableDesc == null) {
      return null;
    }

    Path path = new Path(tableDesc.getUri());
    FileSystem fs = path.getFileSystem(conf);

    return listFiles(fs, path);
  }

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
