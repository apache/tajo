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

package org.apache.tajo.parser.sql;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static org.junit.Assert.*;

/**
 * This class verifies SQLAnalyzer.
 */
public class TestSQLAnalyzer {
  @Rule public TestName name = new TestName();

  public static Expr parseQuery(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    SQLParser parser = new SQLParser(tokens);
    parser.setErrorHandler(new BailErrorStrategy());
    parser.setBuildParseTree(true);

    SQLAnalyzer visitor = new SQLAnalyzer();
    SQLParser.SqlContext context = parser.sql();
    return visitor.visitSql(context);
  }

  public Collection<File> getResourceFiles(String subdir) throws URISyntaxException, IOException {
    URL uri = ClassLoader.getSystemResource("queries/TestSQLAnalyzer");
    Path positiveQueryDir = StorageUtil.concatPath(new Path(uri.toURI()), subdir);
    FileSystem fs = positiveQueryDir.getFileSystem(new TajoConf());

    if (!fs.exists(positiveQueryDir)) {
      throw new IOException("Cannot find " + positiveQueryDir);
    }

    // get only files
    Collection<FileStatus> files = filter(Lists.newArrayList(fs.listStatus(positiveQueryDir)),
        new Predicate<FileStatus>() {
          @Override
          public boolean apply(@Nullable FileStatus input) {
            return input.isFile();
          }
        }
    );

    // transform FileStatus into File
    return transform(files, new Function<FileStatus, File>() {
      @Override
      public File apply(@Nullable FileStatus fileStatus) {
        return new File(URI.create(fileStatus.getPath().toString()));
      }
    });
  }

  /**
   * Return a pair of file name and SQL query
   *
   * @return a pair of file name and SQL query
   * @throws IOException
   * @throws URISyntaxException
   */
  public Collection<Pair<String, String>> getFileContents(String subdir) throws IOException, URISyntaxException {
    return transform(getResourceFiles(subdir), new Function<File, Pair<String, String>>() {
      @Override
      public Pair<String, String> apply(@Nullable File file) {
        try {
          return new Pair<>(file.getName(), FileUtil.readTextFile(file));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * In order to add more unit tests, please add SQL files into resources/results/TestSQLAnalyzer/positive.
   * This test just checkes if SQL statements are parsed successfully.
   *
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test
  public void testPositiveTests() throws IOException, URISyntaxException {
    for (Pair<String, String> pair : getFileContents("positive")) {
      try {
        assertNotNull(parseQuery(pair.getSecond()));
        System.out.println(pair.getFirst() + " test passed...");
      } catch (Throwable t) {
        fail("Parsing '" + pair.getFirst() + "' failed, its cause: " + t.getMessage());
      }
    }
  }

  /**
   * In order to add more unit tests, add a SQL file including a SQL statement
   * into the directory resources/queries/TestSQLAnalyzer
   * and its generated algebraic expression formatted by json into resources/results/TestSQLAnalyzer.
   * Each result file name must be the same to its SQL file.
   *
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test
  public void testGeneratedAlgebras() throws IOException, URISyntaxException {
    for (Pair<String, String> pair : getFileContents(".")) {
      Expr expr = parseQuery(pair.getSecond());

      String expectedResult = null;
      String fileName = null;
      try {
        fileName = pair.getFirst().split("\\.")[0];
        expectedResult = FileUtil.readTextFileFromResource("results/TestSQLAnalyzer/" + fileName + ".result");
      } catch (FileNotFoundException ioe) {
        expectedResult = "";
      } catch (Throwable t) {
        fail(t.getMessage());
      }

      assertEquals(pair.getFirst() + " is different from " + fileName + ".result",
          expectedResult.trim(), expr.toJson().trim());
      System.out.println(pair.getFirst() + " test passed..");
    }
  }

  private static Expr parseExpr(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    SQLParser parser = new SQLParser(tokens);
    parser.setErrorHandler(new BailErrorStrategy());
    parser.setBuildParseTree(true);

    SQLAnalyzer visitor = new SQLAnalyzer();
    SQLParser.Value_expressionContext context = parser.value_expression();
    return visitor.visitValue_expression(context);
  }

  /**
   * In order to add more unit tests, add text files including SQL expressions
   * into the directory resources/queries/TestSQLAnalyzer/exprs.
   *
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test
  public void testExprs() throws IOException, URISyntaxException {
    for (Pair<String, String> pair : getFileContents("exprs")) {
      testExprs(pair.getFirst(), pair.getSecond());
      System.out.println(pair.getFirst() + " test passed..");
    }
  }

  private void testExprs(String file, String fileContents) {
    for (String line : fileContents.split("\n")) {
      try {
        assertNotNull(parseExpr(line));
      } catch (Throwable t) {
        fail(line + " in " + file + " failed..");
      }
    }
  }
}
