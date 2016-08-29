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

package org.apache.tajo.storage.http;

import com.google.common.collect.Lists;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Optional;

/**
 * Example read-only tablespace for HTTP protocol.
 *
 * An example table can be created by using the following SQL query.
 *
 * CREATE TABLE http_test (*) TABLESPACE http_example USING ex_http_json WITH ('path'='2015-01-01-15.json.gz',
 * 'compression.codec'='org.apache.hadoop.io.compress.GzipCodec’);
 */
public class ExampleHttpFileTablespace extends Tablespace {
  private static final Log LOG = LogFactory.getLog(ExampleHttpFileTablespace.class);

  static final String PATH = "path";

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Tablespace properties
  //////////////////////////////////////////////////////////////////////////////////////////////////
  private static final StorageProperty STORAGE_PROPERTY =
      new StorageProperty(
          BuiltinStorages.JSON, // default format is json
          false,                // is not movable
          false,                // is not writable
          true,                 // allow arbitrary path
          false                 // doesn't provide metadata
      );

  private static final FormatProperty FORMAT_PROPERTY =
      new FormatProperty(
          false,  // doesn't support insert
          false,  // doesn't support direct insert
          false   // doesn't support result staging
      );

  public ExampleHttpFileTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);

    LOG.info("ExampleHttpFileTablespace is initialized for " + uri);
  }

  @Override
  protected void storageInit() throws IOException {
    // Add initialization code for your tablespace
  }

  @Override
  public long getTableVolume(TableDesc table, Optional<EvalNode> notUsed) {
    HttpURLConnection connection = null;

    try {
      connection = (HttpURLConnection) new URL(table.getUri().toASCIIString()).openConnection();
      connection.setRequestMethod("HEAD");
      connection.connect();
      return connection.getHeaderFieldLong(Names.CONTENT_LENGTH, -1);

    } catch (IOException e) {
      throw new TajoInternalError(e);

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @Override
  public URI getRootUri() {
    return uri;
  }

  @Override
  public URI getTableUri(TableMeta meta, String databaseName, String tableName) {
    String tablespaceUriString = uri.toASCIIString();
    String tablePath = meta.getProperty(PATH);

    if (!tablespaceUriString.endsWith("/") && !tablePath.startsWith("/")) {
      tablePath = "/" + tablePath;
    }

    return URI.create(tablespaceUriString + tablePath);
  }

  @Override
  public List<Fragment> getSplits(String inputSourceId,
                                  TableDesc tableDesc,
                                  boolean requireSort,
                                  @Nullable EvalNode filterCondition)
      throws IOException, TajoException {

    // getSplits() should return multiple fragments for distributed processing of a large data.
    // This example tablespace returns only one fragment for the whole data for simplicity,
    // but this may significantly increase the query processing time.

    long tableVolume = getTableVolume(tableDesc, Optional.empty());
    return Lists.newArrayList(new ExampleHttpFileFragment(tableDesc.getUri(), inputSourceId, 0, tableVolume));
  }

  @Override
  public StorageProperty getProperty() {
    return STORAGE_PROPERTY;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    return FORMAT_PROPERTY;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext,
                                          TableDesc tableDesc,
                                          Schema inputSchema,
                                          SortSpec[] sortSpecs,
                                          TupleRange dataRange) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void createTable(TableDesc table, boolean ifNotExists) throws TajoException, IOException {
    HttpURLConnection connection = null;

    try {
      connection = (HttpURLConnection) new URL(table.getUri().toASCIIString()).openConnection();
      connection.setRequestMethod("HEAD");
      connection.connect();

      if (connection.getResponseCode() == 404) {
        throw new FileNotFoundException();
      }

    } catch (IOException e) {
      throw new TajoInternalError(e);

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void prepareTable(LogicalNode node) throws IOException, TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public Path commitTable(OverridableConf queryContext,
                          ExecutionBlockId finalEbId,
                          LogicalPlan plan,
                          Schema schema,
                          TableDesc tableDesc) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException, TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }
}
