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
package org.apache.tajo.ws.rs.resources;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestTablesResource extends QueryTestCaseBase {
  
  private URI restServiceURI;
  private URI tablesURI;
  private Client restClient;
  
  private static final String defaultDatabaseName = "TestTablesDB";

  public TestTablesResource() {
    super(defaultDatabaseName);
  }

  @Before
  public void setUp() throws Exception {
    int restPort = testBase.getTestingCluster().getConfiguration().getIntVar(ConfVars.REST_SERVICE_PORT);
    restServiceURI = new URI("http", null, "127.0.0.1", restPort, "/rest", null, null);
    tablesURI = new URI(restServiceURI + "/databases/" + defaultDatabaseName + "/tables");
    restClient = ClientBuilder.newBuilder()
        .register(new GsonFeature(RestTestUtils.registerTypeAdapterMap()))
        .register(LoggingFilter.class)
        .property(ClientProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
        .property(ClientProperties.METAINF_SERVICES_LOOKUP_DISABLE, true)
        .build();
  }
  
  @After
  public void tearDown() throws Exception {
    restClient.close();
  }
  
  private TableDesc createNewTableForTestCreateTable(String tableName) throws Exception {
    Schema tableSchema = new Schema(new Column[] {new Column("column1", TajoDataTypes.Type.TEXT)});
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    TableMeta tableMeta = new TableMeta("CSV", tableOptions);
    return new TableDesc(tableName, tableSchema, tableMeta, null);
  }
  
  @Test
  public void testCreateTable() throws Exception {
    String tableName = "TestCreateTable";
    TableDesc tableDesc = createNewTableForTestCreateTable(tableName);
    
    Response response = restClient.target(tablesURI)
        .request().post(Entity.entity(tableDesc, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
  }
  
  @Test
  public void testGetAllTable() throws Exception {
    String tableName = "TestGetAllTable";
    TableDesc tableDesc = createNewTableForTestCreateTable(tableName);
    
    Response response = restClient.target(tablesURI)
        .request().post(Entity.entity(tableDesc, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());

    Map<String, Collection<String>> tables = restClient.target(tablesURI)
        .request().get(new GenericType<Map<String, Collection<String>>>(Map.class));

    List<String> tableNames = (List<String>)tables.get("tables");
    assertNotNull(tableNames);
    assertTrue(!tableNames.isEmpty());

    boolean tableFound = false;
    for (String table: tableNames) {
      if (tableName.equals(CatalogUtil.extractSimpleName(table))) {
        tableFound = true;
        break;
      }
    }
    
    assertTrue(tableFound);
  }
  
  @Test
  public void testGetTable() throws Exception {
    String tableName = "TestGetTable";
    TableDesc tableDesc = createNewTableForTestCreateTable(tableName);
    
    Response response = restClient.target(tablesURI)
        .request().post(Entity.entity(tableDesc, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    
    TableDesc selectedTable = restClient.target(tablesURI)
        .path("/{tableName}").resolveTemplate("tableName", tableName)
        .request().get(new GenericType<TableDesc>(TableDesc.class));
    
    assertNotNull(selectedTable);
    assertEquals(tableName, CatalogUtil.extractSimpleName(selectedTable.getName()));
  }
  
  @Test
  public void testGetTableNotFound() throws Exception {
    Response response = restClient.target(tablesURI)
        .path("/{tableName}").resolveTemplate("tableName", "TestGetTableNotFound")
        .request().get();
    
    assertNotNull(response);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
  
  @Test
  public void testDropTable() throws Exception {
    String tableName = "TestDropTable";
    TableDesc tableDesc = createNewTableForTestCreateTable(tableName);
    
    Response response = restClient.target(tablesURI)
        .request().post(Entity.entity(tableDesc, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    
    TableDesc selectedTable = restClient.target(tablesURI)
        .path("/{tableName}").resolveTemplate("tableName", tableName)
        .request().get(new GenericType<TableDesc>(TableDesc.class));
    
    assertNotNull(selectedTable);
    assertEquals(tableName, CatalogUtil.extractSimpleName(selectedTable.getName()));
    
    response = restClient.target(tablesURI)
        .path("/{tableName}").resolveTemplate("tableName", tableName)
        .request().delete();
    
    assertNotNull(response);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
  
  @Test
  public void testDropTableNotFound() throws Exception {
    Response response = restClient.target(tablesURI)
        .path("/{tableName}").resolveTemplate("tableName", "TestDropTableNotFound")
        .request().delete();
    
    assertNotNull(response);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
