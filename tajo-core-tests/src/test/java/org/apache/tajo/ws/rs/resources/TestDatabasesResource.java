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

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.apache.tajo.ws.rs.requests.NewDatabaseRequest;
import org.apache.tajo.ws.rs.responses.DatabaseInfoResponse;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDatabasesResource extends QueryTestCaseBase {
  
  private URI restServiceURI;
  private URI databasesURI;
  private Client restClient;
  
  public TestDatabasesResource() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }
  
  @Before
  public void setUp() throws Exception {
    InetSocketAddress address = testBase.getTestingCluster().getConfiguration().getSocketAddrVar(ConfVars.REST_SERVICE_ADDRESS);
    restServiceURI = new URI("http", null, address.getHostName(), address.getPort(), "/rest", null, null);
    databasesURI = new URI(restServiceURI + "/databases");
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
  
  @Test
  public void testGetAllDatabases() throws Exception {
    Map<String, Collection<String>> databaseNames = restClient.target(databasesURI)
        .request().get(new GenericType<>(Map.class));
    
    assertNotNull(databaseNames);
    assertFalse(databaseNames.isEmpty());
    assertNotNull(databaseNames.get("databases"));
    
    Collection<String> databaseNamesCollection = databaseNames.get("databases");
    
    assertTrue(databaseNamesCollection.contains(TajoConstants.DEFAULT_DATABASE_NAME));
  }
  
  @Test
  public void testCreateDatabase() throws Exception {
    String databaseName = "TestDatabasesResource";
    NewDatabaseRequest request = new NewDatabaseRequest();
    
    request.setDatabaseName(databaseName);
    
    Response response = restClient.target(databasesURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    
    Map<String, Collection<String>> databaseNames = restClient.target(databasesURI)
        .request().get(new GenericType<>(Map.class));
    
    assertNotNull(databaseNames);
    assertFalse(databaseNames.isEmpty());
    assertNotNull(databaseNames.get("databases"));
    
    Collection<String> databaseNamesCollection = databaseNames.get("databases");
    
    assertTrue(databaseNamesCollection.contains(databaseName));
  }
  
  @Test
  public void testCreateDatabaseBadRequest() throws Exception {
    NewDatabaseRequest request = new NewDatabaseRequest();
    
    Response response = restClient.target(databasesURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetDatabase() throws Exception {
    DatabaseInfoResponse response =
        restClient.target(databasesURI).path("/{databaseName}")
        .resolveTemplate("databaseName", TajoConstants.DEFAULT_DATABASE_NAME)
        .request().get(new GenericType<>(DatabaseInfoResponse.class));
    
    assertNotNull(response);
    assertEquals(TajoConstants.DEFAULT_DATABASE_NAME, response.getName());
    assertTrue(response.getTablespace() != null && !response.getTablespace().isEmpty());
  }
  
  @Test
  public void testGetDatabaseNotFound() throws Exception {
    Response response =
        restClient.target(databasesURI).path("/{databaseName}")
        .resolveTemplate("databaseName", "testGetDatabaseNotFound")
        .request().get();
    
    assertNotNull(response);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
  
  @Test
  public void testDropDatabase() throws Exception {
    String databaseName = "TestDropDatabase";
    NewDatabaseRequest request = new NewDatabaseRequest();
    
    request.setDatabaseName(databaseName);
    
    Response response = restClient.target(databasesURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    
    Map<String, Collection<String>> databaseNames = restClient.target(databasesURI)
        .request().get(new GenericType<>(Map.class));
    
    assertNotNull(databaseNames);
    assertFalse(databaseNames.isEmpty());
    assertNotNull(databaseNames.get("databases"));
    
    Collection<String> databaseNamesCollection = databaseNames.get("databases");
    
    assertTrue(databaseNamesCollection.contains(databaseName));
    
    response = restClient.target(databasesURI)
        .path("/{databaseName}").resolveTemplate("databaseName", databaseName)
        .request().delete();
    
    assertNotNull(response);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
  
  @Test
  public void testDropDatabaseNotFound() throws Exception {
    Response response = restClient.target(databasesURI)
        .path("/{databaseName}").resolveTemplate("databaseName", "TestDropDatabaseNotFound")
        .request().delete();
    
    assertNotNull(response);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
