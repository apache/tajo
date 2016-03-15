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

import org.apache.commons.lang.StringUtils;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.apache.tajo.ws.rs.requests.NewSessionRequest;
import org.apache.tajo.ws.rs.requests.SubmitQueryRequest;
import org.apache.tajo.ws.rs.responses.GetSubmitQueryResponse;
import org.apache.tajo.ws.rs.responses.NewSessionResponse;
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

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestTablesResource extends QueryTestCaseBase {
  
  private URI restServiceURI;
  private URI tablesURI;
	private URI queriesURI;
	private URI sessionsURI;

	private Client restClient;
  
  private static final String defaultDatabaseName = "testtablesdb";
	private static final String tajoSessionIdHeaderName = "X-Tajo-Session";

	public TestTablesResource() {
    super(defaultDatabaseName);
  }

  @Before
  public void setUp() throws Exception {
    InetSocketAddress address = testBase.getTestingCluster().getConfiguration().getSocketAddrVar(ConfVars.REST_SERVICE_ADDRESS);
    restServiceURI = new URI("http", null, address.getHostName(), address.getPort(), "/rest", null, null);
    tablesURI = new URI(restServiceURI + "/databases/" + defaultDatabaseName + "/tables");
		queriesURI = new URI(restServiceURI + "/queries");
		sessionsURI = new URI(restServiceURI + "/sessions");
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

	private SubmitQueryRequest createNewQueryRequest(String query) throws Exception {
		SubmitQueryRequest request = new SubmitQueryRequest();
		request.setQuery(query);
		return request;
	}

	private String generateNewSessionAndGetId() throws Exception {
		NewSessionRequest request = new NewSessionRequest();
		request.setUserName("tajo-user");
		request.setDatabaseName(defaultDatabaseName);

		NewSessionResponse response = restClient.target(sessionsURI)
			.request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);

		assertNotNull(response);
		assertTrue(ResultCode.OK.equals(response.getResultCode()));
		assertTrue(response.getId() != null && !response.getId().isEmpty());

		return response.getId();
	}

	private void createNewTableForTestCreateTable(String tableName, String sessionId) throws Exception {
		String query = "create table " + tableName + " (column1 text)";
		SubmitQueryRequest queryRequest = createNewQueryRequest(query);

		GetSubmitQueryResponse response = restClient.target(queriesURI)
			.request().header(tajoSessionIdHeaderName, sessionId)
			.post(Entity.entity(queryRequest, MediaType.APPLICATION_JSON),
                    new GenericType<>(GetSubmitQueryResponse.class));

		assertNotNull(response);
		assertEquals(ResultCode.OK, response.getResultCode());
	}

  @Test
  public void testGetAllTable() throws Exception {
    String tableName = "testgetalltable";
		String sessionId = generateNewSessionAndGetId();

		createNewTableForTestCreateTable(tableName, sessionId);

    Map<String, Collection<String>> tables = restClient.target(tablesURI)
        .request().get(new GenericType<>(Map.class));

    List<String> tableNames = (List<String>)tables.get("tables");
    assertNotNull(tableNames);
    assertTrue(!tableNames.isEmpty());

    boolean tableFound = false;
    for (String table: tableNames) {
      if (StringUtils.equalsIgnoreCase(tableName, CatalogUtil.extractSimpleName(table))) {
        tableFound = true;
        break;
      }
    }
    
    assertTrue(tableFound);
  }
  
  @Test
  public void testGetTable() throws Exception {
    String tableName = "testgettable";
		String sessionId = generateNewSessionAndGetId();

		createNewTableForTestCreateTable(tableName, sessionId);
    
    TableDesc selectedTable = restClient.target(tablesURI)
        .path("/{tableName}").resolveTemplate("tableName", tableName)
        .request().get(new GenericType<>(TableDesc.class));
    
    assertNotNull(selectedTable);
    assertTrue(StringUtils.equalsIgnoreCase(tableName, CatalogUtil.extractSimpleName(selectedTable.getName())));
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
    String tableName = "testdroptable";
		String sessionId = generateNewSessionAndGetId();

		createNewTableForTestCreateTable(tableName, sessionId);
    
    TableDesc selectedTable = restClient.target(tablesURI)
        .path("/{tableName}").resolveTemplate("tableName", tableName)
        .request().get(new GenericType<>(TableDesc.class));
    
    assertNotNull(selectedTable);
    assertTrue(StringUtils.equalsIgnoreCase(tableName, CatalogUtil.extractSimpleName(selectedTable.getName())));
    
    Response response = restClient.target(tablesURI)
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
