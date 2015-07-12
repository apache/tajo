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

import com.google.gson.internal.StringMap;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos.ResultCode;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.apache.tajo.ws.rs.requests.NewSessionRequest;
import org.apache.tajo.ws.rs.requests.SubmitQueryRequest;
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
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestQueryResource extends QueryTestCaseBase {
  
  private URI restServiceURI;
  private URI sessionsURI;
  private URI queriesURI;
  private Client restClient;
  
  private static final String tajoSessionIdHeaderName = "X-Tajo-Session";
  
  public TestQueryResource() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }
  
  @Before
  public void setUp() throws Exception {
    int restPort = testBase.getTestingCluster().getConfiguration().getIntVar(ConfVars.REST_SERVICE_PORT);
    restServiceURI = new URI("http", null, "127.0.0.1", restPort, "/rest", null, null);
    sessionsURI = new URI(restServiceURI + "/sessions");
    queriesURI = new URI(restServiceURI + "/queries");
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
    request.setDatabaseName(TajoConstants.DEFAULT_DATABASE_NAME);
    
    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);
    
    assertNotNull(response);
    assertTrue(ResultCode.OK.equals(response.getResultCode()));
    assertTrue(response.getId() != null && !response.getId().isEmpty());
    
    return response.getId();
  }
  
  @Test
  public void testGetAllQueries() throws Exception {
    String sessionId = generateNewSessionAndGetId();
    SubmitQueryRequest queryRequest = createNewQueryRequest("select * from lineitem");

    Response response = restClient.target(queriesURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .post(Entity.entity(queryRequest, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    String locationHeader = response.getHeaderString("Location");
    assertTrue(locationHeader != null && !locationHeader.isEmpty());
    
    String queryId = locationHeader.lastIndexOf('/') >= 0?
        locationHeader.substring(locationHeader.lastIndexOf('/')+1):null;
        
    assertTrue(queryId != null && !queryId.isEmpty());
    
    Map<String, List<StringMap>> queriesMap = restClient.target(queriesURI)
        .request().get(new GenericType<Map<String, List<StringMap>>>(Map.class));
    
    assertNotNull(queriesMap);
    
    List<StringMap> queryInfoList = queriesMap.get("queries");
    assertNotNull(queryInfoList);
    
    boolean assertQueryIdFound = false;
    for (StringMap queryInfo: queryInfoList) {
      if (queryId.equals(queryInfo.get("queryIdStr"))) {
        assertQueryIdFound = true;
      }
    }
    
    assertTrue(assertQueryIdFound);
  }
  
  @Test
  public void testSubmitQuery() throws Exception {
    String sessionId = generateNewSessionAndGetId();
    SubmitQueryRequest queryRequest = createNewQueryRequest("select * from lineitem");

    Response response = restClient.target(queriesURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .post(Entity.entity(queryRequest, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    String locationHeader = response.getHeaderString("Location");
    assertTrue(locationHeader != null && !locationHeader.isEmpty());
    
    String queryId = locationHeader.lastIndexOf('/') >= 0?
        locationHeader.substring(locationHeader.lastIndexOf('/')+1):null;
        
    assertTrue(queryId != null && !queryId.isEmpty());
    
    QueryInfo queryInfo = restClient.target(queriesURI)
        .path("/{queryId}")
        .resolveTemplate("queryId", queryId)
        .queryParam("print", "BRIEF")
        .request().get(new GenericType<QueryInfo>(QueryInfo.class));
    
    assertNotNull(queryInfo);
    assertEquals(queryId, queryInfo.getQueryIdStr());
  }

  @Test
  public void testGetQueryInfoWithDefault() throws Exception {
    String sessionId = generateNewSessionAndGetId();
    SubmitQueryRequest queryRequest = createNewQueryRequest("select * from lineitem");

    Response response = restClient.target(queriesURI)
      .request().header(tajoSessionIdHeaderName, sessionId)
      .post(Entity.entity(queryRequest, MediaType.APPLICATION_JSON));

    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    String locationHeader = response.getHeaderString("Location");
    assertTrue(locationHeader != null && !locationHeader.isEmpty());

    String queryId = locationHeader.lastIndexOf('/') >= 0?
      locationHeader.substring(locationHeader.lastIndexOf('/')+1):null;

    assertTrue(queryId != null && !queryId.isEmpty());

    QueryInfo queryInfo = restClient.target(queriesURI)
      .path("/{queryId}")
      .resolveTemplate("queryId", queryId)
      .request().get(new GenericType<QueryInfo>(QueryInfo.class));

    assertNotNull(queryInfo);
    assertEquals(queryId, queryInfo.getQueryIdStr());
  }
}
