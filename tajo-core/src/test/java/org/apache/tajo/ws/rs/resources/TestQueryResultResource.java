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

import java.net.URI;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos.ResultCode;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.apache.tajo.ws.rs.requests.NewSessionRequest;
import org.apache.tajo.ws.rs.requests.SubmitQueryRequest;
import org.apache.tajo.ws.rs.responses.NewSessionResponse;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.*;

public class TestQueryResultResource extends QueryTestCaseBase {

  private URI restServiceURI;
  private URI sessionsURI;
  private URI queriesURI;
  private Client restClient;
  
  private static final String tajoSessionIdHeaderName = "X-Tajo-Session";
  
  public TestQueryResultResource() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }
  
  @Before
  public void setUp() throws Exception {
    int restPort = testBase.getTestingCluster().getConfiguration().getIntVar(ConfVars.REST_SERVICE_PORT);
    restServiceURI = new URI("http", null, "127.0.0.1", restPort, "/rest", null, null);
    sessionsURI = new URI(restServiceURI + "/sessions");
    queriesURI = new URI(restServiceURI + "/databases/" + TajoConstants.DEFAULT_DATABASE_NAME + "/queries");
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
  
  private String sendNewQueryResquest(String query) throws Exception {
    String sessionId = generateNewSessionAndGetId();
    
    SubmitQueryRequest request = new SubmitQueryRequest();
    request.setQuery(query);
    
    Response response = restClient.target(queriesURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    String locationHeader = response.getHeaderString("Location");
    assertTrue(locationHeader != null && !locationHeader.isEmpty());
    
    String queryId = locationHeader.lastIndexOf('/') >= 0?
        locationHeader.substring(locationHeader.lastIndexOf('/')+1):null;
        
    assertTrue(queryId != null && !queryId.isEmpty());
    
    return queryId;
  }
}
