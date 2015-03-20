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
import javax.ws.rs.core.Response.StatusType;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.apache.tajo.ws.rs.requests.NewSessionRequest;
import org.apache.tajo.ws.rs.responses.NewSessionResponse;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSessionsResource extends QueryTestCaseBase {
  
  private URI restServiceURI;
  private URI sessionsURI;
  private Client restClient;

  public TestSessionsResource() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }
  
  @Before
  public void setUp() throws Exception {
    int restPort = testBase.getTestingCluster().getConfiguration().getIntVar(ConfVars.REST_SERVICE_PORT);
    restServiceURI = new URI("http", null, "127.0.0.1", restPort, "/rest", null, null);
    sessionsURI = new URI(restServiceURI + "/sessions");
    restClient = ClientBuilder.newBuilder()
        .register(GsonFeature.class)
        .register(LoggingFilter.class)
        .property(ClientProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
        .property(ClientProperties.METAINF_SERVICES_LOOKUP_DISABLE, true)
        .build();
  }
  
  @After
  public void tearDown() throws Exception {
    restClient.close();
  }
  
  private NewSessionRequest createNewSessionRequest() {
    NewSessionRequest request = new NewSessionRequest();
    
    request.setUserName("tajo-user1");
    request.setDatabaseName("default");
    
    return request;
  }
  
  @Test
  public void testNewSession() throws Exception {    
    NewSessionRequest request = createNewSessionRequest();
    
    assertNotNull(request);
    
    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);
    
    assertNotNull(response);
    assertNotNull(response.getId());
    assertTrue(response.getMessage() == null || response.getMessage().isEmpty());
    assertFalse(response.getId().isEmpty());
  }
  
  @Test
  public void testNewSessionWithoutDBName() throws Exception {
    NewSessionRequest request = new NewSessionRequest();
    request.setUserName("tajo-user");
    
    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);
    
    assertNotNull(response);
    assertNotNull(response.getId());
    assertTrue(response.getMessage() == null || response.getMessage().isEmpty());
    assertFalse(response.getId().isEmpty());
  }
  
  @Test
  public void testNewSessionWithBadRequest() throws Exception {
    NewSessionRequest request = new NewSessionRequest();
    request.setDatabaseName("default");
    
    Response response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON));
    
    assertNotNull(response);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }
}
