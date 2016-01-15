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
import java.util.HashMap;
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
    InetSocketAddress address = testBase.getTestingCluster().getConfiguration().getSocketAddrVar(ConfVars.REST_SERVICE_ADDRESS);
    restServiceURI = new URI("http", null, address.getHostName(), address.getPort(), "/rest", null, null);
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

  @Test
  public void testRemoveSession() throws Exception {
    NewSessionRequest request = new NewSessionRequest();
    request.setUserName("tajo-user");

    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);

    assertNotNull(response);
    assertTrue(response.getId() != null && !response.getId().isEmpty());

    Response restResponse = restClient.target(sessionsURI)
        .path("/{session-id}").resolveTemplate("session-id", response.getId())
        .request().delete();

    assertNotNull(restResponse);
    assertEquals(Status.OK.getStatusCode(), restResponse.getStatus());
  }

  @Test
  public void testRemoveSessionNotFound() throws Exception {
    String invalidSessionId = "invalid";

    Response response = restClient.target(sessionsURI)
        .path("/{session-id}").resolveTemplate("session-id", invalidSessionId)
        .request().delete();

    assertNotNull(response);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetAllSessionVariables() throws Exception {
    NewSessionRequest request = new NewSessionRequest();
    request.setUserName("tajo-user");

    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);

    assertNotNull(response);
    assertTrue(response.getId() != null && !response.getId().isEmpty());

    Map<String, Map<String, String>> variablesMap = restClient.target(sessionsURI)
        .path("/{session-id}/variables").resolveTemplate("session-id", response.getId())
        .request().get(new GenericType<>(Map.class));

    assertNotNull(variablesMap);
    assertTrue(variablesMap.containsKey("variables"));
  }

  @Test
  public void testGetAllSessionVariablesNotFound() throws Exception {
    String invalidSessionId = "invalid";

    Response response = restClient.target(sessionsURI)
        .path("/{session-id}/variables").resolveTemplate("session-id", invalidSessionId)
        .request().get();

    assertNotNull(response);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testUpdateSessionVariables() throws Exception {
    NewSessionRequest request = new NewSessionRequest();
    request.setUserName("tajo-user");

    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);

    assertNotNull(response);
    assertTrue(response.getId() != null && !response.getId().isEmpty());

    Map<String, String> variablesMap = new HashMap<>();
    variablesMap.put("variableA", "valueA");
    variablesMap.put("variableB", "valueB");
    Map<String, Map<String, String>> variables = new HashMap<>();
    variables.put("variables", variablesMap);
    Response restResponse = restClient.target(sessionsURI)
        .path("/{session-id}/variables").resolveTemplate("session-id", response.getId())
        .request().put(Entity.entity(variables, MediaType.APPLICATION_JSON));

    assertNotNull(restResponse);
    assertEquals(Status.OK.getStatusCode(), restResponse.getStatus());
    
    Map<String, Map<String, String>> retrievedVariables = restClient.target(sessionsURI)
        .path("/{session-id}/variables").resolveTemplate("session-id", response.getId())
        .request().get(new GenericType<>(Map.class));
    
    assertNotNull(retrievedVariables);
    assertFalse(retrievedVariables.isEmpty());
    
    Map<String, String> retrievedVariablesMap = retrievedVariables.get("variables");
    
    assertNotNull(retrievedVariablesMap);
    assertFalse(retrievedVariablesMap.isEmpty());
    
    assertTrue(retrievedVariablesMap.containsKey("variableA"));
    assertTrue(retrievedVariablesMap.containsKey("variableB"));
    assertTrue("valueA".equals(retrievedVariablesMap.get("variableA")));
    assertTrue("valueB".equals(retrievedVariablesMap.get("variableB")));
  }
  
  @Test
  public void testUpdateSessionVariable() throws Exception {
    NewSessionRequest request = new NewSessionRequest();
    request.setUserName("tajo-user");

    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);

    assertNotNull(response);
    assertTrue(response.getId() != null && !response.getId().isEmpty());

    Map<String, String> variablesMap = new HashMap<>();
    variablesMap.put("variableA", "valueA");
    Response restResponse = restClient.target(sessionsURI)
        .path("/{session-id}/variables").resolveTemplate("session-id", response.getId())
        .request().put(Entity.entity(variablesMap, MediaType.APPLICATION_JSON));

    assertNotNull(restResponse);
    assertEquals(Status.OK.getStatusCode(), restResponse.getStatus());
    
    Map<String, Map<String, String>> retrievedVariables = restClient.target(sessionsURI)
        .path("/{session-id}/variables").resolveTemplate("session-id", response.getId())
        .request().get(new GenericType<>(Map.class));
    
    assertNotNull(retrievedVariables);
    assertFalse(retrievedVariables.isEmpty());
    
    Map<String, String> retrievedVariablesMap = retrievedVariables.get("variables");
    
    assertNotNull(retrievedVariablesMap);
    assertFalse(retrievedVariablesMap.isEmpty());
    
    assertTrue(retrievedVariablesMap.containsKey("variableA"));
    assertTrue("valueA".equals(retrievedVariablesMap.get("variableA")));
  }
}
