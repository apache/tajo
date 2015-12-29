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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestFunctionsResource extends QueryTestCaseBase {
  
  private URI restServiceURI;
  private URI functionsURI;
  private Client restClient;
  
  public TestFunctionsResource() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }
  
  @Before
  public void setUp() throws Exception {
    InetSocketAddress address = testBase.getTestingCluster().getConfiguration().getSocketAddrVar(ConfVars.REST_SERVICE_ADDRESS);
    restServiceURI = new URI("http", null, address.getHostName(), address.getPort(), "/rest", null, null);
    functionsURI = new URI(restServiceURI + "/functions");
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
  public void testGetAllFunctions() throws Exception {
    List<FunctionSignature> functionSignatures =
        restClient.target(functionsURI)
        .request().get(new GenericType<>(List.class));
    
    assertNotNull(functionSignatures);
    assertTrue(functionSignatures.size() > 0);
  }
}
