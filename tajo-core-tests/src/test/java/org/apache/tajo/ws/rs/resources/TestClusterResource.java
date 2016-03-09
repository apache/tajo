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
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestClusterResource extends QueryTestCaseBase {
  
  private URI restServiceURI;
  private URI clusterURI;
  private Client restClient;

  public TestClusterResource() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }
  
  @Before
  public void setUp() throws Exception {
    InetSocketAddress address = testBase.getTestingCluster().getConfiguration().getSocketAddrVar(ConfVars.REST_SERVICE_ADDRESS);
    restServiceURI = new URI("http", null, address.getHostName(), address.getPort(), "/rest", null, null);
    clusterURI = new URI(restServiceURI + "/cluster");
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
  public void testGetCluster() throws Exception {
    Map<String, List<Object>> workerMap =
        restClient.target(clusterURI)
        .request().get(new GenericType<>(Map.class));
    
    assertNotNull(workerMap);
    assertFalse(workerMap.isEmpty());
    assertNotNull(workerMap.get("workers"));
    
    List<Object> workerList = workerMap.get("workers");
    
    assertTrue(workerList.size() > 0);
  }
}
