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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.rm.NodeStatus;
import org.apache.tajo.ws.rs.JerseyResourceDelegate;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContext;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContextKey;
import org.apache.tajo.ws.rs.JerseyResourceDelegateUtil;
import org.apache.tajo.ws.rs.ResourcesUtil;
import org.apache.tajo.ws.rs.responses.WorkerResponse;

@Path("/cluster")
public class ClusterResource {
  
  private static final Log LOG = LogFactory.getLog(ClusterResource.class);
  
  @Context
  UriInfo uriInfo;
  
  @Context
  Application application;
  
  JerseyResourceDelegateContext context;
  
  private static final String workersName = "workers";
  
  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
    context.put(uriInfoKey, uriInfo);
  }
  
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getClusterInfo() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a get cluster info request.");
    }
    
    Response response = null;
    
    try {
      initializeContext();
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetClusterInfoDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetClusterInfoDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      Map<Integer, NodeStatus> workerMap = masterContext.getResourceManager().getNodes();
      List<WorkerResponse> workerList = workerMap.values().stream().map(WorkerResponse::new).collect(Collectors.toList());

      Map<String, List<WorkerResponse>> workerResponseMap = new HashMap<>();
      workerResponseMap.put(workersName, workerList);
      
      return Response.ok(workerResponseMap).build();
    }
    
  }

}
