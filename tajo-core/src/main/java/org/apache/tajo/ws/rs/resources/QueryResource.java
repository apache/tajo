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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.ws.rs.JerseyResourceDelegate;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContext;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContextKey;
import org.apache.tajo.ws.rs.JerseyResourceDelegateUtil;

@Path("/databases/{databaseName}/queries")
public class QueryResource {

  private static final Log LOG = LogFactory.getLog(QueryResource.class);
  
  @Context
  UriInfo uriInfo;
  
  @Context
  Application application;
  
  JerseyResourceDelegateContext context;
  
  private static final String stateKeyName = "state";
  private static final String startTimeKeyName = "startTime";
  private static final String endTimeKeyname = "endTime";
  
  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey);
    context.put(uriInfoKey, uriInfo);
  }
  
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllQueries(@QueryParam("state") String state,
      @QueryParam("startTime") long startTime,
      @QueryParam("endTime") long endTime) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a get all queries request.");
    }
    
    Response response = null;
    
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> stateKey =
          JerseyResourceDelegateContextKey.valueOf(stateKeyName);
      context.put(stateKey, state);
      JerseyResourceDelegateContextKey<Long> startTimeKey =
          JerseyResourceDelegateContextKey.valueOf(startTimeKeyName);
      context.put(startTimeKey, startTime);
      JerseyResourceDelegateContextKey<Long> endTimeKey =
          JerseyResourceDelegateContextKey.valueOf(endTimeKeyname);
      context.put(endTimeKey, endTime);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetAllQueriesDelegate(),
          application,
          context,
          LOG);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    
    return response;
  }
  
  private static class GetAllQueriesDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> stateKey =
          JerseyResourceDelegateContextKey.valueOf(stateKeyName);
      String state = context.get(stateKey);
      JerseyResourceDelegateContextKey<Long> startTimeKey =
          JerseyResourceDelegateContextKey.valueOf(startTimeKeyName);
      long startTime = context.get(startTimeKey);
      JerseyResourceDelegateContextKey<Long> endTimeKey =
          JerseyResourceDelegateContextKey.valueOf(endTimeKeyname);
      long endTime = context.get(endTimeKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey);
      MasterContext masterContext = context.get(masterContextKey);
      return null;
    }
  }
}
