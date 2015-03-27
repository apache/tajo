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
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.ipc.ClientProtos.ResultCode;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.ws.rs.JerseyResourceDelegate;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContext;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContextKey;
import org.apache.tajo.ws.rs.JerseyResourceDelegateUtil;
import org.apache.tajo.ws.rs.ResourcesUtil;
import org.apache.tajo.ws.rs.responses.GetQueryResultDataResponse;

public class QueryResultResource {
  
  private static final Log LOG = LogFactory.getLog(QueryResultResource.class);
  
  private UriInfo uriInfo;
  
  private Application application;
  
  private String databaseName;

  private String queryId;
  
  private JerseyResourceDelegateContext context;
  
  private static final String databaseNameKeyName = "databaseName";
  private static final String queryIdKeyName = "queryId";
  private static final String sessionIdKeyName = "sessionId";

  public UriInfo getUriInfo() {
    return uriInfo;
  }

  public void setUriInfo(UriInfo uriInfo) {
    this.uriInfo = uriInfo;
  }

  public Application getApplication() {
    return application;
  }

  public void setApplication(Application application) {
    this.application = application;
  }
  
  public String getDatabaseName() {
    return databaseName;
  }
  
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }
  
  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
    context.put(uriInfoKey, uriInfo);
    JerseyResourceDelegateContextKey<String> databaseNameKey =
        JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
    context.put(databaseNameKey, databaseName);
    JerseyResourceDelegateContextKey<String> queryIdKey =
        JerseyResourceDelegateContextKey.valueOf(queryIdKeyName, String.class);
    context.put(queryIdKey, queryId);
  }
  
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueryResult(@HeaderParam(QueryResource.tajoSessionIdHeaderName) String sessionId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a get query result request.");
    }
    
    Response response = null;
    
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      context.put(sessionIdKey, sessionId);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetQueryResultDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetQueryResultDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      String sessionId = context.get(sessionIdKey);
      JerseyResourceDelegateContextKey<String> queryIdKey =
          JerseyResourceDelegateContextKey.valueOf(queryIdKeyName, String.class);
      String queryId = context.get(queryIdKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      try {
        masterContext.getSessionManager().touch(sessionId);
        Session session = masterContext.getSessionManager().getSession(sessionId);
        QueryId queryIdObj = TajoIdUtils.parseQueryId(queryId);
        
        QueryInfo queryInfo = masterContext.getQueryJobManager().getFinishedQuery(queryIdObj);
        GetQueryResultDataResponse response = new GetQueryResultDataResponse();
        
        if (queryInfo == null) {
          response.setResultCode(ResultCode.ERROR);
          response.setErrorMessage("Unable to find a query info for requested id : " + queryId);
          return Response.status(Status.NOT_FOUND).entity(response).build();
        }
        
        response.setSchema(queryInfo.getResultDesc().getSchema());
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
        
        GetQueryResultDataResponse response = new GetQueryResultDataResponse();
        response.setResultCode(ResultCode.ERROR);
        response.setErrorMessage(e.getMessage());
        response.setErrorTrace(org.apache.hadoop.util.StringUtils.stringifyException(e));
        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(response).build();
      }
      
      return null;
    }
  }
  
}
