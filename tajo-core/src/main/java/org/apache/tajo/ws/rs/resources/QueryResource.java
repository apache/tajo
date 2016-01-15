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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.exception.ReturnStateUtil;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;
import org.apache.tajo.master.QueryInProgress;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.QueryManager;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.querymaster.QueryJobEvent;
import org.apache.tajo.exception.InvalidSessionException;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.ws.rs.*;
import org.apache.tajo.ws.rs.requests.SubmitQueryRequest;
import org.apache.tajo.ws.rs.responses.GetSubmitQueryResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/queries")
public class QueryResource {

  private static final Log LOG = LogFactory.getLog(QueryResource.class);
  
  @Context
  UriInfo uriInfo;
  
  @Context
  Application application;

  JerseyResourceDelegateContext context;
  
  protected static final String tajoSessionIdHeaderName = "X-Tajo-Session";
  
  private static final String stateKeyName = "state";
  private static final String startTimeKeyName = "startTime";
  private static final String endTimeKeyName = "endTime";
  private static final String sessionIdKeyName = "sessionId";
  private static final String submitQueryRequestKeyName = "submitQueryRequest";
  private static final String printTypeKeyName = "printType";
  private static final String queryIdKeyName = "queryId";
  private static final String defaultQueryInfoPrintType = "COMPLICATED";

  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
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
          JerseyResourceDelegateContextKey.valueOf(stateKeyName, String.class);
      if (state != null && !state.isEmpty()) {
        context.put(stateKey, state);
      }
      JerseyResourceDelegateContextKey<Long> startTimeKey =
          JerseyResourceDelegateContextKey.valueOf(startTimeKeyName, Long.class);
      if (startTime > 0) {
        context.put(startTimeKey, startTime);
      }
      JerseyResourceDelegateContextKey<Long> endTimeKey =
          JerseyResourceDelegateContextKey.valueOf(endTimeKeyName, Long.class);
      if (endTime > 0) {
        context.put(endTimeKey, endTime);
      }
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetAllQueriesDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetAllQueriesDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> stateKey =
          JerseyResourceDelegateContextKey.valueOf(stateKeyName, String.class);
      String state = context.get(stateKey);
      JerseyResourceDelegateContextKey<Long> startTimeKey =
          JerseyResourceDelegateContextKey.valueOf(startTimeKeyName, Long.class);
      long startTime = context.get(startTimeKey);
      JerseyResourceDelegateContextKey<Long> endTimeKey =
          JerseyResourceDelegateContextKey.valueOf(endTimeKeyName, Long.class);
      long endTime = context.get(endTimeKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      TajoProtos.QueryState queryState = null;
      try {
        if (state != null && !state.isEmpty()) {
          queryState = TajoProtos.QueryState.valueOf(state);
        }
      } catch (Exception e) {
        return ResourcesUtil.createBadRequestResponse(LOG, state + " is not a valid query state.");
      }
      
      Map<String, List<QueryInfo>> queriesMap = new HashMap<>();
      List<QueryInfo> queriesInfo = new ArrayList<>();
      
      QueryManager queryManager = masterContext.getQueryJobManager();
      for (QueryInProgress queryInProgress: queryManager.getSubmittedQueries()) {
        queriesInfo.add(queryInProgress.getQueryInfo());
      }
      
      for (QueryInProgress queryInProgress: queryManager.getRunningQueries()) {
        queriesInfo.add(queryInProgress.getQueryInfo());
      }

      queriesInfo.addAll(queryManager.getFinishedQueries());
      
      if (state != null) {
        queriesInfo = selectQueriesInfoByState(queriesInfo, queryState);
      }
      
      if (startTime > 0 || endTime > 0) {
        queriesInfo = selectQueriesInfoByTime(queriesInfo, startTime, endTime);
      }
      queriesMap.put("queries", queriesInfo);
      
      return Response.ok(queriesMap).build();
    }
    
    private List<QueryInfo> selectQueriesInfoByState(List<QueryInfo> queriesInfo, TajoProtos.QueryState state) {
      List<QueryInfo> resultQueriesInfo = new ArrayList<>(queriesInfo.size() / 2);

      resultQueriesInfo.addAll(queriesInfo.stream().filter(queryInfo -> state.equals(queryInfo.getQueryState())).collect(Collectors.toList()));
      
      return resultQueriesInfo;
    }
    
    private List<QueryInfo> selectQueriesInfoByTime(List<QueryInfo> queriesInfo, long startTime, long endTime) {
      List<QueryInfo> resultQueriesInfo = new ArrayList<>(queriesInfo.size() / 2);
      
      for (QueryInfo queryInfo: queriesInfo) {
        if (queryInfo.getStartTime() > startTime) {
          resultQueriesInfo.add(queryInfo);
        }
        if (queryInfo.getStartTime() < endTime) {
          resultQueriesInfo.add(queryInfo);
        }
      }
      
      return resultQueriesInfo;
    }
  }
  
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response submitQuery(@HeaderParam(tajoSessionIdHeaderName) String sessionId,
      SubmitQueryRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a submit query request.");
    }

    Response response = null;

    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);

      if (sessionId == null || sessionId.isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "Session id is required. Please refer the header " +
                QueryResource.tajoSessionIdHeaderName);
      }

      context.put(sessionIdKey, sessionId);
      JerseyResourceDelegateContextKey<SubmitQueryRequest> submitQueryRequestKey =
          JerseyResourceDelegateContextKey.valueOf(submitQueryRequestKeyName, SubmitQueryRequest.class);
      context.put(submitQueryRequestKey, request);

      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new SubmitQueryDelegate(),
          application,
          context,
          LOG);

    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);

      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }

    return response;
  }

  private static class SubmitQueryDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      String sessionId = context.get(sessionIdKey);
      JerseyResourceDelegateContextKey<SubmitQueryRequest> submitQueryRequestKey =
          JerseyResourceDelegateContextKey.valueOf(submitQueryRequestKeyName, SubmitQueryRequest.class);
      SubmitQueryRequest request = context.get(submitQueryRequestKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);

      if (sessionId == null || sessionId.isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "Session Id is null or empty string.");
      }
      if (request == null || request.getQuery() == null || request.getQuery().isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "query is null or emptry string.");
      }

      Session session;
      try {
        session = masterContext.getSessionManager().getSession(sessionId);
      } catch (InvalidSessionException e) {
        return ResourcesUtil.createBadRequestResponse(LOG, "Provided session id (" + sessionId + ") is invalid.");
      }
      
      SubmitQueryResponse response =
        masterContext.getGlobalEngine().executeQuery(session, request.getQuery(), false);
      if (ReturnStateUtil.isError(response.getState())) {
        return ResourcesUtil.createExceptionResponse(LOG, response.getState().getMessage());
      } else {
        JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
        UriInfo uriInfo = context.get(uriInfoKey);

        QueryId queryId = new QueryId(response.getQueryId());
        URI queryURI = uriInfo.getBaseUriBuilder()
          .path(QueryResource.class)
          .path(QueryResource.class, "getQuery")
          .build(queryId.toString());

        GetSubmitQueryResponse queryResponse = new GetSubmitQueryResponse();
        if (queryId.isNull() == false) {
          queryResponse.setUri(queryURI);
        }

        queryResponse.setResultCode(response.getState().getReturnCode());
        queryResponse.setQuery(request.getQuery());
        return Response.status(Status.OK).entity(queryResponse).build();
      }
    }
  }
  
  @GET
  @Path("{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQuery(@PathParam("queryId") String queryId,
													 @DefaultValue(defaultQueryInfoPrintType) @QueryParam("print") String printType) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a get query request.");
    }
    
    Response response = null;
    
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> queryIdKey =
          JerseyResourceDelegateContextKey.valueOf(queryIdKeyName, String.class);
      context.put(queryIdKey, queryId);
      JerseyResourceDelegateContextKey<String> printTypeKey =
          JerseyResourceDelegateContextKey.valueOf(printTypeKeyName, String.class);

      context.put(printTypeKey, printType);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetQueryDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetQueryDelegate implements JerseyResourceDelegate {
    
    private static final String briefPrint = "BRIEF";

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> queryIdKey =
          JerseyResourceDelegateContextKey.valueOf(queryIdKeyName, String.class);
      String queryId = context.get(queryIdKey);
      JerseyResourceDelegateContextKey<String> printTypeKey =
          JerseyResourceDelegateContextKey.valueOf(printTypeKeyName, String.class);
      String printType = context.get(printTypeKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      QueryId queryIdObj = TajoIdUtils.parseQueryId(queryId);
      
      QueryManager queryManager = masterContext.getQueryJobManager();
      QueryInProgress queryInProgress = queryManager.getQueryInProgress(queryIdObj);
      
      QueryInfo queryInfo = null;
      if (queryInProgress == null) {
        queryInfo = queryManager.getFinishedQuery(queryIdObj);
      } else {
        queryInfo = queryInProgress.getQueryInfo();
      }
      
      if (queryInfo != null) {
        if (briefPrint.equalsIgnoreCase(printType)) {
          queryInfo = getBriefQueryInfo(queryInfo);
        }
        return Response.ok(queryInfo).build();
      } else {
        return Response.status(Status.NOT_FOUND).build();
      }
    }
    
    private QueryInfo getBriefQueryInfo(QueryInfo queryInfo) {
      QueryInfo newQueryInfo = new QueryInfo(queryInfo.getQueryId(), null, null, null);
      newQueryInfo.setQueryState(queryInfo.getQueryState());
      newQueryInfo.setStartTime(queryInfo.getStartTime());
      return newQueryInfo;
    }
  }
  
  @DELETE
  @Path("{queryId}")
  public Response terminateQuery(@PathParam("queryId") String queryId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a terminate query request.");
    }
    
    Response response = null;
    
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> queryIdKey =
          JerseyResourceDelegateContextKey.valueOf(queryIdKeyName, String.class);
      context.put(queryIdKey, queryId);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new TerminateQueryDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class TerminateQueryDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> queryIdKey =
          JerseyResourceDelegateContextKey.valueOf(queryIdKeyName, String.class);
      String queryId = context.get(queryIdKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      QueryId queryIdObj = TajoIdUtils.parseQueryId(queryId);
      
      QueryManager queryManager = masterContext.getQueryJobManager();
      queryManager.getEventHandler().handle(new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_KILL,
          new QueryInfo(queryIdObj)));
      return Response.ok().build();
    }
  }

  @Path("/{queryId}/result")
  public QueryResultResource getQueryResult(@PathParam("queryId") String queryId) {
    QueryResultResource queryResultResource = new QueryResultResource();
    queryResultResource.setUriInfo(uriInfo);
    queryResultResource.setApplication(application);
    queryResultResource.setQueryId(queryId);
    return queryResultResource;
  }
}
