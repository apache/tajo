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

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.ipc.ClientProtos.ResultCode;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.exec.NonForwardQueryResultFileScanner;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.PartitionedTableScanNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.ws.rs.*;
import org.apache.tajo.ws.rs.responses.GetQueryResultDataResponse;
import org.apache.tajo.ws.rs.responses.ResultSetInfoResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

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
  private static final String cacheIdKeyName = "cacheId";
  private static final String offsetKeyName = "offset";
  private static final String countKeyName = "count";

  private static final String tajoDigestHeaderName = "X-Tajo-Digest";

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
  
  private static NonForwardQueryResultScanner getNonForwardQueryResultScanner(
      MasterContext masterContext,
      Session session,
      QueryId queryId) throws IOException {
    NonForwardQueryResultScanner resultScanner = session.getNonForwardQueryResultScanner(queryId);
    if (resultScanner == null) {
      QueryInfo queryInfo = masterContext.getQueryJobManager().getFinishedQuery(queryId);
      if (queryInfo == null) {
        throw new RuntimeException("QueryInfo isnull.");
      }

      TableDesc resultTableDesc = queryInfo.getResultDesc();
      if (resultTableDesc == null) {
        throw new RuntimeException("Result Table Desc is null.");
      }

      ScanNode scanNode;
      if (resultTableDesc.hasPartition()) {
        scanNode = LogicalPlan.createNodeWithoutPID(PartitionedTableScanNode.class);
        scanNode.init(resultTableDesc);
      } else {
        scanNode = LogicalPlan.createNodeWithoutPID(ScanNode.class);
        scanNode.init(resultTableDesc);
      }

      resultScanner = new NonForwardQueryResultFileScanner(masterContext.getConf(), session.getSessionId(), queryId,
          scanNode, resultTableDesc, Integer.MAX_VALUE);
      resultScanner.init();
      session.addNonForwardQueryResultScanner(resultScanner);
    }

    return resultScanner;
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
      JerseyResourceDelegateContextKey<ClientApplication> clientApplicationKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.ClientApplicationKey, ClientApplication.class);
      ClientApplication clientApplication = context.get(clientApplicationKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
      UriInfo uriInfo = context.get(uriInfoKey);
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      String databaseName = context.get(databaseNameKey);
      
      try {
        masterContext.getSessionManager().touch(sessionId);
        Session session = masterContext.getSessionManager().getSession(sessionId);
        QueryId queryIdObj = TajoIdUtils.parseQueryId(queryId);
        
        masterContext.getSessionManager().touch(sessionId);
        
        QueryInfo queryInfo = masterContext.getQueryJobManager().getFinishedQuery(queryIdObj);
        GetQueryResultDataResponse response = new GetQueryResultDataResponse();
        
        if (queryInfo == null) {
          response.setResultCode(ResultCode.ERROR);
          response.setErrorMessage("Unable to find a query info for requested id : " + queryId);
          return Response.status(Status.NOT_FOUND).entity(response).build();
        }

        NonForwardQueryResultScanner queryResultScanner = getNonForwardQueryResultScanner(masterContext, session, queryIdObj);

        if (queryInfo.getResultDesc() != null && queryInfo.getResultDesc().getSchema() != null) {
          response.setSchema(queryInfo.getResultDesc().getSchema());
        } else {
          response.setSchema(queryResultScanner.getLogicalSchema());
        }

        long cacheId = clientApplication.generateCacheIdIfAbsent(queryIdObj);
        clientApplication.setCachedNonForwardResultScanner(queryIdObj, cacheId, queryResultScanner);
        URI resultSetCacheUri = uriInfo.getBaseUriBuilder()
            .path(QueryResource.class)
            .path(QueryResource.class, "getQueryResult")
            .path(QueryResultResource.class, "getQueryResultSet")
            .build(databaseName, queryId, cacheId);
        ResultSetInfoResponse resultSetInfoResponse = new ResultSetInfoResponse();
        resultSetInfoResponse.setId(cacheId);
        resultSetInfoResponse.setLink(resultSetCacheUri);
        response.setResultset(resultSetInfoResponse);
        response.setResultCode(ResultCode.OK);
        
        return Response.status(Status.OK).entity(response).build();
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
        
        GetQueryResultDataResponse response = new GetQueryResultDataResponse();
        response.setResultCode(ResultCode.ERROR);
        response.setErrorMessage(e.getMessage());
        response.setErrorTrace(org.apache.hadoop.util.StringUtils.stringifyException(e));
        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(response).build();
      }
    }
  }

  @GET
  @Path("{cacheId}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response getQueryResultSet(@HeaderParam(QueryResource.tajoSessionIdHeaderName) String sessionId,
      @PathParam("cacheId") String cacheId,
      @DefaultValue("-1") @QueryParam("offset") int offset,
      @DefaultValue("-1") @QueryParam("count") int count) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a get query result set request.");
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      context.put(sessionIdKey, sessionId);
      JerseyResourceDelegateContextKey<Long> cacheIdKey =
          JerseyResourceDelegateContextKey.valueOf(cacheIdKeyName, Long.class);
      context.put(cacheIdKey, Long.valueOf(cacheId));
      JerseyResourceDelegateContextKey<Integer> offsetKey =
          JerseyResourceDelegateContextKey.valueOf(offsetKeyName, Integer.class);
      context.put(offsetKey, offset);
      JerseyResourceDelegateContextKey<Integer> countKey =
          JerseyResourceDelegateContextKey.valueOf(countKeyName, Integer.class);
      context.put(countKey, count);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetQueryResultSetDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetQueryResultSetDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      String sessionId = context.get(sessionIdKey);
      JerseyResourceDelegateContextKey<String> queryIdKey =
          JerseyResourceDelegateContextKey.valueOf(queryIdKeyName, String.class);
      String queryId = context.get(queryIdKey);
      JerseyResourceDelegateContextKey<Long> cacheIdKey =
          JerseyResourceDelegateContextKey.valueOf(cacheIdKeyName, Long.class);
      Long cacheId = context.get(cacheIdKey);
      JerseyResourceDelegateContextKey<ClientApplication> clientApplicationKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.ClientApplicationKey, ClientApplication.class);
      ClientApplication clientApplication = context.get(clientApplicationKey);
      JerseyResourceDelegateContextKey<Integer> offsetKey =
          JerseyResourceDelegateContextKey.valueOf(offsetKeyName, Integer.class);
      int offset = context.get(offsetKey);
      JerseyResourceDelegateContextKey<Integer> countKey =
          JerseyResourceDelegateContextKey.valueOf(countKeyName, Integer.class);
      int count = context.get(countKey);
      
      if (sessionId == null || sessionId.isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "Session id is required. Please refer the header " + 
            QueryResource.tajoSessionIdHeaderName);
      }
      
      if (queryId == null || queryId.isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "Query id is required. Please specify the query id");
      }
      
      QueryId queryIdObj;
      try {
        queryIdObj = TajoIdUtils.parseQueryId(queryId);
      } catch (Throwable e) {
        return ResourcesUtil.createExceptionResponse(LOG, "Invalid query id : " + queryId);
      }
      
      if (cacheId == null || cacheId.longValue() == 0) {
        return ResourcesUtil.createBadRequestResponse(LOG, "Cache id is null or empty.");
      }

      if (count < 0) {
        return ResourcesUtil.createBadRequestResponse(LOG, "Invalid count value : " + count);
      }

      NonForwardQueryResultScanner cachedQueryResultScanner =
          clientApplication.getCachedNonForwardResultScanner(queryIdObj, cacheId.longValue());

      try {
        skipOffsetRow(cachedQueryResultScanner, offset);

        List<ByteString> output = cachedQueryResultScanner.getNextRows(count);
        String digestString = getEncodedBase64DigestString(output);

        return Response.ok(new QueryResultStreamingOutput(output))
            .header(tajoDigestHeaderName, digestString)
            .build();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);

        return ResourcesUtil.createExceptionResponse(null, e.getMessage());
      } catch (NoSuchAlgorithmException e) {
        LOG.error(e.getMessage(), e);

        return ResourcesUtil.createExceptionResponse(null, e.getMessage());
      }
    }

    private void skipOffsetRow(NonForwardQueryResultScanner queryResultScanner, int offset) throws IOException {
      if (offset < 0) {
        return;
      }

      int currentRow = queryResultScanner.getCurrentRowNumber();

      if (offset < (currentRow+1)) {
        throw new RuntimeException("Offset must be over the current row number");
      }

      queryResultScanner.getNextRows(offset - currentRow - 1);
    }

    private String getEncodedBase64DigestString(List<ByteString> outputList) throws NoSuchAlgorithmException {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");

      for (ByteString byteString: outputList) {
        messageDigest.update(byteString.toByteArray());
      }

      return Base64.encodeBase64String(messageDigest.digest());
    }
  }

  private static class QueryResultStreamingOutput implements StreamingOutput {

    private final List<ByteString> outputList;

    public QueryResultStreamingOutput(List<ByteString> outputList) {
      this.outputList = outputList;
    }

    @Override
    public void write(OutputStream outputStream) throws IOException, WebApplicationException {
      DataOutputStream streamingOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));

      for (ByteString byteString: outputList) {
        byte[] byteStringArray = byteString.toByteArray();
        streamingOutputStream.writeInt(byteStringArray.length);
        streamingOutputStream.write(byteStringArray);
      }

      streamingOutputStream.flush();
    }
  }
  
}
