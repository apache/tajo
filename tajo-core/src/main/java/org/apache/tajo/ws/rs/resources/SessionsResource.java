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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.session.InvalidSessionException;
import org.apache.tajo.session.Session;
import org.apache.tajo.ws.rs.*;
import org.apache.tajo.ws.rs.requests.NewSessionRequest;
import org.apache.tajo.ws.rs.responses.ExceptionResponse;
import org.apache.tajo.ws.rs.responses.NewSessionResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Path("/sessions")
public class SessionsResource {

  private static final Log LOG = LogFactory.getLog(SessionsResource.class);

  @Context
  UriInfo uriInfo;

  @Context
  Application application;
  
  JerseyResourceDelegateContext context;
  
  private static final String newSessionRequestKeyName = "NewSessionRequest";
  private static final String sessionIdKeyName = "SessionId";
  private static final String variablesKeyName = "VariablesMap";
  
  private static final String variablesOutputKeyName = "variables";
  
  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
    context.put(uriInfoKey, uriInfo);
  }

  /**
   * Creates a new client session.
   * 
   * @param request
   * @return
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createNewSession(NewSessionRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a new session request. : " + request);
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<NewSessionRequest> newSessionRequestKey = 
          JerseyResourceDelegateContextKey.valueOf(newSessionRequestKeyName, NewSessionRequest.class);
      context.put(newSessionRequestKey, request);

      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new CreateNewSessionDelegate(), 
          application, 
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    return response;
  }
  
  private static class CreateNewSessionDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      try {
        JerseyResourceDelegateContextKey<NewSessionRequest> sessionRequestKey =
            JerseyResourceDelegateContextKey.valueOf(newSessionRequestKeyName, NewSessionRequest.class);
        NewSessionRequest request = context.get(sessionRequestKey);
        
        if (request.getUserName() == null || request.getUserName().isEmpty()) {
          return ResourcesUtil.createBadRequestResponse(LOG, "userName is null or empty.");
        }

        String userName = request.getUserName();
        String databaseName = request.getDatabaseName();
        if (databaseName == null || databaseName.isEmpty()) {
          databaseName = TajoConstants.DEFAULT_DATABASE_NAME;
        }
        
        JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
            JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
        TajoMaster.MasterContext masterContext = context.get(masterContextKey);
        
        NewSessionResponse sessionResponse = new NewSessionResponse();
        String sessionId = masterContext.getSessionManager().createSession(userName, databaseName);
        
        LOG.info("Session " + sessionId + " is created. ");
        
        sessionResponse.setId(sessionId);
        sessionResponse.setResultCode(ResultCode.OK);
        sessionResponse.setVariables(masterContext.getSessionManager().getAllVariables(sessionId));
        
        JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
            JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
        UriInfo uriInfo = context.get(uriInfoKey);

        URI newSessionUri = uriInfo.getBaseUriBuilder()
            .path(SessionsResource.class).path(sessionId).build();

        return Response.created(newSessionUri).entity(sessionResponse).build();
      } catch (InvalidSessionException e) {
        LOG.error(e.getMessage(), e);
        
        NewSessionResponse sessionResponse = new NewSessionResponse();
        sessionResponse.setResultCode(ResultCode.INTERNAL_ERROR);
        sessionResponse.setMessage(e.getMessage());

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(sessionResponse).build();
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
        
        NewSessionResponse sessionResponse = new NewSessionResponse();
        sessionResponse.setResultCode(ResultCode.INTERNAL_ERROR);
        sessionResponse.setMessage(e.getMessage());

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(sessionResponse).build();
      }
    }
    
  }

  /**
   * Removes existing sessions.
   * 
   * @param sessionId
   * @return
   */
  @DELETE
  @Path("/{session-id}")
  public Response removeSession(@PathParam("session-id") String sessionId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent remove session request : Session Id (" + sessionId + ")");
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      context.put(sessionIdKey, sessionId);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new RemoveSessionDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class RemoveSessionDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      TajoMaster.MasterContext masterContext = context.get(masterContextKey);
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      String sessionId = context.get(sessionIdKey);

      Session session = masterContext.getSessionManager().removeSession(sessionId);

      if (session != null) {
        LOG.info("Session " + sessionId + " is removed.");

        return Response.status(Response.Status.OK).build();
      } else {
        ExceptionResponse response = new ExceptionResponse();
        response.setMessage("Unable to find a session (" + sessionId + ")");

        return Response.status(Response.Status.NOT_FOUND).entity(response).build();
      }
    }
    
  }

  /**
   * Retrieves all session variables
   * 
   * @param sessionId
   * @return
   */
  @GET
  @Path("/{session-id}/variables")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllSessionVariables(@PathParam("session-id") String sessionId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a GetAllSessionVariables request for a session : " + sessionId);
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      context.put(sessionIdKey, sessionId);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetAllSessionVariablesDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetAllSessionVariablesDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      TajoMaster.MasterContext masterContext = context.get(masterContextKey);
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      String sessionId = context.get(sessionIdKey);

      try {
        Map<String, Map<String, String>> variablesMap = new HashMap<>();
        variablesMap.put(variablesOutputKeyName,
            masterContext.getSessionManager().getAllVariables(sessionId));
        GenericEntity<Map<String, Map<String, String>>> variablesEntity =
                new GenericEntity<>(variablesMap, Map.class);
        return Response.ok(variablesEntity).build();
      } catch (InvalidSessionException e) {
        LOG.error("Unable to find a session : " + sessionId);

        return Response.status(Response.Status.NOT_FOUND).build();
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);

        return ResourcesUtil.createExceptionResponse(null, e.getMessage());
      }
    }
    
  }

  /**
   * Updates the specified session varaible or variables
   * 
   * @param sessionId
   * @param variables
   * @return
   */
  @PUT
  @Path("/{session-id}/variables")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateSessionVariables(@PathParam("session-id") String sessionId, 
      Map<String, Object> variables) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a update session variables request for a session : " + sessionId);
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      context.put(sessionIdKey, sessionId);
      JerseyResourceDelegateContextKey<Map> variablesMapKey =
          JerseyResourceDelegateContextKey.valueOf(variablesKeyName, Map.class);
      context.put(variablesMapKey, variables);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new UpdateSessionVariablesDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class UpdateSessionVariablesDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> sessionIdKey =
          JerseyResourceDelegateContextKey.valueOf(sessionIdKeyName, String.class);
      String sessionId = context.get(sessionIdKey);
      JerseyResourceDelegateContextKey<Map> variablesKey =
          JerseyResourceDelegateContextKey.valueOf(variablesKeyName, Map.class);
      Map<String, Object> variables = context.get(variablesKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      TajoMaster.MasterContext masterContext = context.get(masterContextKey);

      try {
        if (variables.containsKey(variablesOutputKeyName)) {
          Map<String, String> variablesMap = (Map<String, String>) variables.get(variablesOutputKeyName);
          for (Map.Entry<String, String> variableEntry: variablesMap.entrySet()) {
            masterContext.getSessionManager().setVariable(sessionId, variableEntry.getKey(), variableEntry.getValue());
          }

          return Response.ok().build();
        } else {
          Iterator<Map.Entry<String, Object>> iterator = variables.entrySet().iterator();
          if (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();

            masterContext.getSessionManager().setVariable(sessionId, entry.getKey(), (String) entry.getValue());

            return Response.ok().build();
          } else {
            return ResourcesUtil.createBadRequestResponse(LOG, "At least one variable is required.");
          }
        }
      } catch (InvalidSessionException e) {
        LOG.error("Unable to find a session : " + sessionId);

        return Response.status(Response.Status.NOT_FOUND).build();
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);

        return ResourcesUtil.createExceptionResponse(null, e.getMessage());
      }
    } 
  }
}
