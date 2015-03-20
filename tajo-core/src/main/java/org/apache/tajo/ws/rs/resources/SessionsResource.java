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
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.session.InvalidSessionException;
import org.apache.tajo.ws.rs.ClientApplication;
import org.apache.tajo.ws.rs.ResourceConfigUtil;
import org.apache.tajo.ws.rs.requests.NewSessionRequest;
import org.apache.tajo.ws.rs.responses.NewSessionResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;

import java.net.URI;

@Path("/sessions")
public class SessionsResource {

  private final Log LOG = LogFactory.getLog(getClass());

  @Context
  UriInfo uriInfo;

  @Context
  Application application;

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
    
    Application localApp = ResourceConfigUtil.getJAXRSApplication(application);
    
    if (localApp instanceof ClientApplication) {
      ClientApplication clientApplication = (ClientApplication) localApp;
      if (request == null || request.getUserName() == null || request.getUserName().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }

      String userName = request.getUserName();
      String databaseName = request.getDatabaseName();
      if (databaseName == null || databaseName.isEmpty()) {
        databaseName = TajoConstants.DEFAULT_DATABASE_NAME;
      }

      TajoMaster.MasterContext masterContext = clientApplication.getMasterContext();

      try {
        NewSessionResponse sessionResponse = new NewSessionResponse();
        String sessionId = masterContext.getSessionManager().createSession(userName, databaseName);
        
        LOG.info("Session " + sessionId + " is created. ");
        
        sessionResponse.setId(sessionId);
        sessionResponse.setResultCode(ClientProtos.ResultCode.OK);
        sessionResponse.setVariables(masterContext.getSessionManager().getAllVariables(sessionId));

        URI newSessionUri = uriInfo.getBaseUriBuilder()
            .path(SessionsResource.class).path(sessionId).build();

        return Response.created(newSessionUri).entity(sessionResponse).build();
      } catch (InvalidSessionException e) {
        LOG.error(e.getMessage(), e);
        
        NewSessionResponse sessionResponse = new NewSessionResponse();
        sessionResponse.setResultCode(ClientProtos.ResultCode.ERROR);
        sessionResponse.setMessage(e.getMessage());

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(sessionResponse).build();
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
        
        NewSessionResponse sessionResponse = new NewSessionResponse();
        sessionResponse.setResultCode(ClientProtos.ResultCode.ERROR);
        sessionResponse.setMessage(e.getMessage());

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(sessionResponse).build();
      }
    } else {
      NewSessionResponse sessionResponse = new NewSessionResponse();
      sessionResponse.setResultCode(ClientProtos.ResultCode.ERROR);
      sessionResponse.setMessage("Invalid injection on SessionsResource.");
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(sessionResponse).build();
    }
  }
}
