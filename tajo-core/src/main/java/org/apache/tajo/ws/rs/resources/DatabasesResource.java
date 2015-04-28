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

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.proto.CatalogProtos.DatabaseProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TablespaceProto;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.ws.rs.JerseyResourceDelegate;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContext;
import org.apache.tajo.ws.rs.JerseyResourceDelegateContextKey;
import org.apache.tajo.ws.rs.JerseyResourceDelegateUtil;
import org.apache.tajo.ws.rs.ResourcesUtil;
import org.apache.tajo.ws.rs.requests.NewDatabaseRequest;
import org.apache.tajo.ws.rs.responses.DatabaseInfoResponse;

/**
 * Deals with Database Management
 */
@Path("/databases")
public class DatabasesResource {
  
  private static final Log LOG = LogFactory.getLog(DatabasesResource.class);
  
  @Context
  UriInfo uriInfo;
  
  @Context
  Application application;
  
  JerseyResourceDelegateContext context;
  
  private static final String databasesKeyName = "databases";
  private static final String newDatabaseRequestKeyName = "NewDatabaseKey";
  private static final String databaseNameKeyName = "databaseName";
  
  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
    context.put(uriInfoKey, uriInfo);
  }
  
  /**
   * Get all databases from catalog server
   * 
   * @return
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllDatabases() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent retrieve all databases request.");
    }
    
    Response response = null;
    try {
      initializeContext();
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetAllDatabasesDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetAllDatabasesDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      Collection<String> databaseNames = masterContext.getCatalog().getAllDatabaseNames();
      Map<String, Collection<String>> databaseNamesMap = new HashMap<String, Collection<String>>();
      databaseNamesMap.put(databasesKeyName, databaseNames);
      return Response.ok(databaseNamesMap).build();
    }
  }
  
  /**
   * 
   * @param request
   * @return
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createNewDatabase(NewDatabaseRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a new database creation request");
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<NewDatabaseRequest> newDatabaseRequestKey =
          JerseyResourceDelegateContextKey.valueOf(newDatabaseRequestKeyName, NewDatabaseRequest.class);
      context.put(newDatabaseRequestKey, request);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new CreateNewDatabaseDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class CreateNewDatabaseDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<NewDatabaseRequest> newDatabaseRequestKey =
          JerseyResourceDelegateContextKey.valueOf(newDatabaseRequestKeyName, NewDatabaseRequest.class);
      NewDatabaseRequest request = context.get(newDatabaseRequestKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
      UriInfo uriInfo = context.get(uriInfoKey);
      
      if (request.getDatabaseName() == null || request.getDatabaseName().isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "databaseName is null or empty.");
      }
      
      boolean databaseCreated =
          masterContext.getCatalog().createDatabase(request.getDatabaseName(), 
              TajoConstants.DEFAULT_TABLESPACE_NAME);
      
      if (databaseCreated) {
        URI newDatabaseURI = uriInfo.getBaseUriBuilder()
            .path(DatabasesResource.class)
            .path(DatabasesResource.class, "getDatabase")
            .build(request.getDatabaseName());
        return Response.created(newDatabaseURI).build();
      } else {
        return ResourcesUtil.createExceptionResponse(LOG, "Failed to create a new database.");
      }
    }
  }
  
  /**
   * 
   * @param databaseName
   * @return
   */
  @GET
  @Path("/{databaseName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatabase(@PathParam("databaseName") String databaseName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a getDatabase request.");
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      context.put(databaseNameKey, databaseName);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetDatabaseDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetDatabaseDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      String databaseName = context.get(databaseNameKey);
      
      if (databaseName.isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "DatabaseName is empty string.");
      }
      
      CatalogService catalogService = masterContext.getCatalog();
      List<DatabaseProto> databasesList = catalogService.getAllDatabases();
      DatabaseProto selectedDatabase = null;
      for (DatabaseProto database: databasesList) {
        if (database.getName().equals(databaseName)) {
          selectedDatabase = database;
          break;
        }
      }
      
      if (selectedDatabase != null) {
        List<TablespaceProto> tablespacesList = catalogService.getAllTablespaces();
        TablespaceProto selectedTablespace = null;

        for (TablespaceProto tablespace: tablespacesList) {
          if (tablespace.hasId() && tablespace.getId() == selectedDatabase.getSpaceId()) {
            selectedTablespace = tablespace;
            break;
          }
        }

        if(selectedTablespace ==  null) {
          return ResourcesUtil.createExceptionResponse(LOG, "Tablespace not found.");
        }

        DatabaseInfoResponse databaseInfo = new DatabaseInfoResponse();
        databaseInfo.setId(selectedDatabase.getId());
        databaseInfo.setName(selectedDatabase.getName());
        databaseInfo.setTablespace(selectedTablespace.getUri());
        return Response.ok(databaseInfo).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }
  }
  
  /**
   * 
   * @param databaseName
   * @return
   */
  @DELETE
  @Path("/{databaseName}")
  public Response deleteDatabase(@PathParam("databaseName") String databaseName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a delete database request.");
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      context.put(databaseNameKey, databaseName);
      
      return JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new DeleteDatabaseDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class DeleteDatabaseDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      String databaseName = context.get(databaseNameKey);
      
      if (databaseName.isEmpty()) {
        return ResourcesUtil.createBadRequestResponse(LOG, "DatabaseName is empty string.");
      }
      
      CatalogService catalogService = masterContext.getCatalog();
      
      if (!catalogService.existDatabase(databaseName)) {
        return Response.status(Status.NOT_FOUND).build();
      }
      
      boolean databaseDropped = catalogService.dropDatabase(databaseName);
      
      if (databaseDropped) {
        return Response.ok().build();
      } else {
        return ResourcesUtil.createExceptionResponse(LOG, "Unable to drop a database " + databaseName);
      }
    }
  }
}
