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
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.ws.rs.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Path("/databases/{databaseName}/tables")
public class TablesResource {
  
  private static final Log LOG = LogFactory.getLog(TablesResource.class);

  @Context
  UriInfo uriInfo;
  
  @Context
  Application application;
  
  @PathParam("databaseName")
  String databaseName;
  
  JerseyResourceDelegateContext context;

  private static final String tablesKeyName = "tables";
  private static final String databaseNameKeyName = "databaseName";
  private static final String tableNameKeyName = "tableName";
  private static final String tableDescKeyName = "tableDesc";
  
  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
    context.put(uriInfoKey, uriInfo);
  }
  
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }
  
  public void setUriInfo(UriInfo uriInfo) {
    this.uriInfo = uriInfo;
  }
  
  public void setApplication(Application application) {
    this.application = application;
  }
  
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllTables() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a get all tables request.");
    }
    
    Response response = null;
    
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      context.put(databaseNameKey, databaseName);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetAllTablesDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetAllTablesDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      String databaseName = context.get(databaseNameKey);
      
      CatalogService catalogService = masterContext.getCatalog();
      
      if (!catalogService.existDatabase(databaseName)) {
        return Response.status(Status.NOT_FOUND).build();
      }

      Collection<String> tableNames = catalogService.getAllTableNames(databaseName);
      Map<String, Collection<String>> tableNamesMap = new HashMap<>();
      tableNamesMap.put(tablesKeyName, tableNames);
      return Response.ok(tableNamesMap).build();
    }
    
  }
  
  @GET
  @Path("/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTable(@PathParam("tableName") String tableName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a get table request.");
    }
    
    Response response = null;
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      context.put(databaseNameKey, databaseName);
      JerseyResourceDelegateContextKey<String> tableNameKey =
          JerseyResourceDelegateContextKey.valueOf(tableNameKeyName, String.class);
      context.put(tableNameKey, tableName);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetTableDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class GetTableDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      String databaseName = context.get(databaseNameKey).toLowerCase();
      JerseyResourceDelegateContextKey<String> tableNameKey =
          JerseyResourceDelegateContextKey.valueOf(tableNameKeyName, String.class);
      String tableName = context.get(tableNameKey).toLowerCase();
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      if (IdentifierUtil.isFQTableName(tableName)) {
        tableName = IdentifierUtil.extractSimpleName(tableName);
      }
      
      CatalogService catalogService = masterContext.getCatalog();
      if (!catalogService.existDatabase(databaseName) ||
          !catalogService.existsTable(databaseName, tableName)) {
        return Response.status(Status.NOT_FOUND).build();
      }

      TableDesc tableDesc = null;
      try {
        tableDesc = catalogService.getTableDesc(databaseName, tableName);
        return Response.ok(tableDesc).build();
      } catch (TajoException e) {
        return ResourcesUtil.createExceptionResponse(LOG, e.getMessage());
      }
    }
  }
  
  @DELETE
  @Path("/{tableName}")
  public Response deleteTable(@PathParam("tableName") String tableName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a drop table request for the table " + tableName);
    }
    
    Response response = null;
    
    try {
      initializeContext();
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      context.put(databaseNameKey, databaseName);
      JerseyResourceDelegateContextKey<String> tableNameKey =
          JerseyResourceDelegateContextKey.valueOf(tableNameKeyName, String.class);
      context.put(tableNameKey, tableName);
      
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new DeleteTableDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    
    return response;
  }
  
  private static class DeleteTableDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<String> databaseNameKey =
          JerseyResourceDelegateContextKey.valueOf(databaseNameKeyName, String.class);
      String databaseName = context.get(databaseNameKey);
      JerseyResourceDelegateContextKey<String> tableNameKey =
          JerseyResourceDelegateContextKey.valueOf(tableNameKeyName, String.class);
      String tableName = context.get(tableNameKey);
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      if (IdentifierUtil.isFQTableName(tableName)) {
        tableName = IdentifierUtil.extractSimpleName(tableName);
      }
      
      CatalogService catalogService = masterContext.getCatalog();
      if (!catalogService.existDatabase(databaseName) || 
          !catalogService.existsTable(databaseName, tableName)) {
        return Response.status(Status.NOT_FOUND).build();
      }
      
      String canonicalTableName = IdentifierUtil.getCanonicalTableName(databaseName, tableName);

      try {
        catalogService.dropTable(canonicalTableName);
        return Response.ok().build();
      } catch (TajoException e) {
        return ResourcesUtil.createExceptionResponse(LOG, e.getMessage());
      }
    }
  }
}
