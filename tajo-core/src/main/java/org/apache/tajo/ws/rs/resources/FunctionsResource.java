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
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.ws.rs.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Path("/functions")
public class FunctionsResource {
  
  private static final Log LOG = LogFactory.getLog(TablesResource.class);

  @Context
  UriInfo uriInfo;
  
  @Context
  Application application;

  JerseyResourceDelegateContext context;
  
  private static final String databaseNameKeyName = "databaseName";
  
  private void initializeContext() {
    context = new JerseyResourceDelegateContext();
    JerseyResourceDelegateContextKey<UriInfo> uriInfoKey =
        JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.UriInfoKey, UriInfo.class);
    context.put(uriInfoKey, uriInfo);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllFunctions() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client sent a retrieve all functions request.");
    }
    
    Response response = null;
    try {
      initializeContext();
      response = JerseyResourceDelegateUtil.runJerseyResourceDelegate(
          new GetAllFunctionsDelegate(),
          application,
          context,
          LOG);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      
      response = ResourcesUtil.createExceptionResponse(null, e.getMessage());
    }
    return response;
  }
  
  private static class GetAllFunctionsDelegate implements JerseyResourceDelegate {

    @Override
    public Response run(JerseyResourceDelegateContext context) {
      JerseyResourceDelegateContextKey<MasterContext> masterContextKey =
          JerseyResourceDelegateContextKey.valueOf(JerseyResourceDelegateUtil.MasterContextKey, MasterContext.class);
      MasterContext masterContext = context.get(masterContextKey);
      
      Collection<FunctionDesc> functionDescriptors = masterContext.getCatalog().getFunctions();
      if (functionDescriptors.size() > 0) {
        List<FunctionSignature> functionSignature =
                new ArrayList<>(functionDescriptors.size());
        functionSignature.addAll(functionDescriptors.stream().map(FunctionDesc::getSignature).collect(Collectors.toList()));
        
        return Response.ok(functionSignature).build();
      } else {
        return Response.status(Status.NOT_FOUND).build();
      }
    }
  }
}
