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

package org.apache.tajo.ws.rs;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.ws.rs.ResourcesUtil;

public class JerseyResourceDelegateUtil {
  
  public static final String ClientApplicationKey = "ClientApplication";
  public static final String MasterContextKey = "MasterContextKey";
  public static final String UriInfoKey = "UriInfoKey";

  public static Response runJerseyResourceDelegate(JerseyResourceDelegate delegate, 
      Application application,
      JerseyResourceDelegateContext context,
      Log log) {
    Application localApp = ResourceConfigUtil.getJAXRSApplication(application);
    
    if ((localApp != null) && (localApp instanceof ClientApplication)) {
      ClientApplication clientApplication = (ClientApplication) localApp;
      JerseyResourceDelegateContextKey<ClientApplication> clientApplicationKey =
          JerseyResourceDelegateContextKey.valueOf(ClientApplicationKey, ClientApplication.class);
      context.put(clientApplicationKey, clientApplication);
      
      MasterContext masterContext = clientApplication.getMasterContext();
      
      if (masterContext != null) {
        JerseyResourceDelegateContextKey<MasterContext> key = 
            JerseyResourceDelegateContextKey.valueOf(MasterContextKey, MasterContext.class);
        context.put(key, masterContext);
      
        return delegate.run(context);
      } else {
        return ResourcesUtil.createExceptionResponse(log, "MasterContext is null.");
      }
    } else {
      return ResourcesUtil.createExceptionResponse(log, "Invalid injection on SessionsResource.");
    }
  }
}
