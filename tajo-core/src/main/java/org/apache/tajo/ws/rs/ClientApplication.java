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

import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.ws.rs.resources.ClusterResource;
import org.apache.tajo.ws.rs.resources.DatabasesResource;
import org.apache.tajo.ws.rs.resources.FunctionsResource;
import org.apache.tajo.ws.rs.resources.QueryResource;
import org.apache.tajo.ws.rs.resources.SessionsResource;
import org.apache.tajo.ws.rs.resources.TablesResource;

import javax.ws.rs.core.Application;

import java.util.HashSet;
import java.util.Set;

/**
 * It loads client classes for Tajo protocol.
 */
public class ClientApplication extends Application {

  private final MasterContext masterContext;

  public ClientApplication(MasterContext masterContext) {
    this.masterContext = masterContext;
  }

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> classes = new HashSet<Class<?>>();
    
    classes.add(SessionsResource.class);
    classes.add(DatabasesResource.class);
    classes.add(TablesResource.class);
    classes.add(FunctionsResource.class);
    classes.add(ClusterResource.class);
    classes.add(QueryResource.class);
    
    return classes;
  }

  public MasterContext getMasterContext() {
    return masterContext;
  }
}
