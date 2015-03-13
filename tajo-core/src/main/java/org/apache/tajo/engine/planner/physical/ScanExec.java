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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public abstract class ScanExec extends PhysicalExec {

  /* if this is a broadcasted table or not  */
  private boolean canBroadcast;

  public ScanExec(TaskAttemptContext context, Schema inSchema, Schema outSchema) {
    super(context, inSchema, outSchema);
  }

  public abstract String getTableName();

  public abstract String getCanonicalName();

  public abstract CatalogProtos.FragmentProto[] getFragments();

  @Override
  public void init() throws IOException {
    canBroadcast = checkIfBroadcast();

    super.init();
  }

  public boolean canBroadcast() {
    return canBroadcast;
  }

  /* check if this scan is broadcasted */
  private boolean checkIfBroadcast() {
    Enforcer enforcer = context.getEnforcer();

    if (enforcer != null && enforcer.hasEnforceProperty(TajoWorkerProtocol.EnforceProperty.EnforceType.BROADCAST)) {
      List<TajoWorkerProtocol.EnforceProperty> properties =
          enforcer.getEnforceProperties(TajoWorkerProtocol.EnforceProperty.EnforceType.BROADCAST);

      for (TajoWorkerProtocol.EnforceProperty property : properties) {
        if (getCanonicalName().equals(property.getBroadcast().getTableName())) {
          return true;
        }
      }
    }
    return false;
  }
}
