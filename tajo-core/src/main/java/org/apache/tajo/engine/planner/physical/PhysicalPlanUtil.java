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

import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.engine.planner.PhysicalPlanningException;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.PersistentStoreNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.StorageConstants;

import java.util.Stack;

public class PhysicalPlanUtil {
  public static <T extends PhysicalExec> T findExecutor(PhysicalExec plan, Class<? extends PhysicalExec> clazz)
      throws PhysicalPlanningException {
    return (T) new FindVisitor().visit(plan, new Stack<PhysicalExec>(), clazz);
  }

  private static class FindVisitor extends BasicPhysicalExecutorVisitor<Class<? extends PhysicalExec>, PhysicalExec> {
    public PhysicalExec visit(PhysicalExec exec, Stack<PhysicalExec> stack, Class<? extends PhysicalExec> target)
        throws PhysicalPlanningException {
      if (target.isAssignableFrom(exec.getClass())) {
        return exec;
      } else {
        return super.visit(exec, stack, target);
      }
    }
  }

  /**
   * Set nullChar to TableMeta according to file format
   *
   * @param meta TableMeta
   * @param nullChar A character for NULL representation
   */
  private static void setNullCharForTextSerializer(TableMeta meta, String nullChar) {
    switch (meta.getStoreType()) {
    case CSV:
      meta.putOption(StorageConstants.CSVFILE_NULL, nullChar);
      break;
    case RCFILE:
      meta.putOption(StorageConstants.RCFILE_NULL, nullChar);
      break;
    case SEQUENCEFILE:
      meta.putOption(StorageConstants.SEQUENCEFILE_NULL, nullChar);
      break;
    default: // nothing to do
    }
  }

  /**
   * Check if TableMeta contains NULL char property according to file format
   *
   * @param meta Table Meta
   * @return True if TableMeta contains NULL char property according to file format
   */
  public static boolean containsNullChar(TableMeta meta) {
    switch (meta.getStoreType()) {
    case CSV:
      return meta.containsOption(StorageConstants.CSVFILE_NULL);
    case RCFILE:
      return meta.containsOption(StorageConstants.RCFILE_NULL);
    case SEQUENCEFILE:
      return meta.containsOption(StorageConstants.SEQUENCEFILE_NULL);
    default: // nothing to do
      return false;
    }
  }

  /**
   * Set session variable null char TableMeta if necessary
   *
   * @param context QueryContext
   * @param plan StoreTableNode
   * @param meta TableMeta
   */
  public static void setNullCharIfNecessary(QueryContext context, PersistentStoreNode plan, TableMeta meta) {
    if (plan.getType() != NodeType.INSERT) {
      // table property in TableMeta is the first priority, and session is the second priority
      if (!containsNullChar(meta) && context.containsKey(SessionVars.NULL_CHAR)) {
        setNullCharForTextSerializer(meta, context.get(SessionVars.NULL_CHAR));
      }
    }
  }
}
