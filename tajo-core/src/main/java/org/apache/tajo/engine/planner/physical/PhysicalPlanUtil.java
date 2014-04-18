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
}
