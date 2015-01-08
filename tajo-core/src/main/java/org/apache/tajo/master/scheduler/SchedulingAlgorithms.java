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

package org.apache.tajo.master.scheduler;

import java.util.Comparator;

/**
 * Utility class containing scheduling algorithms used in the scheduler.
 */

public class SchedulingAlgorithms  {
  /**
   * Compare Schedulables in order of priority and then submission time, as in
   * the default FIFO scheduler in Tajo.
   */
  public static class FifoComparator implements Comparator<QuerySchedulingInfo> {
    @Override
    public int compare(QuerySchedulingInfo q1, QuerySchedulingInfo q2) {
      int res = q1.getPriority().compareTo(q2.getPriority());
      if (res == 0) {
        res = (int) Math.signum(q1.getStartTime() - q2.getStartTime());
      }
      if (res == 0) {
        // In the rare case where jobs were submitted at the exact same time,
        // compare them by name (which will be the QueryId) to get a deterministic ordering
        res = q1.getName().compareTo(q2.getName());
      }
      return res;
    }
  }
}
