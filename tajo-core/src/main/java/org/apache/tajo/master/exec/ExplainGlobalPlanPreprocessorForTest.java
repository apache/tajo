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

package org.apache.tajo.master.exec;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.MasterPlan;

import java.util.Arrays;
import java.util.List;

/**
 * Data channels of a global plan can have multiple shuffle keys, and their appearance order is basically not preserved.
 * However, to test the equivalence of global plans, the appearance order of shuffle keys must be preserved.
 * This class guarantees the consistency of the order of shuffle keys.
 */
public class ExplainGlobalPlanPreprocessorForTest {
  private static final ExplainPlanPreprocessorForTest.ColumnComparator columnComparator =
      new ExplainPlanPreprocessorForTest.ColumnComparator();

  /**
   * For all data channels, sort shuffle keys by their names.
   *
   * @param plan master plan
   */
  public void prepareTest(MasterPlan plan) {
    ExecutionBlockCursor cursor = new ExecutionBlockCursor(plan);

    for (ExecutionBlock block : cursor) {
      List<DataChannel> outgoingChannels = plan.getOutgoingChannels(block.getId());
      if (outgoingChannels != null) {
        outgoingChannels.stream().filter(channel -> channel.hasShuffleKeys()).forEach(channel -> {
          Column[] shuffleKeys = channel.getShuffleKeys();
          Arrays.sort(shuffleKeys, columnComparator);
          channel.setShuffleKeys(shuffleKeys);
        });
      }
    }
  }

}
