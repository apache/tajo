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

package org.apache.tajo.util;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;

import java.text.DecimalFormat;

public class TajoIdUtils {
  public static final DecimalFormat MASTER_ID_FORMAT = new DecimalFormat("0000000000000");

  public static ExecutionBlockId createExecutionBlockId(String idStr) {
    String[] tokens = idStr.split("_");

    return new ExecutionBlockId(new QueryId(tokens[1], Integer.parseInt(tokens[2])), Integer.parseInt(tokens[3]));
  }

  public static TaskAttemptId parseTaskAttemptId(String idStr) {
    String[] tokens = idStr.split("_");

    return new TaskAttemptId(new TaskId(
        new ExecutionBlockId(new QueryId(tokens[1], Integer.parseInt(tokens[2])), Integer.parseInt(tokens[3])),
        Integer.parseInt(tokens[4])), Integer.parseInt(tokens[5]));
  }

  public static QueryId parseQueryId(String idStr) {
    String[] tokens = idStr.split("_");
    return new QueryId(tokens[1], Integer.parseInt(tokens[2]));
  }
}
