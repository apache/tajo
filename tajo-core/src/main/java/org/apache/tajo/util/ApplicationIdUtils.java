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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;

public class ApplicationIdUtils {
  public static ApplicationAttemptId createApplicationAttemptId(QueryId queryId, int attemptId) {
    return BuilderUtils.newApplicationAttemptId(queryIdToAppId(queryId), attemptId);
  }

  public static ApplicationAttemptId createApplicationAttemptId(QueryId queryId) {
    return BuilderUtils.newApplicationAttemptId(queryIdToAppId(queryId), 1);
  }

  public static ApplicationId queryIdToAppId(QueryId queryId) {
    return BuilderUtils.newApplicationId(Long.parseLong(queryId.getId()), queryId.getSeq());
  }

  public static QueryId appIdToQueryId(YarnProtos.ApplicationIdProto appId) {
    return QueryIdFactory.newQueryId(appId.getClusterTimestamp(), appId.getId());
  }
}
