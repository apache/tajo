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
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.QueryId;
import org.apache.tajo.SubQueryId;
import org.apache.tajo.TajoIdProtos.SubQueryIdProto;

import java.util.Iterator;

import static org.apache.hadoop.yarn.util.StringHelper._split;

public class TajoIdUtils {
  public static final String YARN_APPLICATION_PREFIX = "application";
  public static final String YARN_CONTAINER_PREFIX = "container";
  public static final String YARN_APPLICATION_ATTEMPT_PREFIX = "appattempt";

  /** It is mainly for DDL statements which don's have any query id. */
  public static final QueryId NullQueryId =
      TajoIdUtils.createQueryId(BuilderUtils.newApplicationId(0, 0), 0);

  public static QueryId createQueryId(ApplicationId appId, int attemptId) {
    return newQueryId(appId, attemptId);
  }

  public static QueryId createQueryId(ApplicationAttemptId appAttemptId) {
    QueryId queryId = new QueryId();
    queryId.setApplicationId(appAttemptId.getApplicationId());
    queryId.setAttemptId(appAttemptId.getAttemptId());
    return queryId;
  }

  public static QueryId createQueryId(String queryId) {
    String[] split = queryId.split(QueryId.SEPARATOR);
    ApplicationId appId = BuilderUtils.newApplicationId(Long.valueOf(split[1]),
        Integer.parseInt(split[2]));
    int idInt = Integer.parseInt(split[3]);
    return newQueryId(appId, idInt);
  }

  public static SubQueryId createSubQueryId(QueryId queryId,
                                            int subQueryIdInt) {
    return newSubQueryId(queryId, subQueryIdInt);
  }

  public static QueryId newQueryId(ApplicationId appId, int id) {
    QueryId queryId = new QueryId();
    queryId.setApplicationId(appId);
    queryId.setAttemptId(id);
    return queryId;
  }

  public static SubQueryId newSubQueryId(QueryId jobId, int id) {
    SubQueryId taskId = new SubQueryId();
    taskId.setQueryId(jobId);
    taskId.setId(id);
    return taskId;
  }

  public static SubQueryId newSubQueryId(String subQueryId) {
    String [] split = subQueryId.split(QueryId.SEPARATOR);
    ApplicationId appId = BuilderUtils.newApplicationId(Long.valueOf(split[1]),
        Integer.valueOf(split[2]));
    QueryId queryId = TajoIdUtils.createQueryId(appId, Integer.valueOf(split[3]));
    return createSubQueryId(queryId, Integer.parseInt(split[4]));
  }

  public static SubQueryId newSubQueryId(SubQueryIdProto proto) {
    SubQueryId subId = new SubQueryId(proto);
    return subId;
  }

  public static ApplicationAttemptId toApplicationAttemptId(
          String applicationAttmeptIdStr) {
    //This methood from YARN.ConvertUtils
    Iterator<String> it = _split(applicationAttmeptIdStr).iterator();
    if (!it.next().equals(YARN_APPLICATION_ATTEMPT_PREFIX)) {
      throw new IllegalArgumentException("Invalid AppAttemptId prefix: "
              + applicationAttmeptIdStr);
    }
    try {
      return toApplicationAttemptId(it);
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid AppAttemptId: "
              + applicationAttmeptIdStr, n);
    }
  }

  private static ApplicationAttemptId toApplicationAttemptId(
          Iterator<String> it) throws NumberFormatException {
    //This methood from YARN.ConvertUtils
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(Long.parseLong(it.next()));
    appId.setId(Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId = Records
            .newRecord(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(Integer.parseInt(it.next()));
    return appAttemptId;
  }
}
