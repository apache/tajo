/*
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

package tajo.util;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import tajo.QueryId;
import tajo.SubQueryId;
import tajo.engine.TCommonProtos.SubQueryIdProto;
import tajo.impl.pb.SubQueryIdPBImpl;

/**
 * @author Hyunsik Choi
 */
public class TajoIdUtils {
  public static QueryId createQueryId(long clusterTimestamp, int idInt) {
    ApplicationId appId = BuilderUtils.newApplicationId(clusterTimestamp, idInt);
    return newQueryId(appId, idInt);
  }

  public static QueryId createQueryId(String queryId) {
    String[] split = queryId.split(QueryId.SEPARATOR);
    ApplicationId appId = BuilderUtils.newApplicationId(Long.valueOf(split[1]),
        Integer.parseInt(split[2]));
    int idInt = Integer.parseInt(split[2]);
    return newQueryId(appId, idInt);
  }

  public static SubQueryId createSubQueryId(QueryId queryId,
                                            int subQueryIdInt) {
    return newSubQueryId(queryId, subQueryIdInt);
  }

  public static QueryId newQueryId(ApplicationId appId, int id) {
    QueryId queryId = Records.newRecord(QueryId.class);
    queryId.setAppId(appId);
    queryId.setId(id);
    return queryId;
  }

  public static SubQueryId newSubQueryId(QueryId jobId, int id) {
    SubQueryId taskId = Records.newRecord(SubQueryId.class);
    taskId.setQueryId(jobId);
    taskId.setId(id);
    return taskId;
  }

  public static SubQueryId newSubQueryId(String subQueryId) {
    String [] split = subQueryId.split(QueryId.SEPARATOR);
    return createSubQueryId(
        createQueryId(Long.valueOf(split[1]), Integer.parseInt(split[2])),
        Integer.parseInt(split[3]));
  }

  public static SubQueryId newSubQueryId(SubQueryIdProto proto) {
    SubQueryId subId = new SubQueryIdPBImpl(proto);
    return subId;
  }
}
