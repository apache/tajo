/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package tajo.master;

import tajo.QueryId;
import tajo.QueryIdFactory;
import tajo.SubQueryId;
import tajo.engine.MasterWorkerProtos.QueryStatus;

/**
 * @author jihoon
 */
public class GlobalEngineUtil {
  public static Query newQuery(String tql) {
    QueryId qid = QueryIdFactory.newQueryId();
    Query query = new Query(qid, tql);
    query.setStatus(QueryStatus.QUERY_INITED);
    return query;
  }

  public static SubQuery newSubQuery(QueryId qid) {
    SubQueryId subId = QueryIdFactory.newSubQueryId(qid);
    SubQuery subQuery = new SubQuery(subId);
    subQuery.setStatus(QueryStatus.QUERY_INITED);
    return subQuery;
  }
}
