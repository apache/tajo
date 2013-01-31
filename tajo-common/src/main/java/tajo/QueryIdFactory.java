
/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo;

import org.apache.hadoop.yarn.util.BuilderUtils;
import tajo.util.TajoIdUtils;

import java.util.concurrent.atomic.AtomicInteger;

public class QueryIdFactory {
  private static AtomicInteger nextId = 
      new AtomicInteger(-1);
  
  public static void reset() {
    nextId.set(-1);
  }

  public synchronized static QueryId newQueryId() {
    int idInt = nextId.incrementAndGet();
    return TajoIdUtils.createQueryId(BuilderUtils.newApplicationId(
        System.currentTimeMillis(), idInt), idInt);
  }
  
  public synchronized static SubQueryId newSubQueryId(QueryId queryId) {
    return TajoIdUtils.createSubQueryId(queryId, nextId.incrementAndGet());
  }
  
  public synchronized static QueryUnitId newQueryUnitId(SubQueryId subQueryId) {
    return new QueryUnitId(subQueryId, nextId.incrementAndGet());
  }

  public synchronized static QueryUnitId newQueryUnitId(SubQueryId subQueryId, int taskId) {
    return new QueryUnitId(subQueryId, taskId);
  }

  public synchronized static QueryUnitAttemptId newQueryUnitAttemptId(
      final QueryUnitId queryUnitId, final int attemptId) {
    return new QueryUnitAttemptId(queryUnitId, attemptId);
  }
}
