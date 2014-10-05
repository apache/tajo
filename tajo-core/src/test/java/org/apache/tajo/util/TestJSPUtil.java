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

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryUnitId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.event.QueryUnitAttemptScheduleEvent;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestJSPUtil {
  @Test
  public void testSortQueryUnit() throws Exception {
    List<QueryUnit> queryUnits = new ArrayList<QueryUnit>();

    Configuration conf = new TajoConf();

    QueryUnitAttemptScheduleEvent.QueryUnitAttemptScheduleContext scheduleContext =
        new QueryUnitAttemptScheduleEvent.QueryUnitAttemptScheduleContext();

    ExecutionBlockId ebId = TajoIdUtils.createExecutionBlockId("eb_000001_00001_00001");

    for (int i = 0; i < 10; i++) {
      QueryUnitId id = new QueryUnitId(ebId, i);
      QueryUnit queryUnit = new QueryUnit(conf, scheduleContext, id, true, null);
      queryUnits.add(queryUnit);

      int launchTime = i + 1;
      int runningTime = i + 1;
      if(i < 9) {
        queryUnit.setLaunchTime(launchTime);
        queryUnit.setFinishTime(launchTime + runningTime);
      }
    }

    Collections.shuffle(queryUnits);

    QueryUnit[] queryUnitArray = queryUnits.toArray(new QueryUnit[]{});
    JSPUtil.sortQueryUnitArray(queryUnitArray, "id", "asc");
    for (int i = 0; i < 10; i++) {
      assertEquals(i, queryUnitArray[i].getId().getId());
    }

    queryUnitArray = queryUnits.toArray(new QueryUnit[]{});
    JSPUtil.sortQueryUnitArray(queryUnitArray, "id", "desc");
    for (int i = 0; i < 10; i++) {
      assertEquals(9 - i, queryUnitArray[i].getId().getId());
    }

    queryUnitArray = queryUnits.toArray(new QueryUnit[]{});
    JSPUtil.sortQueryUnitArray(queryUnitArray, "runTime", "asc");
    assertEquals(0, queryUnitArray[0].getId().getId());
    assertEquals(9, queryUnitArray[9].getId().getId());

    queryUnitArray = queryUnits.toArray(new QueryUnit[]{});
    JSPUtil.sortQueryUnitArray(queryUnitArray, "runTime", "desc");
    assertEquals(8, queryUnitArray[0].getId().getId());
    assertEquals(9, queryUnitArray[9].getId().getId());
  }

  @Test
  public void testGetPageNavigationList() {
    List<String> originList = new ArrayList<String>();

    for (int i = 0; i < 35; i++) {
      originList.add("Data" + (i + 1));
    }

    List<String> pageList = JSPUtil.getPageNavigationList(originList, 1, 10);
    assertEquals(10, pageList.size());
    assertEquals("Data1", pageList.get(0));
    assertEquals("Data10", pageList.get(9));

    pageList = JSPUtil.getPageNavigationList(originList, 2, 10);
    assertEquals(10, pageList.size());
    assertEquals("Data11", pageList.get(0));
    assertEquals("Data20", pageList.get(9));

    pageList = JSPUtil.getPageNavigationList(originList, 3, 10);
    assertEquals(10, pageList.size());
    assertEquals("Data21", pageList.get(0));
    assertEquals("Data30", pageList.get(9));

    pageList = JSPUtil.getPageNavigationList(originList, 4, 10);
    assertEquals(5, pageList.size());
    assertEquals("Data31", pageList.get(0));
    assertEquals("Data35", pageList.get(4));
  }
}
