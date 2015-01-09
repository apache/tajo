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
import org.apache.tajo.TaskId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.event.TaskAttemptToSchedulerEvent;
import org.apache.tajo.querymaster.Task;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestJSPUtil {
  @Test
  public void testSortTask() throws Exception {
    List<Task> tasks = new ArrayList<Task>();

    Configuration conf = new TajoConf();

    TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext scheduleContext =
        new TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext();

    ExecutionBlockId ebId = TajoIdUtils.createExecutionBlockId("eb_000001_00001_00001");

    for (int i = 0; i < 10; i++) {
      TaskId id = new TaskId(ebId, i);
      Task task = new Task(conf, scheduleContext, id, true, null);
      tasks.add(task);

      int launchTime = i + 1;
      int runningTime = i + 1;
      if(i < 9) {
        task.setLaunchTime(launchTime);
        task.setFinishTime(launchTime + runningTime);
      }
    }

    Collections.shuffle(tasks);

    Task[] taskArray = tasks.toArray(new Task[]{});
    JSPUtil.sortTaskArray(taskArray, "id", "asc");
    for (int i = 0; i < 10; i++) {
      assertEquals(i, taskArray[i].getId().getId());
    }

    taskArray = tasks.toArray(new Task[]{});
    JSPUtil.sortTaskArray(taskArray, "id", "desc");
    for (int i = 0; i < 10; i++) {
      assertEquals(9 - i, taskArray[i].getId().getId());
    }

    taskArray = tasks.toArray(new Task[]{});
    JSPUtil.sortTaskArray(taskArray, "runTime", "asc");
    assertEquals(0, taskArray[0].getId().getId());
    assertEquals(9, taskArray[9].getId().getId());

    taskArray = tasks.toArray(new Task[]{});
    JSPUtil.sortTaskArray(taskArray, "runTime", "desc");
    assertEquals(8, taskArray[0].getId().getId());
    assertEquals(9, taskArray[9].getId().getId());
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
