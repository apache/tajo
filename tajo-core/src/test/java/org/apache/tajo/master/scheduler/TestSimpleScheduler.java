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

import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.rm.TajoRMContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSimpleScheduler {
  SimpleScheduler scheduler;
  TajoRMContext rmContext;
  AsyncDispatcher dispatcher;
  TajoConf conf;

  @Before
  public void setup() {
    conf = new TajoConf();
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();

    rmContext = new TajoRMContext(dispatcher);
    scheduler = new MySimpleScheduler(rmContext);
  }

  @After
  public void tearDown() {
    scheduler.stop();
    dispatcher.stop();
  }

  @Test
  public void testReserveResource() {
  }

  class MySimpleScheduler extends SimpleScheduler {

    public MySimpleScheduler(TajoRMContext rmContext) {
      super(null, rmContext);
    }

    @Override
    protected boolean startQuery(QueryId queryId, QueryCoordinatorProtocol.AllocationResourceProto allocation) {
      return true;
    }

    @Override
    protected QueryInfo getQueryInfo(QueryId queryId) {
      return new QueryInfo(queryId);
    }
  }
}
