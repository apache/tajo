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

package org.apache.tajo.worker.dataserver;

import org.apache.hadoop.net.NetUtils;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TaskId;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.InterDataRetriever;
import org.apache.tajo.worker.dataserver.retriever.DataRetriever;
import org.apache.tajo.worker.dataserver.retriever.DirectoryRetriever;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestHttpDataServer {
  private String TEST_DATA = "target/test-data/TestHttpDataServer";
  
  @Before
  public void setUp() throws Exception {
    CommonTestingUtil.getTestDir(TEST_DATA);
  }

  @Test
  public final void testHttpDataServer() throws Exception {
    Random rnd = new Random();
    FileWriter writer = new FileWriter(TEST_DATA+"/"+"testHttp");
    String watermark = "test_"+rnd.nextInt();
    writer.write(watermark+"\n");
    writer.flush();
    writer.close();

    DataRetriever ret = new DirectoryRetriever(TEST_DATA);
    HttpDataServer server = new HttpDataServer(
        NetUtils.createSocketAddr("127.0.0.1:0"), ret);
    server.start();

    InetSocketAddress addr = server.getBindAddress();
    URL url = new URL("http://127.0.0.1:"+addr.getPort() 
        + "/testHttp");
    BufferedReader in = new BufferedReader(new InputStreamReader(
        url.openStream()));
    String line;
    boolean found = false;
    while ((line = in.readLine()) != null) {
      if (line.equals(watermark))
        found = true;
    }    
    assertTrue(found);
    in.close();    
    server.stop();
  }
  
  @Test
  public final void testInterDataRetriver() throws Exception {
    MasterPlan plan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);
    ExecutionBlockId schid = plan.newExecutionBlockId();
    TaskId qid1 = QueryIdFactory.newTaskId(schid);
    TaskId qid2 = QueryIdFactory.newTaskId(schid);
    
    File qid1Dir = new File(TEST_DATA + "/" + qid1.toString() + "/out");
    qid1Dir.mkdirs();
    File qid2Dir = new File(TEST_DATA + "/" + qid2.toString() + "/out");
    qid2Dir.mkdirs();
    
    Random rnd = new Random();
    FileWriter writer = new FileWriter(qid1Dir+"/"+"testHttp");
    String watermark1 = "test_"+rnd.nextInt();
    writer.write(watermark1);
    writer.flush();
    writer.close();
        
    writer = new FileWriter(qid2Dir+"/"+"testHttp");
    String watermark2 = "test_"+rnd.nextInt();
    writer.write(watermark2);
    writer.flush();
    writer.close();
    
    InterDataRetriever ret = new InterDataRetriever();
    HttpDataServer server = new HttpDataServer(
        NetUtils.createSocketAddr("127.0.0.1:0"), ret);
    server.start();
    
    ret.register(qid1, qid1Dir.getPath());
    ret.register(qid2, qid2Dir.getPath());    
    
    InetSocketAddress addr = server.getBindAddress();
    
    assertDataRetrival(qid1, addr.getPort(), watermark1);
    assertDataRetrival(qid2, addr.getPort(), watermark2);
    
    server.stop();
  }
  
  @Test(expected = FileNotFoundException.class)
  public final void testNoSuchFile() throws Exception {
    MasterPlan plan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);
    ExecutionBlockId schid = plan.newExecutionBlockId();
    TaskId qid1 = QueryIdFactory.newTaskId(schid);
    TaskId qid2 = QueryIdFactory.newTaskId(schid);
    
    File qid1Dir = new File(TEST_DATA + "/" + qid1.toString() + "/out");
    qid1Dir.mkdirs();
    File qid2Dir = new File(TEST_DATA + "/" + qid2.toString() + "/out");
    qid2Dir.mkdirs();
    
    Random rnd = new Random();
    FileWriter writer = new FileWriter(qid1Dir+"/"+"testHttp");
    String watermark1 = "test_"+rnd.nextInt();
    writer.write(watermark1);
    writer.flush();
    writer.close();
        
    writer = new FileWriter(qid2Dir+"/"+"testHttp");
    String watermark2 = "test_"+rnd.nextInt();
    writer.write(watermark2);
    writer.flush();
    writer.close();
    
    InterDataRetriever ret = new InterDataRetriever();
    HttpDataServer server = new HttpDataServer(
        NetUtils.createSocketAddr("127.0.0.1:0"), ret);
    server.start();
    
    ret.register(qid1, qid1Dir.getPath());
    InetSocketAddress addr = server.getBindAddress();
    assertDataRetrival(qid1, addr.getPort(), watermark1);
    ret.unregister(qid1);
    assertDataRetrival(qid1, addr.getPort(), watermark1);
  }
  
  private static void assertDataRetrival(TaskId id, int port,
      String watermark) throws IOException {
    URL url = new URL("http://127.0.0.1:"+port
        + "/?qid=" + id.toString() + "&fn=testHttp");
    BufferedReader in = new BufferedReader(new InputStreamReader(
        url.openStream()));
    String line;
    boolean found = false;
    while ((line = in.readLine()) != null) {
      if (line.equals(watermark))
        found = true;
    }    
    assertTrue(found);
    in.close();    
  }
}
