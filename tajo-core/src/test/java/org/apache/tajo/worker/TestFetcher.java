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

package org.apache.tajo.worker;

import org.apache.hadoop.fs.*;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

import static org.junit.Assert.*;

public class TestFetcher {
  private String TEST_DATA = "target/test-data/TestFetcher";
  private String INPUT_DIR = TEST_DATA+"/in/";
  private String OUTPUT_DIR = TEST_DATA+"/out/";
  private TajoConf conf = new TajoConf();
  private TajoPullServerService pullServerService;
  private ClientSocketChannelFactory channelFactory;
  private Timer timer;

  @Before
  public void setUp() throws Exception {
    CommonTestingUtil.getTestDir(TEST_DATA);
    CommonTestingUtil.getTestDir(INPUT_DIR);
    CommonTestingUtil.getTestDir(OUTPUT_DIR);
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, INPUT_DIR);
    conf.setIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_READ_TIMEOUT, 1);
    conf.setIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE, 127);

    pullServerService = new TajoPullServerService();
    pullServerService.init(conf);
    pullServerService.start();

    channelFactory = RpcChannelFactory.createClientChannelFactory("Fetcher", 1);
    timer = new HashedWheelTimer();
  }

  @After
  public void tearDown(){
    pullServerService.stop();
    channelFactory.releaseExternalResources();
    timer.stop();
  }

  @Test
  public void testGet() throws IOException {
    Random rnd = new Random();
    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String partId = "1";

    int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
    String dataPath = conf.getVar(ConfVars.WORKER_TEMPORAL_DIR) +
       queryId.toString() + "/output/" + sid + "/hash-shuffle/" + partParentId + "/" + partId;

    String params = String.format("qid=%s&sid=%s&p=%s&type=%s", queryId, sid, partId, "h");

    Path inputPath = new Path(dataPath);
    FSDataOutputStream stream = FileSystem.getLocal(conf).create(inputPath, true);
    for (int i = 0; i < 100; i++) {
      String data = ""+rnd.nextInt();
      stream.write(data.getBytes());
    }
    stream.flush();
    stream.close();

    URI uri = URI.create("http://127.0.0.1:" + pullServerService.getPort() + "/?" + params);
    final Fetcher fetcher = new Fetcher(conf, uri, new File(OUTPUT_DIR + "data"), channelFactory, timer);
    assertNotNull(fetcher.get());

    FileSystem fs = FileSystem.getLocal(new TajoConf());
    FileStatus inStatus = fs.getFileStatus(inputPath);
    FileStatus outStatus = fs.getFileStatus(new Path(OUTPUT_DIR, "data"));

    assertEquals(inStatus.getLen(), outStatus.getLen());
    assertEquals(TajoProtos.FetcherState.FETCH_FINISHED, fetcher.getState());
  }

  @Test
  public void testAdjustFetchProcess() {
    assertEquals(0.0f, Task.adjustFetchProcess(0, 0), 0);
    assertEquals(0.0f, Task.adjustFetchProcess(10, 10), 0);
    assertEquals(0.05f, Task.adjustFetchProcess(10, 9), 0);
    assertEquals(0.1f, Task.adjustFetchProcess(10, 8), 0);
    assertEquals(0.25f, Task.adjustFetchProcess(10, 5), 0);
    assertEquals(0.45f, Task.adjustFetchProcess(10, 1), 0);
    assertEquals(0.5f, Task.adjustFetchProcess(10, 0), 0);
  }

  @Test
  public void testStatus() throws Exception {
    Random rnd = new Random();
    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    String dataPath = INPUT_DIR + queryId.toString() + "/output"+ "/" + sid + "/" +ta + "/output/" + partId;
    String params = String.format("qid=%s&sid=%s&p=%s&type=%s&ta=%s", queryId, sid, partId, "h", ta);

    FSDataOutputStream stream =  FileSystem.getLocal(conf).create(new Path(dataPath), true);
    for (int i = 0; i < 100; i++) {
      String data = ""+rnd.nextInt();
      stream.write(data.getBytes());
    }
    stream.flush();
    stream.close();

    URI uri = URI.create("http://127.0.0.1:" + pullServerService.getPort() + "/?" + params);
    final Fetcher fetcher = new Fetcher(conf, uri, new File(OUTPUT_DIR + "data"), channelFactory, timer);
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    fetcher.get();
    assertEquals(TajoProtos.FetcherState.FETCH_FINISHED, fetcher.getState());
  }

  @Test
  public void testNoContentFetch() throws Exception {

    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    String dataPath = INPUT_DIR + queryId.toString() + "/output"+ "/" + sid + "/" +ta + "/output/" + partId;
    String params = String.format("qid=%s&sid=%s&p=%s&type=%s&ta=%s", queryId, sid, partId, "h", ta);

    Path inputPath = new Path(dataPath);
    FileSystem fs = FileSystem.getLocal(conf);
    if(fs.exists(inputPath)){
      fs.delete(new Path(dataPath), true);
    }

    FSDataOutputStream stream =  FileSystem.getLocal(conf).create(new Path(dataPath).getParent(), true);
    stream.close();

    URI uri = URI.create("http://127.0.0.1:" + pullServerService.getPort() + "/?" + params);
    final Fetcher fetcher = new Fetcher(conf, uri, new File(OUTPUT_DIR + "data"), channelFactory, timer);
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    fetcher.get();
    assertEquals(TajoProtos.FetcherState.FETCH_FINISHED, fetcher.getState());
  }

  @Test
  public void testFailureStatus() throws Exception {
    Random rnd = new Random();

    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    String dataPath = INPUT_DIR + queryId.toString() + "/output"+ "/" + sid + "/" +ta + "/output/" + partId;

    //TajoPullServerService will be throws BAD_REQUEST by Unknown shuffle type
    String shuffleType = "x";
    String params = String.format("qid=%s&sid=%s&p=%s&type=%s&ta=%s", queryId, sid, partId, shuffleType, ta);

    FSDataOutputStream stream =  FileSystem.getLocal(conf).create(new Path(dataPath), true);

    for (int i = 0; i < 100; i++) {
      String data = params + rnd.nextInt();
      stream.write(data.getBytes());
    }
    stream.flush();
    stream.close();

    URI uri = URI.create("http://127.0.0.1:" + pullServerService.getPort() + "/?" + params);
    final Fetcher fetcher = new Fetcher(conf, uri, new File(OUTPUT_DIR + "data"), channelFactory, timer);
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    fetcher.get();
    assertEquals(TajoProtos.FetcherState.FETCH_FAILED, fetcher.getState());
  }

  @Test
  public void testServerFailure() throws Exception {
    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    String dataPath = INPUT_DIR + queryId.toString() + "/output"+ "/" + sid + "/" +ta + "/output/" + partId;
    String params = String.format("qid=%s&sid=%s&p=%s&type=%s&ta=%s", queryId, sid, partId, "h", ta);

    URI uri = URI.create("http://127.0.0.1:" + pullServerService.getPort() + "/?" + params);
    final Fetcher fetcher = new Fetcher(conf, uri, new File(OUTPUT_DIR + "data"), channelFactory, timer);
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    pullServerService.stop();

    boolean failure = false;
    try{
      fetcher.get();
    } catch (Throwable e){
      failure = true;
    }
    assertTrue(failure);
    assertEquals(TajoProtos.FetcherState.FETCH_FAILED, fetcher.getState());
  }
}
