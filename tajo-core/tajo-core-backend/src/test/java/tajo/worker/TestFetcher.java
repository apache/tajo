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

package tajo.worker;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.junit.Before;
import org.junit.Test;
import tajo.conf.TajoConf;
import tajo.util.CommonTestingUtil;
import tajo.worker.dataserver.HttpDataServer;
import tajo.worker.dataserver.retriever.DataRetriever;
import tajo.worker.dataserver.retriever.DirectoryRetriever;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestFetcher {
  private String TEST_DATA = "target/test-data/TestFetcher";
  private String INPUT_DIR = TEST_DATA+"/in/";
  private String OUTPUT_DIR = TEST_DATA+"/out/";

  @Before
  public void setUp() throws Exception {
    CommonTestingUtil.getTestDir(TEST_DATA);
    CommonTestingUtil.getTestDir(INPUT_DIR);
    CommonTestingUtil.getTestDir(OUTPUT_DIR);
  }

  @Test
  public void testGet() throws IOException {
    Random rnd = new Random();
    FileWriter writer = new FileWriter(INPUT_DIR + "data");
    String data;
    for (int i = 0; i < 100; i++) {
      data = ""+rnd.nextInt();
      writer.write(data);
    }
    writer.flush();
    writer.close();

    DataRetriever ret = new DirectoryRetriever(INPUT_DIR);
    HttpDataServer server = new HttpDataServer(
        NetUtils.createSocketAddr("127.0.0.1:0"), ret);
    server.start();
    InetSocketAddress addr = server.getBindAddress();
    
    URI uri = URI.create("http://127.0.0.1:"+addr.getPort() + "/data");
    Fetcher fetcher = new Fetcher(uri, new File(OUTPUT_DIR + "data"));
    fetcher.get();
    server.stop();
    
    FileSystem fs = FileSystem.getLocal(new TajoConf());
    FileStatus inStatus = fs.getFileStatus(new Path(INPUT_DIR, "data"));
    FileStatus outStatus = fs.getFileStatus(new Path(OUTPUT_DIR, "data"));
    assertEquals(inStatus.getLen(), outStatus.getLen());
  }
}
