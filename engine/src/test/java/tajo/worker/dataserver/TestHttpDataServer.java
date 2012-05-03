package tajo.worker.dataserver;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Random;

import nta.engine.WorkerTestingUtil;
import nta.engine.InterDataRetriever;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.ScheduleUnitId;

import org.apache.hadoop.net.NetUtils;
import org.junit.Before;
import org.junit.Test;

import tajo.worker.dataserver.retriever.DataRetriever;
import tajo.worker.dataserver.retriever.DirectoryRetriever;

/**
 * @author Hyunsik Choi
 */
public class TestHttpDataServer {
  private String TEST_DATA = "target/test-data/TestHttpDataServer";
  
  @Before
  public void setUp() throws Exception {
    WorkerTestingUtil.buildTestDir(TEST_DATA);
  }

  @Test
  public final void testHttpDataServer() throws Exception {
    Random rnd = new Random();
    FileWriter writer = new FileWriter(TEST_DATA+"/"+"testHttp");
    String watermark = "test_"+rnd.nextInt();
    writer.write(watermark);
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
    String line = null;    
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
    QueryIdFactory.reset();
    ScheduleUnitId schid = QueryIdFactory.newScheduleUnitId(
        QueryIdFactory.newSubQueryId(
            QueryIdFactory.newQueryId()));
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId(schid);
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId(schid);
    
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
    QueryIdFactory.reset();
    ScheduleUnitId schid = QueryIdFactory.newScheduleUnitId(
        QueryIdFactory.newSubQueryId(
            QueryIdFactory.newQueryId()));
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId(schid);
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId(schid);
    
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
  
  private static void assertDataRetrival(QueryUnitId id, int port, 
      String watermark) throws IOException {
    URL url = new URL("http://127.0.0.1:"+port
        + "/?qid=" + id.toString() + "&fn=testHttp");
    BufferedReader in = new BufferedReader(new InputStreamReader(
        url.openStream()));
    String line = null;    
    boolean found = false;
    while ((line = in.readLine()) != null) {
      if (line.equals(watermark))
        found = true;
    }    
    assertTrue(found);
    in.close();    
  }
}
