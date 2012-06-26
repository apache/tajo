package tajo.client;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;

import nta.engine.NtaTestingUtility;
import nta.engine.WorkerTestingUtil;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import tajo.client.PeriodicQueryDaemon.QueryInfo;
import tajo.client.PeriodicQueryProtos.QueryStatusProto;

public class TestPeriodicQuery {

  private static NtaTestingUtility utility;
  private static Configuration conf;
  private static PeriodicQueryDaemon daemon;
  private static PeriodicQueryClient client;
  private static TajoClient tajoClient;
  private final static String baseDir = "target/periodicquery/test/test";
  
  private final String[] querySet = {
      "select * from table1",
      "select deptname from table1",
      "select score from table1",
      "select score from table1 where score = 55",
      "select score from table1 where score > 40",
      "select deptname, score from table1",
      "select sum(score)"
  };
  private final String[] contents = { "1" , "2" , "3" , "4" , "5" , "6" , "7"};
  private final long[] period = {
      5000,20000,30000,40000,50000,60000,70000
  };
  
  @BeforeClass
  public static void setup() throws Exception {
    utility = new NtaTestingUtility();
    utility.startMiniCluster(1);
    conf = utility.getConfiguration();
    tajoClient = new TajoClient(conf);
    daemon = new PeriodicQueryDaemon(tajoClient , "target/test/");
    final String tableName = "table1";
    WorkerTestingUtil.writeTmpTable(conf, "/tajo/data", tableName, true);
    tajoClient.attachTable("table1", "/tajo/data/table1");
    client = new PeriodicQueryClient();
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    daemon.shutdown();
//    daemon.removeFiles();
    utility.shutdownMiniCluster();
  }
  @Test
  public void test() throws Exception {
    for(int i = 0 ; i < querySet.length ; i ++) {
      client.registerNewPeriodicQuery(querySet[i], contents[i], period[i]);
    }
    
    HashMap<String, QueryInfo> queryMap = daemon.getAllPeriodicQueries();
    List<QueryStatusProto> queryList = client.getQueryList();
    for(int i = 0 ; i < querySet.length ; i ++) {
      if(!queryMap.containsKey(querySet[i])){
        assertTrue(false);
      }
      assertTrue(queryMap.get(querySet[i]).getPeriod()==period[i]);
    }
    
    for(QueryStatusProto proto : queryList) {
      if(!queryMap.containsKey(proto.getQuery())) {
        assertTrue(false);
      }
    }
    assertEquals(queryList.size() , queryMap.size());
    
    String path = daemon.getPath() + "/querylist";
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(path)));
    
    String str;
    int i = 0;
    while( (str = reader.readLine()) != null ) {
      String[] strArr = str.split(PeriodicQueryDaemon.SPLITWORD);
      assertEquals(strArr[0], querySet[i]);
      assertEquals(strArr[1] , contents[i]);
      assertEquals(Long.parseLong(strArr[2]) , period[i]);
      i ++;
    }
    reader.close();
    
  }

 
  
}
