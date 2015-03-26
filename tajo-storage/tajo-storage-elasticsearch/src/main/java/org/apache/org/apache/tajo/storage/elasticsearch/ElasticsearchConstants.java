package org.apache.org.apache.tajo.storage.elasticsearch;

/**
 * Created by hwjeong on 15. 3. 25..
 */
public class ElasticsearchConstants {
  public static final String FETCH_SIZE = "1000";
  public static final String CLUSTER_NAME = "elasticsearch";
  public static final String NODES = "localhost:9300";
  public static final String INDEX_TYPE = "*";
  public static final String PRIMARY_SHARD = "5";
  public static final String REPLICA_SHARD = "0";
  public static final String PING_TIMEOUT = "10s";
  public static final String CONNECT_TIMEOUT = "10s";
  public static final String TIME_SCROLL = "1s";
  public static final String TIME_ACTION = "10s";
  public static final String THREADPOOL_RECOVERY = "1";
  public static final String THREADPOOL_BULK = "1";
  public static final String THREADPOOL_REG = "3";
  public static final String NODES_DELIMITER = ",";
  public static final String HOST_DELIMITER = ":";
  public static final String GLOBAL_FIELDS_TYPE = "_type";
  public static final String GLOBAL_FIELDS_SCORE = "_score";
  public static final String GLOBAL_FIELDS_ID = "_id";
  public static final String CHARSET = "UTF-8";
  public static final int THRANSPORT_PORT = 9300;

  // with option parameter names.
  public static final String OPT_CLUSTER = "es.cluster";
  public static final String OPT_NODES = "es.nodes";
  public static final String OPT_INDEX = "es.index";
  public static final String OPT_TYPE = "es.type";
  public static final String OPT_FETCH_SIZE = "es.fetch.size";
  public static final String OPT_PRIMARY_SHARD = "es.primary.shard";
  public static final String OPT_REPLICA_SHARD = "es.replica.shard";
  public static final String OPT_PING_TIMEOUT = "es.ping.timeout";
  public static final String OPT_CONNECT_TIMEOUT = "es.connect.timeout";
  public static final String OPT_THREADPOOL_RECOVERY = "es.threadpool.recovery";
  public static final String OPT_THREADPOOL_BULK = "es.threadpool.bulk";
  public static final String OPT_THREADPOOL_REG = "es.threadpool.reg";
  public static final String OPT_TIME_SCROLL = "es.time.scroll";
  public static final String OPT_TIME_ACTION = "es.time.action";
}
