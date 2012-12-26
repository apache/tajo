package tajo.client;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import tajo.QueryId;
import tajo.TajoProtos.QueryState;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableDesc;
import tajo.catalog.TableMeta;
import tajo.client.ClientProtocol.*;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.query.ResultSetImpl;
import tajo.rpc.ProtoBlockingRpcClient;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author Hyunsik Choi
 */
public class TajoClient {
  private final Log LOG = LogFactory.getLog(TajoClient.class);

  private final TajoConf conf;
  private ProtoBlockingRpcClient client;
  private ClientProtocolService.BlockingInterface service;

  public TajoClient(TajoConf conf) throws IOException {
    this.conf = conf;
    String masterAddr = this.conf.getVar(ConfVars.CLIENT_SERVICE_ADDRESS);
    InetSocketAddress addr = NetUtils.createSocketAddr(masterAddr);
    connect(addr);
  }

  public TajoClient(InetSocketAddress addr) throws IOException {
    this.conf = new TajoConf();
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);
    connect(addr);
  }

  public TajoClient(String hostname, int port) throws IOException {
    this.conf = new TajoConf();
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);
    connect(NetUtils.createSocketAddr(hostname, port));
  }

  private void connect(InetSocketAddress addr) throws IOException {
    try {
      client = new ProtoBlockingRpcClient(ClientProtocol.class, addr);
      service = client.getStub();
    } catch (Exception e) {
      throw new IOException(e);
    }

    LOG.info("Connected to tajo cluster (" +
        tajo.util.NetUtils.getIpPortString(addr) + ")");
  }

  public void close() {
    client.close();
  }

  public boolean isConnected() {
    return client.isConnected();
  }

  private String errorMessage;

  public QueryId executeQuery(String tql) throws ServiceException {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setQuery(tql);
    SubmitQueryRespose response = service.submitQuery(null, builder.build());
    if (response.hasErrorMessage()) {
      errorMessage = response.getErrorMessage();
      return null;
    }
    return new QueryId(response.getQueryId());
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public ResultSet executeQueryAndWait(String tql)
      throws ServiceException, IOException {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setQuery(tql);
    SubmitQueryRespose response = service.submitQuery(null, builder.build());

    return getQueryResultAndWait(new QueryId(response.getQueryId()));
  }

  public QueryStatus getQueryStatus(QueryId queryId) throws ServiceException {
    GetQueryStatusRequest.Builder builder
        = GetQueryStatusRequest.newBuilder();
    builder.setQueryId(queryId.getProto());

    GetQueryStatusResponse res = service.getQueryStatus(null,
        builder.build());

    return new QueryStatus(res);
  }

  private static boolean isQueryRunnning(QueryState state) {
    return state == QueryState.QUERY_NEW ||
        state == QueryState.QUERY_INIT ||
        state == QueryState.QUERY_RUNNING;
  }

  public ResultSet getQueryResult(QueryId queryId)
      throws ServiceException, IOException {
    TableDesc tableDesc = getQueryResultDesc(queryId);
    ResultSet resultSet = new ResultSetImpl(conf, tableDesc.getPath());
    return resultSet;
  }

  public ResultSet getQueryResultAndWait(QueryId queryId)
      throws ServiceException, IOException {
    QueryStatus status = getQueryStatus(queryId);

    while(status != null && isQueryRunnning(status.getState())) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      status = getQueryStatus(queryId);
    }

    if (status.getState() == QueryState.QUERY_SUCCEEDED) {
      return getQueryResult(queryId);
    } else {
      LOG.error(status.getErrorMessage());

      return null;
    }
  }

  public TableDesc getQueryResultDesc(QueryId queryId) throws ServiceException {
    GetQueryResultRequest.Builder builder = GetQueryResultRequest.newBuilder();
    builder.setQueryId(queryId.getProto());
    GetQueryResultResponse response = service.getQueryResult(null,
        builder.build());

    TableDesc tableDesc = TCatUtil.newTableDesc(response.getTableDesc());
    return tableDesc;
  }

  public boolean updateQuery(String tql) throws ServiceException {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setQuery(tql);

    if (service.updateQuery(null, builder.build()).getResultCode()
        == ResultCode.OK) {
      return true;
    } else {
      return false;
    }
  }

  public boolean existTable(String name) throws ServiceException {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    return service.existTable(null, builder.build()).getValue();
  }

  public TableDesc attachTable(String name, String path)
      throws ServiceException {
    AttachTableRequest.Builder builder = AttachTableRequest.newBuilder();
    builder.setName(name);
    builder.setPath(path);
    TableResponse res = null;
    res = service.attachTable(null, builder.build());
    return TCatUtil.newTableDesc(res.getTableDesc());
  }

  public boolean detachTable(String name) throws ServiceException {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    return service.detachTable(null, builder.build()).getValue();
  }

  public TableDesc createTable(String name, Path path, TableMeta meta)
      throws ServiceException {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setName(name);
    builder.setPath(path.toString());
    builder.setMeta(meta.getProto());
    TableResponse res = service.createTable(null, builder.build());
    return TCatUtil.newTableDesc(res.getTableDesc());
  }

  public boolean dropTable(String name) throws ServiceException {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    return service.dropTable(null, builder.build()).getValue();
  }

  public List<String> getClusterInfo() {
    return null;
  }

  public List<String> getTableList() throws ServiceException {
    GetTableListRequest.Builder builder = GetTableListRequest.newBuilder();
    GetTableListResponse res = service.getTableList(null, builder.build());
    return res.getTablesList();
  }

  public TableDesc getTableDesc(String tableName) throws ServiceException {
    GetTableDescRequest.Builder build = GetTableDescRequest.newBuilder();
    build.setTableName(tableName);
    TableResponse res = service.getTableDesc(null, build.build());
    if (res == null) {
      return null;
    } else {
      return TCatUtil.newTableDesc(res.getTableDesc());
    }
  }

  public boolean killQuery(QueryId queryId)
      throws ServiceException, IOException {

    QueryStatus status = getQueryStatus(queryId);

    try {
      /* send a kill to the TM */
      service.killQuery(null, queryId.getProto());
      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L) && (status.getState()
          != QueryState.QUERY_KILLED)) {
        try {
          Thread.sleep(1000L);
        } catch(InterruptedException ie) {
          /** interrupted, just break */
          break;
        }
        currentTimeMillis = System.currentTimeMillis();
        status = getQueryStatus(queryId);
      }
    } catch(ServiceException io) {
      LOG.debug("Error when checking for application status", io);
      return false;
    }

    return true;
  }
}