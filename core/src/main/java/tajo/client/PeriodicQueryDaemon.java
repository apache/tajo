package tajo.client;

import tajo.client.PeriodicQueryProtos.*;
import tajo.engine.ClientServiceProtos.ExecuteQueryRespose;
import tajo.rpc.NettyRpc;
import tajo.rpc.NettyRpcServer;
import tajo.rpc.RemoteException;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class PeriodicQueryDaemon implements PeriodicQueryService{
  public static final String SPLITWORD = "::::";
  public static final String LOCALHOST = "localhost";
  public static final int PORT = 9098;
  
  private TajoClient tajoClient = null;
  private static final String DIRPATH = "timerquery";
  private String workDir = DIRPATH;
  private BufferedReader reader;
  private BufferedWriter queryWriter;
  private File queryFile;
  private HashMap<String, QueryInfo> queryMap = null;
  private HashMap<String, ScheduledExecutorService> taskMap = null;
  private NettyRpcServer server;
  private boolean running = true;
  
  public PeriodicQueryDaemon (TajoClient client) throws Exception {
    if (tajoClient == null) {
      this.tajoClient = client;
    }
    init(DIRPATH);
  }
  public PeriodicQueryDaemon (InetSocketAddress inetSocketAddress) throws Exception {
    if(tajoClient == null) {
      tajoClient = new TajoClient(inetSocketAddress);
    }
    init(DIRPATH);
  }
  public PeriodicQueryDaemon(InetSocketAddress inetSocketAddress, String baseDir) 
      throws Exception{
    if(tajoClient == null) {
      tajoClient = new TajoClient(inetSocketAddress);
    }
    this.workDir = baseDir;
    init(baseDir);
  }
  public PeriodicQueryDaemon(TajoClient client, String baseDir) 
      throws Exception {
    tajoClient = client;
    this.workDir = baseDir;
    init(baseDir);
  }
  
  private void init(String baseDir) throws Exception{

    this.queryMap = new HashMap<String, QueryInfo>();
    this.taskMap = new HashMap<String, ScheduledExecutorService>();
    
    /*writer for store new periodic queries*/
    queryFile = new File(baseDir + "/querylist");
    if(!queryFile.exists()) {
      queryFile.getParentFile().mkdirs();
      queryFile.createNewFile();
    }
    queryWriter = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(queryFile , true)));
    
    /*reading queries*/
    /*query SPLITWORD content SPLITWORD period*/
    reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(baseDir + "/querylist")));
    String query = "";
    while( (query = reader.readLine()) != null ) {
      String[] querySet = query.split(SPLITWORD);
      if(querySet.length != 3) {
        throw new Exception("Parsing Exception : the length of queryset is must 3 , but " 
      + querySet.length);
      }
      queryMap.put(querySet[0],
          new QueryInfo(querySet[0], querySet[1], Long.parseLong(querySet[2])));
    }
    reader.close();
    
    InetSocketAddress address = new InetSocketAddress(LOCALHOST , PORT);
    server =
        NettyRpc
            .getProtoParamRpcServer(this,
                PeriodicQueryService.class, address);
    this.server.start();
    
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook())); 
  }
  
  private void startAll() throws Exception {
    //query = query:millisecond
    Set<String> queries = queryMap.keySet();
    for( String query : queries) {
      long period = queryMap.get(query).getPeriod();
      QueryTask task = new QueryTask(query);
      final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
      exec.scheduleAtFixedRate(task, 0, period, TimeUnit.MILLISECONDS );
      this.taskMap.put(query, exec);
    }
  }
  
  
  private boolean startQuery(String query) throws Exception {
    if(queryMap.containsKey(query)) {
      if(!this.taskMap.containsKey(query)){
        final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        
        long period = queryMap.get(query).getPeriod();
        QueryTask task = new QueryTask(query);
        exec.scheduleAtFixedRate(task, 0, period , TimeUnit.MILLISECONDS);
        this.taskMap.put(query, exec);
        return true;
      } 
    } else {
    }
    return false;
  }
  
  private boolean stopQuery(String query) throws Exception {
    if(queryMap.containsKey(query)) {
      if(taskMap.containsKey(query)) {
        ScheduledExecutorService exec = taskMap.remove(query);
        exec.shutdown();
        return true;
      }
    } else {
    }
    return false;
  }
  
 /**
  * 
 * @throws IOException 
  * */
  private boolean addNewPeriodicQuery (String query, 
      String content, long period) throws IOException {
    if(queryMap.containsKey(query)) {
      return false;
    }
   synchronized(queryWriter){
      queryWriter.append(query + SPLITWORD + content + SPLITWORD +  period +"\n");
      queryWriter.flush();
    }
    queryMap.put(query, new QueryInfo(query,content,period));
    return true;
  }
  
  private boolean addAndStartNewPeriodicQuery (String query, 
      String content, long period) throws IOException {
    if(addNewPeriodicQuery(query, content, period)) {
      QueryTask task = new QueryTask(query);
      final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
      exec.scheduleAtFixedRate(task, 0, period , TimeUnit.MILLISECONDS);
      taskMap.put(query, exec);
      return true;
    } else {
      return false;
    }
  }
  
  public HashMap<String, QueryInfo> getAllPeriodicQueries () {
    return this.queryMap;
  }
  
  public void shutdown() throws IOException {
    this.server.shutdown();
    
    /*stopping timer*/
    queryMap.clear();
    Set<String> taskSet = taskMap.keySet();
    for(String query : taskSet) {
      ScheduledExecutorService exec = taskMap.get(query);
      if (exec != null) {
        exec.shutdown();
      }
    }
    taskMap.clear();
    /*data flushing*/
  
    queryWriter.flush();
    queryWriter.close();
    System.out.println("Daemon is closed");
  }
  
  public String getPath() {
    return this.workDir;
  }
  
  
  
  private class QueryTask implements Runnable {
    private String query;
    public QueryTask(String query) {
      this.query = query;
    }
    @Override
    public void run() {
      long startTime;
      long endTime;
      try {
        //TO-DO : To consider multiple queries store in the same resultpath variable
        /*path  SPLITWORD  startTime  SPLITWORD endTime*/
        if (tajoClient != null) {
          
          startTime = System.currentTimeMillis();
          tajoClient.executeQuery(query);
          endTime = System.currentTimeMillis();
          
          String filePath = workDir + "/" + query.hashCode() + ".query";
          BufferedWriter writer = new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(filePath , true)));
          
          synchronized(tajoClient){
          writer.append(tajoClient.getResultPath() + SPLITWORD + 
              startTime + SPLITWORD + endTime + "\n");
          writer.flush();
          writer.close();
          }
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public void removeFiles(){
    this.queryFile.delete();
    this.queryFile.getParentFile().delete();
  }
  
  public void run() {
    try{
      while(running) {
        Thread.sleep(1000);
      }
    }catch(InterruptedException e) {
    }
  }
  
  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      try {
        running = false;
        shutdown();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  ////////////////////////////////////RPC////////////////////////////////////////
  /////////////////////////////PeriodicQueryService////////////////////////////////
  @Override
  public QueryListResponse getQueryList(NullProto request) {
    QueryListResponse.Builder builder = QueryListResponse.newBuilder();
    Set<String> keySet = this.queryMap.keySet();

    for(String key : keySet) {
      QueryInfo info = queryMap.get(key);
      QueryStatusProto.Builder queryBuilder = QueryStatusProto.newBuilder();
      queryBuilder.setQuery(info.getQuery());
      queryBuilder.setContent(info.getContent());
      queryBuilder.setPeriod(this.queryMap.get(key).getPeriod());
      builder.addQuery(queryBuilder);
    }
    return builder.build();
  }
  @Override
  public StatusResponse registerNewPeriodicQuery(QueryStatusProto request) {
    try {
      StatusResponse.Builder builder = StatusResponse.newBuilder();
      builder.setFinished(this.addNewPeriodicQuery(
          request.getQuery(), request.getContent(), request.getPeriod()));
      return builder.build();
    } catch (Exception e) {
      throw new RemoteException(e);
    }
  }
  @Override
  public StatusResponse executePeriodicQuery(ChooseQueryRequest request) {
    try{
      StatusResponse.Builder builder = StatusResponse.newBuilder();
      builder.setFinished(this.startQuery(request.getQuery()));
      return builder.build();
    }catch(Exception e) {
      throw new RemoteException(e);
    }
  }
  @Override
  public StatusResponse regiAndexeNewPeriodicQuery(QueryStatusProto request) {
    try{
      StatusResponse.Builder builder = StatusResponse.newBuilder();
      builder.setFinished(this.addAndStartNewPeriodicQuery(
          request.getQuery(), request.getContent(), request.getPeriod()));
      return builder.build();
    }catch(Exception e) {
      throw new RemoteException(e);
    }
  }
  @Override
  public StatusResponse cancelPeriodicQuery(ChooseQueryRequest request) {
    try{
      StatusResponse.Builder builder = StatusResponse.newBuilder();
      builder.setFinished(this.stopQuery(request.getQuery()));
      return builder.build();
    }catch(Exception e) {
      throw new RemoteException(e);
    }
  }
  @Override
  public NullProto executeAllPeriodicQuery(NullProto request) {
    try{
      this.startAll();
      return NullProto.newBuilder().build();
    }catch(Exception e) {
      throw new RemoteException(e);
    }
  }
  @Override
  public NullProto cancelAllPeirodicQuery(NullProto request) {
    try{
      Set<String> taskSet = taskMap.keySet();
      for(String query : taskSet) {
        ScheduledExecutorService exec = taskMap.get(query);
        exec.shutdown();
      }
      return NullProto.newBuilder().build();
    }catch(Exception e) {
      throw new RemoteException(e);
    }
  }
  @Override
  public ExecuteQueryRespose getQueryResultPath(ChooseQueryRequest request) {
    try {
      String filePath = workDir + "/" + request.getQuery().hashCode() + ".query";
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(filePath)));
      String str;
      String path = "";
      while((str = reader.readLine()) != null) {
        path = str;
      }
      ExecuteQueryRespose.Builder builder = ExecuteQueryRespose.newBuilder();
      builder.setPath(path);
      builder.setResponseTime(-1); //dummy
      return builder.build();
    }catch (FileNotFoundException ee) { 
      ExecuteQueryRespose.Builder builder = ExecuteQueryRespose.newBuilder();
      builder.setPath("null");
      builder.setResponseTime(-1); //dummy
      return builder.build();
    } 
    catch (Exception e) {
      throw new RemoteException(e);
    }
  }
  @Override
  public QueryResultInfoResponse getQueryResultInfo(ChooseQueryRequest request) {
    QueryResultInfoResponse.Builder builder = QueryResultInfoResponse.newBuilder();
    try {
      String filePath = workDir + "/" + request.getQuery().hashCode() + ".query";
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(filePath)));
      String str;
      String path = "";
      while((str = reader.readLine()) != null) {
        path = str;
      }
      String [] info = path.split(PeriodicQueryDaemon.SPLITWORD);
      builder.setQuery(info[0]);
      builder.setStartTime(Long.parseLong(info[1]));
      builder.setEndTime(Long.parseLong(info[2]));
      return builder.build();
    }catch (FileNotFoundException ee) { 
      builder.setQuery("null");
      builder.setStartTime(-1); //dummy
      builder.setEndTime(-1);
      return builder.build();
    } 
    catch (Exception e) {
      throw new RemoteException(e);
    }
  }
  
  
  public class QueryInfo {
    private String query;
    private String content;
    private long period;
    public QueryInfo (String query, String content, long period) {
      this.query = query;
      this.content = content;
      this.period = period;
    }
    public String getQuery(){
      return this.query;
    }
    public String getContent(){
      return this.content;
    }
    public long getPeriod(){
      return this.period;
    }
  }
  
  
  public static void main (String [] args) throws Exception {
    InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost" , 9004);
    PeriodicQueryDaemon daemon = new PeriodicQueryDaemon(inetSocketAddress);
    daemon.run();
  }
 
 
}
