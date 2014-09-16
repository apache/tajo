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

package org.apache.tajo.pullserver;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.mapred.FadvisedChunkedFile;
import org.apache.hadoop.mapred.FadvisedFileRegion;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.pullserver.listener.FileCloseListener;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.util.CharsetUtil;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class TajoPullServerService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(TajoPullServerService.class);

  public static final String SHUFFLE_MANAGE_OS_CACHE = "tajo.pullserver.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_READAHEAD_BYTES = "tajo.pullserver.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;

  private int port;
  private ChannelFactory selector;
  private final ChannelGroup accepted = new DefaultChannelGroup();
  private HttpPipelineFactory pipelineFact;
  private int sslFileBufferSize;

  private ApplicationId appId;
  private FileSystem localFS;

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile
   */
  private boolean manageOsCache;
  private int readaheadLength;
  private ReadaheadPool readaheadPool = ReadaheadPool.getInstance();


  public static final String PULLSERVER_SERVICEID = "tajo.pullserver";

  private static final Map<String,String> userRsrc =
    new ConcurrentHashMap<String,String>();
  private String userName;

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
    "tajo.pullserver.ssl.file.buffer.size";

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

  private static boolean STANDALONE = false;

  static {
    String standalone = System.getenv("TAJO_PULLSERVER_STANDALONE");
    if (!StringUtils.isEmpty(standalone)) {
      STANDALONE = standalone.equalsIgnoreCase("true");
    }
  }

  @Metrics(name="PullServerShuffleMetrics", about="PullServer output metrics", context="tajo")
  static class ShuffleMetrics implements ChannelFutureListener {
    @Metric({"OutputBytes","PullServer output in bytes"})
    MutableCounterLong shuffleOutputBytes;
    @Metric({"Failed","# of failed shuffle outputs"})
    MutableCounterInt shuffleOutputsFailed;
    @Metric({"Succeeded","# of succeeded shuffle outputs"})
    MutableCounterInt shuffleOutputsOK;
    @Metric({"Connections","# of current shuffle connections"})
    MutableGaugeInt shuffleConnections;

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        shuffleOutputsOK.incr();
      } else {
        shuffleOutputsFailed.incr();
      }
      shuffleConnections.decr();
    }
  }

  final ShuffleMetrics metrics;

  TajoPullServerService(MetricsSystem ms) {
    super("httpshuffle");
    metrics = ms.register(new ShuffleMetrics());
  }

  @SuppressWarnings("UnusedDeclaration")
  public TajoPullServerService() {
    this(DefaultMetricsSystem.instance());
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * @param port the port to be sent to the ApplciationMaster
   * @return the serialized form of the port.
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer port_dob = new DataOutputBuffer();
    port_dob.writeInt(port);
    return ByteBuffer.wrap(port_dob.getData(), 0, port_dob.getLength());
  }

  /**
   * A helper function to deserialize the metadata returned by PullServerAuxService.
   * @param meta the metadata returned by the PullServerAuxService
   * @return the port the PullServer Handler is listening on to serve shuffle data.
   */
  public static int deserializeMetaData(ByteBuffer meta) throws IOException {
    //TODO this should be returning a class not just an int
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    return in.readInt();
  }

  public void initApp(String user, ApplicationId appId, ByteBuffer secret) {
    // TODO these bytes should be versioned
    // TODO: Once SHuffle is out of NM, this can use MR APIs
    this.appId = appId;
    this.userName = user;
    userRsrc.put(appId.toString(), user);
  }

  public void stopApp(ApplicationId appId) {
    userRsrc.remove(appId.toString());
  }

  @Override
  public void init(Configuration conf) {
    try {
      manageOsCache = conf.getBoolean(SHUFFLE_MANAGE_OS_CACHE,
          DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

      readaheadLength = conf.getInt(SHUFFLE_READAHEAD_BYTES,
          DEFAULT_SHUFFLE_READAHEAD_BYTES);

      int workerNum = conf.getInt("tajo.shuffle.rpc.server.io-thread-num",
          Runtime.getRuntime().availableProcessors() * 2);

      selector = RpcChannelFactory.createServerChannelFactory("PullServerAuxService", workerNum);

      localFS = new LocalFileSystem();
      super.init(conf);

      this.getConfig().setInt(TajoConf.ConfVars.PULLSERVER_PORT.varname
          , TajoConf.ConfVars.PULLSERVER_PORT.defaultIntVal);

      LOG.info("Tajo PullServer initialized: readaheadLength=" + readaheadLength);
    } catch (Throwable t) {
      LOG.error(t);
    }
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  public synchronized void start() {
    Configuration conf = getConfig();
    ServerBootstrap bootstrap = new ServerBootstrap(selector);

    try {
      pipelineFact = new HttpPipelineFactory(conf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    bootstrap.setPipelineFactory(pipelineFact);

    port = conf.getInt(ConfVars.PULLSERVER_PORT.varname,
        ConfVars.PULLSERVER_PORT.defaultIntVal);
    Channel ch = bootstrap.bind(new InetSocketAddress(port));

    accepted.add(ch);
    port = ((InetSocketAddress)ch.getLocalAddress()).getPort();
    conf.set(ConfVars.PULLSERVER_PORT.varname, Integer.toString(port));
    pipelineFact.PullServer.setPort(port);
    LOG.info(getName() + " listening on port " + port);
    super.start();

    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
                                    DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);

    if (STANDALONE) {
      File pullServerPortFile = getPullServerPortFile();
      if (pullServerPortFile.exists()) {
        pullServerPortFile.delete();
      }
      pullServerPortFile.getParentFile().mkdirs();
      LOG.info("Write PullServerPort to " + pullServerPortFile);
      FileOutputStream out = null;
      try {
        out = new FileOutputStream(pullServerPortFile);
        out.write(("" + port).getBytes());
      } catch (Exception e) {
        LOG.fatal("PullServer exists cause can't write PullServer port to " + pullServerPortFile +
            ", " + e.getMessage(), e);
        System.exit(-1);
      } finally {
        IOUtils.closeStream(out);
      }
    }
    LOG.info("TajoPullServerService started: port=" + port);
  }

  public static boolean isStandalone() {
    return STANDALONE;
  }

  private static File getPullServerPortFile() {
    String pullServerPortInfoFile = System.getenv("TAJO_PID_DIR");
    if (StringUtils.isEmpty(pullServerPortInfoFile)) {
      pullServerPortInfoFile = "/tmp";
    }
    return new File(pullServerPortInfoFile + "/pullserver.port");
  }

  // TODO change to get port from master or tajoConf
  public static int readPullServerPort() {
    FileInputStream in = null;
    try {
      File pullServerPortFile = getPullServerPortFile();

      if (!pullServerPortFile.exists() || pullServerPortFile.isDirectory()) {
        return -1;
      }
      in = new FileInputStream(pullServerPortFile);
      byte[] buf = new byte[1024];
      int readBytes = in.read(buf);
      return Integer.parseInt(new String(buf, 0, readBytes));
    } catch (IOException e) {
      LOG.fatal(e.getMessage(), e);
      return -1;
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public int getPort() {
    return port;
  }

  @Override
  public synchronized void stop() {
    try {
      accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
      ServerBootstrap bootstrap = new ServerBootstrap(selector);
      bootstrap.releaseExternalResources();
      pipelineFact.destroy();

      localFS.close();
    } catch (Throwable t) {
      LOG.error(t);
    } finally {
      super.stop();
    }
  }

  public synchronized ByteBuffer getMeta() {
    try {
      return serializeMetaData(port); 
    } catch (IOException e) {
      LOG.error("Error during getMeta", e);
      // TODO add API to AuxiliaryServices to report failures
      return null;
    }
  }

  class HttpPipelineFactory implements ChannelPipelineFactory {

    final PullServer PullServer;
    private SSLFactory sslFactory;

    public HttpPipelineFactory(Configuration conf) throws Exception {
      PullServer = new PullServer(conf);
      if (conf.getBoolean(ConfVars.SHUFFLE_SSL_ENABLED_KEY.varname,
          ConfVars.SHUFFLE_SSL_ENABLED_KEY.defaultBoolVal)) {
        sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
        sslFactory.init();
      }
    }

    public void destroy() {
      if (sslFactory != null) {
        sslFactory.destroy();
      }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      if (sslFactory != null) {
        pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
      }

      int maxChunkSize = getConfig().getInt(ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.varname,
          ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.defaultIntVal);
      pipeline.addLast("codec", new HttpServerCodec(4096, 8192, maxChunkSize));
      pipeline.addLast("aggregator", new HttpChunkAggregator(1 << 16));
      pipeline.addLast("chunking", new ChunkedWriteHandler());
      pipeline.addLast("shuffle", PullServer);
      return pipeline;
      // TODO factor security manager into pipeline
      // TODO factor out encode/decode to permit binary shuffle
      // TODO factor out decode of index to permit alt. models
    }
  }


  Map<String, ProcessingStatus> processingStatusMap = new ConcurrentHashMap<String, ProcessingStatus>();

  public void completeFileChunk(FadvisedFileRegion filePart,
                                   String requestUri,
                                   long startTime) {
    ProcessingStatus status = processingStatusMap.get(requestUri);
    if (status != null) {
      status.decrementRemainFiles(filePart, startTime);
    }
  }

  class ProcessingStatus {
    String requestUri;
    int numFiles;
    AtomicInteger remainFiles;
    long startTime;
    long makeFileListTime;
    long minTime = Long.MAX_VALUE;
    long maxTime;
    int numSlowFile;

    public ProcessingStatus(String requestUri) {
      this.requestUri = requestUri;
      this.startTime = System.currentTimeMillis();
    }

    public void setNumFiles(int numFiles) {
      this.numFiles = numFiles;
      this.remainFiles = new AtomicInteger(numFiles);
    }
    public void decrementRemainFiles(FadvisedFileRegion filePart, long fileStartTime) {
      synchronized(remainFiles) {
        long fileSendTime = System.currentTimeMillis() - fileStartTime;
        if (fileSendTime > 20 * 1000) {
          LOG.info("PullServer send too long time: filePos=" + filePart.getPosition() + ", fileLen=" + filePart.getCount());
          numSlowFile++;
        }
        if (fileSendTime > maxTime) {
          maxTime = fileSendTime;
        }
        if (fileSendTime < minTime) {
          minTime = fileSendTime;
        }
        int remain = remainFiles.decrementAndGet();
        if (remain <= 0) {
          processingStatusMap.remove(requestUri);
          LOG.info("PullServer processing status: totalTime=" + (System.currentTimeMillis() - startTime) + " ms, " +
              "makeFileListTime=" + makeFileListTime + " ms, minTime=" + minTime + " ms, maxTime=" + maxTime + " ms, " +
              "numFiles=" + numFiles + ", numSlowFile=" + numSlowFile);
        }
      }
    }
  }

  class PullServer extends SimpleChannelUpstreamHandler {

    private final Configuration conf;
//    private final IndexCache indexCache;
    private final LocalDirAllocator lDirAlloc =
      new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);
    private int port;

    public PullServer(Configuration conf) throws IOException {
      this.conf = conf;
//      indexCache = new IndexCache(new JobConf(conf));
      this.port = conf.getInt(ConfVars.PULLSERVER_PORT.varname,
          ConfVars.PULLSERVER_PORT.defaultIntVal);

      // init local temporal dir
      lDirAlloc.getAllLocalPathsToRead(".", conf);
    }
    
    public void setPort(int port) {
      this.port = port;
    }

    private List<String> splitMaps(List<String> mapq) {
      if (null == mapq) {
        return null;
      }
      final List<String> ret = new ArrayList<String>();
      for (String s : mapq) {
        Collections.addAll(ret, s.split(","));
      }
      return ret;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent evt)
        throws Exception {

      accepted.add(evt.getChannel());
      LOG.info(String.format("Current number of shuffle connections (%d)", accepted.size()));
      super.channelOpen(ctx, evt);

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      HttpRequest request = (HttpRequest) e.getMessage();
      if (request.getMethod() != GET) {
        sendError(ctx, METHOD_NOT_ALLOWED);
        return;
      }

      ProcessingStatus processingStatus = new ProcessingStatus(request.getUri().toString());
      synchronized(processingStatusMap) {
        processingStatusMap.put(request.getUri().toString(), processingStatus);
      }
      // Parsing the URL into key-values
      final Map<String, List<String>> params =
          new QueryStringDecoder(request.getUri()).getParameters();
      final List<String> types = params.get("type");
      final List<String> qids = params.get("qid");
      final List<String> taskIdList = params.get("ta");
      final List<String> subQueryIds = params.get("sid");
      final List<String> partIds = params.get("p");
      final List<String> offsetList = params.get("offset");
      final List<String> lengthList = params.get("length");

      if (types == null || subQueryIds == null || qids == null || partIds == null) {
        sendError(ctx, "Required queryId, type, subquery Id, and part id",
            BAD_REQUEST);
        return;
      }

      if (qids.size() != 1 && types.size() != 1 || subQueryIds.size() != 1) {
        sendError(ctx, "Required qids, type, taskIds, subquery Id, and part id",
            BAD_REQUEST);
        return;
      }

      String partId = partIds.get(0);
      String queryId = qids.get(0);
      String shuffleType = types.get(0);
      String sid = subQueryIds.get(0);

      long offset = (offsetList != null && !offsetList.isEmpty()) ? Long.parseLong(offsetList.get(0)) : -1L;
      long length = (lengthList != null && !lengthList.isEmpty()) ? Long.parseLong(lengthList.get(0)) : -1L;

      if (!shuffleType.equals("h") && !shuffleType.equals("s") && taskIdList == null) {
        sendError(ctx, "Required taskIds", BAD_REQUEST);
      }

      List<String> taskIds = splitMaps(taskIdList);

      String queryBaseDir = queryId.toString() + "/output";

      if (LOG.isDebugEnabled()) {
        LOG.debug("PullServer request param: shuffleType=" + shuffleType +
            ", sid=" + sid + ", partId=" + partId + ", taskIds=" + taskIdList);

        // the working dir of tajo worker for each query
        LOG.debug("PullServer baseDir: " + conf.get(ConfVars.WORKER_TEMPORAL_DIR.varname) + "/" + queryBaseDir);
      }

      final List<FileChunk> chunks = Lists.newArrayList();

      // if a subquery requires a range shuffle
      if (shuffleType.equals("r")) {
        String ta = taskIds.get(0);
        if(!lDirAlloc.ifExists(queryBaseDir + "/" + sid + "/" + ta + "/output/", conf)){
          LOG.warn(e);
          sendError(ctx, NO_CONTENT);
          return;
        }
        Path path = localFS.makeQualified(
            lDirAlloc.getLocalPathToRead(queryBaseDir + "/" + sid + "/" + ta + "/output/", conf));
        String startKey = params.get("start").get(0);
        String endKey = params.get("end").get(0);
        boolean last = params.get("final") != null;

        FileChunk chunk;
        try {
          chunk = getFileCunks(path, startKey, endKey, last);
        } catch (Throwable t) {
          LOG.error("ERROR Request: " + request.getUri(), t);
          sendError(ctx, "Cannot get file chunks to be sent", BAD_REQUEST);
          return;
        }
        if (chunk != null) {
          chunks.add(chunk);
        }

        // if a subquery requires a hash shuffle or a scattered hash shuffle
      } else if (shuffleType.equals("h") || shuffleType.equals("s")) {
        int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), (TajoConf) conf);
        String partPath = queryBaseDir + "/" + sid + "/hash-shuffle/" + partParentId + "/" + partId;
        if (!lDirAlloc.ifExists(partPath, conf)) {
          LOG.warn("Partition shuffle file not exists: " + partPath);
          sendError(ctx, NO_CONTENT);
          return;
        }

        Path path = localFS.makeQualified(lDirAlloc.getLocalPathToRead(partPath, conf));

        File file = new File(path.toUri());
        long startPos = (offset >= 0 && length >= 0) ? offset : 0;
        long readLen = (offset >= 0 && length >= 0) ? length : file.length();

        if (startPos >= file.length()) {
          String errorMessage = "Start pos[" + startPos + "] great than file length [" + file.length() + "]";
          LOG.error(errorMessage);
          sendError(ctx, errorMessage, BAD_REQUEST);
          return;
        }
        LOG.info("RequestURL: " + request.getUri() + ", fileLen=" + file.length());
        FileChunk chunk = new FileChunk(file, startPos, readLen);
        chunks.add(chunk);
      } else {
        LOG.error("Unknown shuffle type: " + shuffleType);
        sendError(ctx, "Unknown shuffle type:" + shuffleType, BAD_REQUEST);
        return;
      }

      processingStatus.setNumFiles(chunks.size());
      processingStatus.makeFileListTime = System.currentTimeMillis() - processingStatus.startTime;
      // Write the content.
      Channel ch = e.getChannel();
      if (chunks.size() == 0) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NO_CONTENT);
        ch.write(response);
        if (!isKeepAlive(request)) {
          ch.close();
        }
      }  else {
        FileChunk[] file = chunks.toArray(new FileChunk[chunks.size()]);
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        long totalSize = 0;
        for (FileChunk chunk : file) {
          totalSize += chunk.length();
        }
        setContentLength(response, totalSize);

        // Write the initial line and the header.
        ch.write(response);

        ChannelFuture writeFuture = null;

        for (FileChunk chunk : file) {
          writeFuture = sendFile(ctx, ch, chunk, request.getUri().toString());
          if (writeFuture == null) {
            sendError(ctx, NOT_FOUND);
            return;
          }
        }

        // Decide whether to close the connection or not.
        if (!isKeepAlive(request)) {
          // Close the connection when the whole content is written out.
          writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
      }
    }

    private ChannelFuture sendFile(ChannelHandlerContext ctx,
                                   Channel ch,
                                   FileChunk file,
                                   String requestUri) throws IOException {
      long startTime = System.currentTimeMillis();
      RandomAccessFile spill = null;
      ChannelFuture writeFuture;
      try {
        spill = new RandomAccessFile(file.getFile(), "r");
        if (ch.getPipeline().get(SslHandler.class) == null) {
          final FadvisedFileRegionWrapper filePart = new FadvisedFileRegionWrapper(spill,
              file.startOffset, file.length(), manageOsCache, readaheadLength,
              readaheadPool, file.getFile().getAbsolutePath());
          writeFuture = ch.write(filePart);
          writeFuture.addListener(new FileCloseListener(filePart, requestUri, startTime, TajoPullServerService.this));
        } else {
          // HTTPS cannot be done with zero copy.
          final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
              file.startOffset, file.length, sslFileBufferSize,
              manageOsCache, readaheadLength, readaheadPool,
              file.getFile().getAbsolutePath());
          writeFuture = ch.write(chunk);
        }
      } catch (FileNotFoundException e) {
        LOG.info(file.getFile() + " not found");
        return null;
      } catch (Throwable e) {
        if (spill != null) {
          //should close a opening file
          spill.close();
        }
        return null;
      }
      metrics.shuffleConnections.incr();
      metrics.shuffleOutputBytes.incr(file.length); // optimistic
      return writeFuture;
    }

    private void sendError(ChannelHandlerContext ctx,
        HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    private void sendError(ChannelHandlerContext ctx, String message,
        HttpResponseStatus status) {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
      response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
      response.setContent(
        ChannelBuffers.copiedBuffer(message, CharsetUtil.UTF_8));

      // Close the connection as soon as the error message is sent.
      ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      LOG.error(e.getCause().getMessage(), e.getCause());
      //if channel.close() is not called, never closed files in this request
      if (ctx.getChannel().isConnected()){
        ctx.getChannel().close();
      }
    }
  }

  public FileChunk getFileCunks(Path outDir,
                                      String startKey,
                                      String endKey,
                                      boolean last) throws IOException {
    BSTIndex index = new BSTIndex(new TajoConf());
    BSTIndex.BSTIndexReader idxReader =
        index.getIndexReader(new Path(outDir, "index"));
    idxReader.open();
    Schema keySchema = idxReader.getKeySchema();
    TupleComparator comparator = idxReader.getComparator();

    LOG.info("BSTIndex is loaded from disk (" + idxReader.getFirstKey() + ", "
        + idxReader.getLastKey());

    File data = new File(URI.create(outDir.toUri() + "/output"));
    byte [] startBytes = Base64.decodeBase64(startKey);
    byte [] endBytes = Base64.decodeBase64(endKey);

    RowStoreDecoder decoder = RowStoreUtil.createDecoder(keySchema);
    Tuple start;
    Tuple end;
    try {
      start = decoder.toTuple(startBytes);
    } catch (Throwable t) {
      throw new IllegalArgumentException("StartKey: " + startKey
          + ", decoded byte size: " + startBytes.length, t);
    }

    try {
      end = decoder.toTuple(endBytes);
    } catch (Throwable t) {
      throw new IllegalArgumentException("EndKey: " + endKey
          + ", decoded byte size: " + endBytes.length, t);
    }

    LOG.info("GET Request for " + data.getAbsolutePath() + " (start="+start+", end="+ end +
        (last ? ", last=true" : "") + ")");

    if (idxReader.getFirstKey() == null && idxReader.getLastKey() == null) { // if # of rows is zero
      LOG.info("There is no contents");
      return null;
    }

    if (comparator.compare(end, idxReader.getFirstKey()) < 0 ||
        comparator.compare(idxReader.getLastKey(), start) < 0) {
      LOG.warn("Out of Scope (indexed data [" + idxReader.getFirstKey() + ", " + idxReader.getLastKey() +
          "], but request start:" + start + ", end: " + end);
      return null;
    }

    long startOffset;
    long endOffset;
    try {
      startOffset = idxReader.find(start);
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + "[" + start + ", " + end +")" + ", idx min: "
          + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
      throw ioe;
    }
    try {
      endOffset = idxReader.find(end);
      if (endOffset == -1) {
        endOffset = idxReader.find(end, true);
      }
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + "[" + start + ", " + end +")" + ", idx min: "
          + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
      throw ioe;
    }

    // if startOffset == -1 then case 2-1 or case 3
    if (startOffset == -1) { // this is a hack
      // if case 2-1 or case 3
      try {
        startOffset = idxReader.find(start, true);
      } catch (IOException ioe) {
        LOG.error("State Dump (the requested range: "
            + "[" + start + ", " + end +")" + ", idx min: "
            + idxReader.getFirstKey() + ", idx max: "
            + idxReader.getLastKey());
        throw ioe;
      }
    }

    if (startOffset == -1) {
      throw new IllegalStateException("startOffset " + startOffset + " is negative \n" +
          "State Dump (the requested range: "
          + "[" + start + ", " + end +")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
    }

    // if greater than indexed values
    if (last || (endOffset == -1
        && comparator.compare(idxReader.getLastKey(), end) < 0)) {
      endOffset = data.length();
    }

    idxReader.close();

    FileChunk chunk = new FileChunk(data, startOffset, endOffset - startOffset);
    LOG.info("Retrieve File Chunk: " + chunk);
    return chunk;
  }
}
