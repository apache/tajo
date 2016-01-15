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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
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
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.InvalidURLException;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.rpc.NettyUtils;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.TajoIdUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class TajoPullServerService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(TajoPullServerService.class);

  public static final String SHUFFLE_MANAGE_OS_CACHE = "tajo.pullserver.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_READAHEAD_BYTES = "tajo.pullserver.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;

  private int port;
  private ServerBootstrap selector;
  private final ChannelGroup accepted = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private HttpChannelInitializer channelInitializer;
  private int sslFileBufferSize;
  private int maxUrlLength;

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
          new ConcurrentHashMap<>();
  private String userName;

  private static LoadingCache<CacheKey, BSTIndexReader> indexReaderCache = null;
  private static int lowCacheHitCheckThreshold;

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
    "tajo.pullserver.ssl.file.buffer.size";

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

  private static final boolean STANDALONE;

  private static final AtomicIntegerFieldUpdater<ProcessingStatus> SLOW_FILE_UPDATER;
  private static final AtomicIntegerFieldUpdater<ProcessingStatus> REMAIN_FILE_UPDATER;

  public static final String CHUNK_LENGTH_HEADER_NAME = "c";

  static class CacheKey {
    private Path path;
    private String queryId;
    private String ebSeqId;

    public CacheKey(Path path, String queryId, String ebSeqId) {
      this.path = path;
      this.queryId = queryId;
      this.ebSeqId = ebSeqId;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CacheKey) {
        CacheKey other = (CacheKey) o;
        return Objects.equals(this.path, other.path)
            && Objects.equals(this.queryId, other.queryId)
            && Objects.equals(this.ebSeqId, other.ebSeqId);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, queryId, ebSeqId);
    }
  }

  static {
    /* AtomicIntegerFieldUpdater can save the memory usage instead of AtomicInteger instance */
    SLOW_FILE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ProcessingStatus.class, "numSlowFile");
    REMAIN_FILE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ProcessingStatus.class, "remainFiles");

    String standalone = System.getenv("TAJO_PULLSERVER_STANDALONE");
    STANDALONE = !StringUtils.isEmpty(standalone) && standalone.equalsIgnoreCase("true");
  }

  @Metrics(name="PullServerShuffleMetrics", about="PullServer output metrics", context="tajo")
  static class ShuffleMetrics implements GenericFutureListener<ChannelFuture> {
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

      int workerNum = conf.getInt("tajo.shuffle.rpc.server.worker-thread-num",
          Runtime.getRuntime().availableProcessors() * 2);

      selector = NettyUtils.createServerBootstrap("TajoPullServerService", workerNum)
                   .option(ChannelOption.TCP_NODELAY, true)
                   .childOption(ChannelOption.ALLOCATOR, NettyUtils.ALLOCATOR)
                   .childOption(ChannelOption.TCP_NODELAY, true);

      localFS = new LocalFileSystem();

      maxUrlLength = conf.getInt(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH.name(),
          ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH.defaultIntVal);

      conf.setInt(TajoConf.ConfVars.PULLSERVER_PORT.varname
          , conf.getInt(TajoConf.ConfVars.PULLSERVER_PORT.varname, TajoConf.ConfVars.PULLSERVER_PORT.defaultIntVal));
      super.init(conf);
      LOG.info("Tajo PullServer initialized: readaheadLength=" + readaheadLength);
    } catch (Throwable t) {
      LOG.error(t, t);
    }
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }

    ServerBootstrap bootstrap = selector.clone();
    TajoConf tajoConf = (TajoConf)conf;
    try {
      channelInitializer = new HttpChannelInitializer(tajoConf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    bootstrap.childHandler(channelInitializer)
      .channel(NioServerSocketChannel.class);

    port = tajoConf.getIntVar(ConfVars.PULLSERVER_PORT);
    ChannelFuture future = bootstrap.bind(new InetSocketAddress(port))
        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE)
        .syncUninterruptibly();

    accepted.add(future.channel());
    port = ((InetSocketAddress)future.channel().localAddress()).getPort();
    tajoConf.set(ConfVars.PULLSERVER_PORT.varname, Integer.toString(port));
    LOG.info(getName() + " listening on port " + port);

    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
                                    DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);

    int cacheSize = tajoConf.getIntVar(ConfVars.PULLSERVER_CACHE_SIZE);
    int cacheTimeout = tajoConf.getIntVar(ConfVars.PULLSERVER_CACHE_TIMEOUT);

    indexReaderCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .expireAfterWrite(cacheTimeout, TimeUnit.MINUTES)
        .removalListener(removalListener)
        .build(
            new CacheLoader<CacheKey, BSTIndexReader>() {
              @Override
              public BSTIndexReader load(CacheKey key) throws Exception {
                return new BSTIndex(tajoConf).getIndexReader(new Path(key.path, "index"));
              }
            }
        );
    lowCacheHitCheckThreshold = (int) (cacheSize * 0.1f);

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
    super.serviceInit(conf);
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
  public void stop() {
    try {
      accepted.close();
      if (selector != null) {
        if (selector.group() != null) {
          selector.group().shutdownGracefully();
        }
        if (selector.childGroup() != null) {
          selector.childGroup().shutdownGracefully();
        }
      }

      if (channelInitializer != null) {
        channelInitializer.destroy();
      }

      localFS.close();
      indexReaderCache.invalidateAll();
    } catch (Throwable t) {
      LOG.error(t, t);
    } finally {
      super.stop();
    }
  }

  class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {

    final PullServer PullServer;
    private SSLFactory sslFactory;

    public HttpChannelInitializer(TajoConf conf) throws Exception {
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
    protected void initChannel(SocketChannel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      if (sslFactory != null) {
        pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
      }

      int maxChunkSize = getConfig().getInt(ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.varname,
          ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.defaultIntVal);
      pipeline.addLast("codec", new HttpServerCodec(maxUrlLength, 8192, maxChunkSize));
      pipeline.addLast("aggregator", new HttpObjectAggregator(1 << 16));
      pipeline.addLast("chunking", new ChunkedWriteHandler());
      pipeline.addLast("shuffle", PullServer);
      // TODO factor security manager into pipeline
      // TODO factor out encode/decode to permit binary shuffle
      // TODO factor out decode of index to permit alt. models
    }
  }


  Map<String, ProcessingStatus> processingStatusMap = new ConcurrentHashMap<>();

  public void completeFileChunk(FileRegion filePart,
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
    long startTime;
    long makeFileListTime;
    long minTime = Long.MAX_VALUE;
    long maxTime;
    volatile int numSlowFile;
    volatile int remainFiles;

    public ProcessingStatus(String requestUri) {
      this.requestUri = requestUri;
      this.startTime = System.currentTimeMillis();
    }

    public void setNumFiles(int numFiles) {
      this.numFiles = numFiles;
      this.remainFiles = numFiles;
    }

    public void decrementRemainFiles(FileRegion filePart, long fileStartTime) {
      long fileSendTime = System.currentTimeMillis() - fileStartTime;

      if (fileSendTime > maxTime) {
        maxTime = fileSendTime;
      }
      if (fileSendTime < minTime) {
        minTime = fileSendTime;
      }

      if (fileSendTime > 20 * 1000) {
        LOG.warn("Sending data takes too long. " + fileSendTime + "ms elapsed, " +
            "length:" + (filePart.count() - filePart.position()) + ", URI:" + requestUri);
        SLOW_FILE_UPDATER.compareAndSet(this, numSlowFile, numSlowFile + 1);
      }

      REMAIN_FILE_UPDATER.compareAndSet(this, remainFiles, remainFiles - 1);
      if (REMAIN_FILE_UPDATER.get(this) <= 0) {
        processingStatusMap.remove(requestUri);
        if(LOG.isDebugEnabled()) {
          LOG.debug("PullServer processing status: totalTime=" + (System.currentTimeMillis() - startTime) + " ms, "
              + "makeFileListTime=" + makeFileListTime + " ms, minTime=" + minTime + " ms, maxTime=" + maxTime + " ms, "
              + "numFiles=" + numFiles + ", numSlowFile=" + numSlowFile);
        }
      }
    }
  }

  @ChannelHandler.Sharable
  class PullServer extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final TajoConf conf;
    private final LocalDirAllocator lDirAlloc =
      new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);

    public PullServer(TajoConf conf) throws IOException {
      this.conf = conf;

      // init local temporal dir
      lDirAlloc.getAllLocalPathsToRead(".", conf);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      accepted.add(ctx.channel());

      if(LOG.isDebugEnabled()) {
        LOG.debug(String.format("Current number of shuffle connections (%d)", accepted.size()));
      }
      super.channelRegistered(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
            throws Exception {

      if (request.getDecoderResult().isFailure()) {
        LOG.error("Http decoding failed. ", request.getDecoderResult().cause());
        sendError(ctx, request.getDecoderResult().toString(), HttpResponseStatus.BAD_REQUEST);
        return;
      }

      if (request.getMethod() == HttpMethod.DELETE) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);

        clearIndexCache(request.getUri());
        return;
      } else if (request.getMethod() != HttpMethod.GET) {
        sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
        return;
      }

      // Parsing the URL into key-values
      Map<String, List<String>> params = null;
      try {
        params = decodeParams(request.getUri());
      } catch (Throwable e) {
        LOG.error("Failed to decode uri " + request.getUri());
        sendError(ctx, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        return;
      }

      ProcessingStatus processingStatus = new ProcessingStatus(request.getUri());
      processingStatusMap.put(request.getUri(), processingStatus);

      String partId = params.get("p").get(0);
      String queryId = params.get("qid").get(0);
      String shuffleType = params.get("type").get(0);
      String sid =  params.get("sid").get(0);

      final List<String> taskIdList = params.get("ta");
      final List<String> offsetList = params.get("offset");
      final List<String> lengthList = params.get("length");

      long offset = (offsetList != null && !offsetList.isEmpty()) ? Long.parseLong(offsetList.get(0)) : -1L;
      long length = (lengthList != null && !lengthList.isEmpty()) ? Long.parseLong(lengthList.get(0)) : -1L;

      List<String> taskIds = splitMaps(taskIdList);

      Path queryBaseDir = getBaseOutputDir(queryId, sid);

      if (LOG.isDebugEnabled()) {
        LOG.debug("PullServer request param: shuffleType=" + shuffleType + ", sid=" + sid + ", partId=" + partId
            + ", taskIds=" + taskIdList);

        // the working dir of tajo worker for each query
        LOG.debug("PullServer baseDir: " + conf.get(ConfVars.WORKER_TEMPORAL_DIR.varname) + "/" + queryBaseDir);
      }

      final List<FileChunk> chunks = Lists.newArrayList();

      // if a stage requires a range shuffle
      if (shuffleType.equals("r")) {
        final String startKey = params.get("start").get(0);
        final String endKey = params.get("end").get(0);
        final boolean last = params.get("final") != null;

        long before = System.currentTimeMillis();
        for (String eachTaskId : taskIds) {
          Path outputPath = StorageUtil.concatPath(queryBaseDir, eachTaskId, "output");
          if (!lDirAlloc.ifExists(outputPath.toString(), conf)) {
            LOG.warn(outputPath + "does not exist.");
            continue;
          }
          Path path = localFS.makeQualified(lDirAlloc.getLocalPathToRead(outputPath.toString(), conf));

          FileChunk chunk;
          try {
            chunk = getFileChunks(queryId, sid, path, startKey, endKey, last);
          } catch (Throwable t) {
            LOG.error("ERROR Request: " + request.getUri(), t);
            sendError(ctx, "Cannot get file chunks to be sent", HttpResponseStatus.BAD_REQUEST);
            return;
          }
          if (chunk != null) {
            chunks.add(chunk);
          }
        }
        long after = System.currentTimeMillis();
        LOG.info("Index lookup time: " + (after - before) + " ms");

        // if a stage requires a hash shuffle or a scattered hash shuffle
      } else if (shuffleType.equals("h") || shuffleType.equals("s")) {
        int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
        Path partPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);
        if (!lDirAlloc.ifExists(partPath.toString(), conf)) {
          LOG.warn("Partition shuffle file not exists: " + partPath);
          sendError(ctx, HttpResponseStatus.NO_CONTENT);
          return;
        }

        Path path = localFS.makeQualified(lDirAlloc.getLocalPathToRead(partPath.toString(), conf));

        File file = new File(path.toUri());
        long startPos = (offset >= 0 && length >= 0) ? offset : 0;
        long readLen = (offset >= 0 && length >= 0) ? length : file.length();

        if (startPos >= file.length()) {
          String errorMessage = "Start pos[" + startPos + "] great than file length [" + file.length() + "]";
          LOG.error(errorMessage);
          sendError(ctx, errorMessage, HttpResponseStatus.BAD_REQUEST);
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("RequestURL: " + request.getUri() + ", fileLen=" + file.length());
        }
        FileChunk chunk = new FileChunk(file, startPos, readLen);
        chunks.add(chunk);
      } else {
        LOG.error("Unknown shuffle type: " + shuffleType);
        sendError(ctx, "Unknown shuffle type:" + shuffleType, HttpResponseStatus.BAD_REQUEST);
        return;
      }

      // Write the content.
      if (chunks.size() == 0) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);

        if (!HttpHeaders.isKeepAlive(request)) {
          ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } else {
          response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
          ctx.writeAndFlush(response);
        }
      } else {
        FileChunk[] file = chunks.toArray(new FileChunk[chunks.size()]);
        ChannelFuture writeFuture = null;
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        long totalSize = 0;
        StringBuilder sb = new StringBuilder();
        for (FileChunk chunk : file) {
          totalSize += chunk.length();
          sb.append(Long.toString(chunk.length())).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        HttpHeaders.addHeader(response, CHUNK_LENGTH_HEADER_NAME, sb.toString());
        HttpHeaders.setContentLength(response, totalSize);

        if (HttpHeaders.isKeepAlive(request)) {
          response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }
        // Write the initial line and the header.
        writeFuture = ctx.write(response);

        for (FileChunk chunk : file) {
          writeFuture = sendFile(ctx, chunk, request.getUri());
          if (writeFuture == null) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND);
            return;
          }
        }

        if (ctx.pipeline().get(SslHandler.class) == null) {
          writeFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        } else {
          ctx.flush();
        }

        // Decide whether to close the connection or not.
        if (!HttpHeaders.isKeepAlive(request)) {
          // Close the connection when the whole content is written out.
          writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
      }
    }

    /**
     * Upon a request from TajoWorker, this method clears index cache for fetching data of an execution block.
     * It is called whenever an execution block is completed.
     *
     * @param uri query URI which indicates the execution block id
     * @throws IOException
     * @throws InvalidURLException
     */
    private void clearIndexCache(String uri) throws IOException, InvalidURLException {
      // Simply parse the given uri
      String[] tokens = uri.split("=");
      if (tokens.length != 2 || !tokens[0].equals("ebid")) {
        throw new IllegalArgumentException("invalid params: " + uri);
      }
      ExecutionBlockId ebId = TajoIdUtils.createExecutionBlockId(tokens[1]);
      String queryId = ebId.getQueryId().toString();
      String ebSeqId = Integer.toString(ebId.getId());
      List<CacheKey> removed = new ArrayList<>();
      synchronized (indexReaderCache) {
        for (Entry<CacheKey, BSTIndexReader> e : indexReaderCache.asMap().entrySet()) {
          CacheKey key = e.getKey();
          if (key.queryId.equals(queryId) && key.ebSeqId.equals(ebSeqId)) {
            e.getValue().forceClose();
            removed.add(e.getKey());
          }
        }
        indexReaderCache.invalidateAll(removed);
      }
      removed.clear();
      synchronized (waitForRemove) {
        for (Entry<CacheKey, BSTIndexReader> e : waitForRemove.entrySet()) {
          CacheKey key = e.getKey();
          if (key.queryId.equals(queryId) && key.ebSeqId.equals(ebSeqId)) {
            e.getValue().forceClose();
            removed.add(e.getKey());
          }
        }
        for (CacheKey eachKey : removed) {
          waitForRemove.remove(eachKey);
        }
      }
    }

    private ChannelFuture sendFile(ChannelHandlerContext ctx,
                                   FileChunk file,
                                   String requestUri) throws IOException {
      long startTime = System.currentTimeMillis();
      RandomAccessFile spill = null;      
      ChannelFuture writeFuture;
      try {
        spill = new RandomAccessFile(file.getFile(), "r");
        if (ctx.pipeline().get(SslHandler.class) == null) {
          final FadvisedFileRegion filePart = new FadvisedFileRegion(spill,
              file.startOffset(), file.length(), manageOsCache, readaheadLength,
              readaheadPool, file.getFile().getAbsolutePath());
          writeFuture = ctx.write(filePart);
          writeFuture.addListener(new FileCloseListener(filePart, requestUri, startTime, TajoPullServerService.this));
        } else {
          // HTTPS cannot be done with zero copy.
          final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
              file.startOffset(), file.length(), sslFileBufferSize,
              manageOsCache, readaheadLength, readaheadPool,
              file.getFile().getAbsolutePath());
          writeFuture = ctx.write(new HttpChunkedInput(chunk));
        }
      } catch (FileNotFoundException e) {
        LOG.fatal(file.getFile() + " not found");
        return null;
      } catch (Throwable e) {
        if (spill != null) {
          //should close a opening file
          spill.close();
        }
        return null;
      }
      metrics.shuffleConnections.incr();
      metrics.shuffleOutputBytes.incr(file.length()); // optimistic
      return writeFuture;
    }

    private void sendError(ChannelHandlerContext ctx,
        HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    private void sendError(ChannelHandlerContext ctx, String message,
        HttpResponseStatus status) {
      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
          Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));
      response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

      // Close the connection as soon as the error message is sent.
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      LOG.error(cause.getMessage(), cause);
      if (ctx.channel().isOpen()) {
        ctx.channel().close();
      }
    }
  }

  // Temporal space to wait for the completion of all index lookup operations
  private static final ConcurrentHashMap<CacheKey, BSTIndexReader> waitForRemove = new ConcurrentHashMap<>();

  // RemovalListener is triggered when an item is removed from the index reader cache.
  // It closes index readers when they are not used anymore.
  // If they are still being used, they are moved to waitForRemove map to wait for other operations' completion.
  private static final RemovalListener<CacheKey, BSTIndexReader> removalListener = (removal) -> {
    BSTIndexReader reader = removal.getValue();
    if (reader.getReferenceNum() == 0) {
      try {
        reader.close(); // tear down properly
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      waitForRemove.remove(removal.getKey());
    } else {
      waitForRemove.put(removal.getKey(), reader);
    }
  };

  public static FileChunk getFileChunks(String queryId,
                                        String ebSeqId,
                                        Path outDir,
                                        String startKey,
                                        String endKey,
                                        boolean last) throws IOException, ExecutionException {

    BSTIndexReader idxReader = indexReaderCache.get(new CacheKey(outDir, queryId, ebSeqId));
    idxReader.retain();

    File data;
    long startOffset;
    long endOffset;
    try {
      if (LOG.isDebugEnabled()) {
        if (indexReaderCache.size() > lowCacheHitCheckThreshold && indexReaderCache.stats().hitRate() < 0.5) {
          LOG.debug("Too low cache hit rate: " + indexReaderCache.stats());
        }
      }

      Tuple indexedFirst = idxReader.getFirstKey();
      Tuple indexedLast = idxReader.getLastKey();

      if (indexedFirst == null && indexedLast == null) { // if # of rows is zero
        if (LOG.isDebugEnabled()) {
          LOG.debug("There is no contents");
        }
        return null;
      }

      byte[] startBytes = Base64.decodeBase64(startKey);
      byte[] endBytes = Base64.decodeBase64(endKey);


      Tuple start;
      Tuple end;
      Schema keySchema = idxReader.getKeySchema();
      RowStoreDecoder decoder = RowStoreUtil.createDecoder(keySchema);

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

      data = new File(URI.create(outDir.toUri() + "/output"));
      if (LOG.isDebugEnabled()) {
        LOG.debug("GET Request for " + data.getAbsolutePath() + " (start=" + start + ", end=" + end +
            (last ? ", last=true" : "") + ")");
      }

      TupleComparator comparator = idxReader.getComparator();

      if (comparator.compare(end, indexedFirst) < 0 ||
          comparator.compare(indexedLast, start) < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Out of Scope (indexed data [" + indexedFirst + ", " + indexedLast +
              "], but request start:" + start + ", end: " + end);
        }
        return null;
      }

      try {
        idxReader.init();
        startOffset = idxReader.find(start);
      } catch (IOException ioe) {
        LOG.error("State Dump (the requested range: "
            + "[" + start + ", " + end + ")" + ", idx min: "
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
            + "[" + start + ", " + end + ")" + ", idx min: "
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
              + "[" + start + ", " + end + ")" + ", idx min: "
              + idxReader.getFirstKey() + ", idx max: "
              + idxReader.getLastKey());
          throw ioe;
        }
      }

      if (startOffset == -1) {
        throw new IllegalStateException("startOffset " + startOffset + " is negative \n" +
            "State Dump (the requested range: "
            + "[" + start + ", " + end + ")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
            + idxReader.getLastKey());
      }

      // if greater than indexed values
      if (last || (endOffset == -1
          && comparator.compare(idxReader.getLastKey(), end) < 0)) {
        endOffset = data.length();
      }
    } finally {
      idxReader.release();
    }

    FileChunk chunk = new FileChunk(data, startOffset, endOffset - startOffset);

    if (LOG.isDebugEnabled()) LOG.debug("Retrieve File Chunk: " + chunk);
    return chunk;
  }

  public static List<String> splitMaps(List<String> mapq) {
    if (null == mapq) {
      return null;
    }
    final List<String> ret = new ArrayList<>();
    for (String s : mapq) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }

  public static Map<String, List<String>> decodeParams(String uri) {
    final Map<String, List<String>> params = new QueryStringDecoder(uri).parameters();
    final List<String> types = params.get("type");
    final List<String> qids = params.get("qid");
    final List<String> ebIds = params.get("sid");
    final List<String> partIds = params.get("p");

    if (types == null || ebIds == null || qids == null || partIds == null) {
      throw new IllegalArgumentException("invalid params. required :" + params);
    }

    if (qids.size() != 1 && types.size() != 1 || ebIds.size() != 1) {
      throw new IllegalArgumentException("invalid params. required :" + params);
    }

    return params;
  }

  public static Path getBaseOutputDir(String queryId, String executionBlockSequenceId) {
    Path workDir =
        StorageUtil.concatPath(
            queryId,
            "output",
            executionBlockSequenceId);
    return workDir;
  }

  public static Path getBaseInputDir(String queryId, String executionBlockId) {
    Path workDir =
        StorageUtil.concatPath(
            queryId,
            "in",
            executionBlockId);
    return workDir;
  }
}
