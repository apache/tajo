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

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.exception.ExceptionUtil;
import org.apache.tajo.pullserver.retriever.DataRetriever;
import org.apache.tajo.pullserver.retriever.FileChunk;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HttpDataServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private final static Log LOG = LogFactory.getLog(HttpDataServerHandler.class);

  Map<ExecutionBlockId, DataRetriever> retrievers =
          new ConcurrentHashMap<>();
  private String userName;
  private String appId;

  public HttpDataServerHandler(String userName, String appId) {
    this.userName= userName;
    this.appId = appId;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
      throws Exception {

    if (request.getMethod() != HttpMethod.GET) {
      sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
      return;
    }

    String base = ContainerLocalizer.USERCACHE + "/" + userName + "/" + ContainerLocalizer.APPCACHE + "/" + appId
        + "/output" + "/";

    final Map<String, List<String>> params = new QueryStringDecoder(request.getUri()).parameters();

    List<FileChunk> chunks = Lists.newArrayList();
    List<String> taskIds = splitMaps(params.get("ta"));
    int sid = Integer.valueOf(params.get("sid").get(0));
    int partitionId = Integer.valueOf(params.get("p").get(0));
    for (String ta : taskIds) {

      File file = new File(base + "/" + sid + "/" + ta + "/output/" + partitionId);
      FileChunk chunk = new FileChunk(file, 0, file.length());
      chunks.add(chunk);
    }

    FileChunk[] file = chunks.toArray(new FileChunk[chunks.size()]);

    // Write the content.
    if (file.length == 0) {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
      if (!HttpHeaders.isKeepAlive(request)) {
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
      } else {
        response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        ctx.writeAndFlush(response);
      }
    } else {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      ChannelFuture writeFuture = null;
      long totalSize = 0;
      for (FileChunk chunk : file) {
        totalSize += chunk.length();
      }
      HttpHeaders.setContentLength(response, totalSize);

      if (HttpHeaders.isKeepAlive(request)) {
        response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
      }
      // Write the initial line and the header.
      writeFuture = ctx.write(response);

      for (FileChunk chunk : file) {
        writeFuture = sendFile(ctx, chunk);
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

  private ChannelFuture sendFile(ChannelHandlerContext ctx,
                                 FileChunk file) throws IOException {
    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(file.getFile(), "r");
    } catch (FileNotFoundException fnfe) {
      return null;
    }

    ChannelFuture writeFuture;
    ChannelFuture lastContentFuture;
    if (ctx.pipeline().get(SslHandler.class) != null) {
      // Cannot use zero-copy with HTTPS.
      lastContentFuture = ctx.write(new HttpChunkedInput(new ChunkedFile(raf, file.startOffset(),
          file.length(), 8192)));
    } else {
      // No encryption - use zero-copy.
      final FileRegion region = new DefaultFileRegion(raf.getChannel(),
          file.startOffset(), file.length());
      writeFuture = ctx.write(region);
      lastContentFuture = ctx.write(LastHttpContent.EMPTY_LAST_CONTENT);
      writeFuture.addListener(new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
          if (region.refCnt() > 0) {
            region.release();
          }
        }
      });
    }

    return lastContentFuture;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    Channel ch = ctx.channel();
    if (cause instanceof TooLongFrameException) {
      sendError(ctx, HttpResponseStatus.BAD_REQUEST);
      return;
    }

    LOG.error(cause.getMessage());
    ExceptionUtil.printStackTraceIfError(LOG, cause);
    if (ch.isActive()) {
      sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public static String sanitizeUri(String uri) {
    // Decode the path.
    try {
      uri = URLDecoder.decode(uri, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      try {
        uri = URLDecoder.decode(uri, "ISO-8859-1");
      } catch (UnsupportedEncodingException e1) {
        throw new Error();
      }
    }

    // Convert file separators.
    uri = uri.replace('/', File.separatorChar);

    // Simplistic dumb security check.
    // You will have to do something serious in the production environment.
    if (uri.contains(File.separator + ".")
        || uri.contains("." + File.separator) || uri.startsWith(".")
        || uri.endsWith(".")) {
      return null;
    }

    // Convert to absolute path.
    return uri;
  }

  private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
        Unpooled.copiedBuffer("Failure: " + status.toString() + "\r\n", CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

    // Close the connection as soon as the error message is sent.
    ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  private List<String> splitMaps(List<String> qids) {
    if (null == qids) {
      LOG.error("QueryId is EMPTY");
      return null;
    }

    final List<String> ret = new ArrayList<>();
    for (String qid : qids) {
      Collections.addAll(ret, qid.split(","));
    }
    return ret;
  }
}
