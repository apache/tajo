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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.pullserver.retriever.DataRetriever;
import org.apache.tajo.pullserver.retriever.FileChunk;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
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

public class HttpDataServerHandler extends ChannelInboundHandlerAdapter {
  private final static Log LOG = LogFactory.getLog(HttpDataServerHandler.class);

  Map<ExecutionBlockId, DataRetriever> retrievers =
      new ConcurrentHashMap<ExecutionBlockId, DataRetriever>();
  private String userName;
  private String appId;

  public HttpDataServerHandler(String userName, String appId) {
    this.userName= userName;
    this.appId = appId;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) msg;
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
      // try {
      // file = retriever.handle(ctx, request);
      // } catch (FileNotFoundException fnf) {
      // LOG.error(fnf);
      // sendError(ctx, NOT_FOUND);
      // return;
      // } catch (IllegalArgumentException iae) {
      // LOG.error(iae);
      // sendError(ctx, BAD_REQUEST);
      // return;
      // } catch (FileAccessForbiddenException fafe) {
      // LOG.error(fafe);
      // sendError(ctx, FORBIDDEN);
      // return;
      // } catch (IOException ioe) {
      // LOG.error(ioe);
      // sendError(ctx, INTERNAL_SERVER_ERROR);
      // return;
      // }

      // Write the content.
      Channel ch = ctx.channel();
      if (file == null) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        ch.writeAndFlush(response);
        if (!HttpHeaders.isKeepAlive(request)) {
          ch.close();
        }
      } else {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        long totalSize = 0;
        for (FileChunk chunk : file) {
          totalSize += chunk.length();
        }
        HttpHeaders.setContentLength(response, totalSize);

        // Write the initial line and the header.
        ch.writeAndFlush(response);

        ChannelFuture writeFuture = null;

        for (FileChunk chunk : file) {
          writeFuture = sendFile(ctx, ch, chunk);
          if (writeFuture == null) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND);
            return;
          }
        }

        // Decide whether to close the connection or not.
        if (!HttpHeaders.isKeepAlive(request)) {
          // Close the connection when the whole content is written out.
          writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
      }
    }
  }

  private ChannelFuture sendFile(ChannelHandlerContext ctx,
                                 Channel ch,
                                 FileChunk file) throws IOException {
    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(file.getFile(), "r");
    } catch (FileNotFoundException fnfe) {
      return null;
    }

    ChannelFuture writeFuture;
    if (ch.pipeline().get(SslHandler.class) != null) {
      // Cannot use zero-copy with HTTPS.
      writeFuture = ch.writeAndFlush(new ChunkedFile(raf, file.startOffset(),
          file.length(), 8192));
    } else {
      // No encryption - use zero-copy.
      final FileRegion region = new DefaultFileRegion(raf.getChannel(),
          file.startOffset(), file.length());
      writeFuture = ch.writeAndFlush(region);
      writeFuture.addListener(new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
          if (region.refCnt() > 0) {
            region.release();
          }
        }
      });
    }

    return writeFuture;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    Channel ch = ctx.channel();
    if (cause instanceof TooLongFrameException) {
      sendError(ctx, HttpResponseStatus.BAD_REQUEST);
      return;
    }

    LOG.error(cause.getMessage(), cause);
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
    response.headers().add(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

    // Close the connection as soon as the error message is sent.
    ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  private List<String> splitMaps(List<String> qids) {
    if (null == qids) {
      LOG.error("QueryId is EMPTY");
      return null;
    }

    final List<String> ret = new ArrayList<String>();
    for (String qid : qids) {
      Collections.addAll(ret, qid.split(","));
    }
    return ret;
  }
}
