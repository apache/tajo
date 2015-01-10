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

package org.apache.tajo.worker.dataserver;

import io.netty.handler.codec.http.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.worker.dataserver.retriever.DataRetriever;
import org.apache.tajo.worker.dataserver.retriever.FileChunk;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class HttpDataServerHandler extends ChannelInboundHandlerAdapter {
  private final static Log LOG = LogFactory.getLog(HttpDataServer.class);
  private final DataRetriever retriever;

  public HttpDataServerHandler(DataRetriever retriever) {
    this.retriever = retriever;
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

      FileChunk[] file;
      try {
        file = retriever.handle(ctx, request);
      } catch (FileNotFoundException fnf) {
        LOG.error(fnf);
        sendError(ctx, HttpResponseStatus.NOT_FOUND);
        return;
      } catch (IllegalArgumentException iae) {
        LOG.error(iae);
        sendError(ctx, HttpResponseStatus.BAD_REQUEST);
        return;
      } catch (FileAccessForbiddenException fafe) {
        LOG.error(fafe);
        sendError(ctx, HttpResponseStatus.FORBIDDEN);
        return;
      } catch (IOException ioe) {
        LOG.error(ioe);
        sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }

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

  private ChannelFuture sendFile(ChannelHandlerContext ctx, Channel ch, FileChunk file) throws IOException {
    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(file.getFile(), "r");
    } catch (FileNotFoundException fnfe) {
      return null;
    }

    ChannelFuture writeFuture;
    ChannelFuture lastContentFuture;
    if (ch.pipeline().get(SslHandler.class) != null) {
      // Cannot use zero-copy with HTTPS.
      lastContentFuture = ch.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, file.startOffset(), file.length(), 8192)));
    } else {
      // No encryption - use zero-copy.
      final FileRegion region = new DefaultFileRegion(raf.getChannel(), file.startOffset(), file.length());
      writeFuture = ch.write(region);
      lastContentFuture = ch.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
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
}
