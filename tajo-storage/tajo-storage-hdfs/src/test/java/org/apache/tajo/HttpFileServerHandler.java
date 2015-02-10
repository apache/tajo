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

package org.apache.tajo;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HttpFileServerHandler extends ChannelInboundHandlerAdapter {
  
  private final Log LOG = LogFactory.getLog(HttpFileServerHandler.class);

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    
    try {
      if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;
        if (request.getMethod() != HttpMethod.GET) {
          sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
          return;
        }

        final String path = sanitizeUri(request.getUri());
        if (path == null) {
          sendError(ctx, HttpResponseStatus.FORBIDDEN);
          return;
        }

        File file = new File(path);
        if (file.isHidden() || !file.exists()) {
          sendError(ctx, HttpResponseStatus.NOT_FOUND);
          return;
        }
        if (!file.isFile()) {
          sendError(ctx, HttpResponseStatus.FORBIDDEN);
          return;
        }

        RandomAccessFile raf;
        try {
          raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException fnfe) {
          sendError(ctx, HttpResponseStatus.NOT_FOUND);
          return;
        }
        long fileLength = raf.length();

        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpHeaders.setContentLength(response, fileLength);
        setContentTypeHeader(response);

        // Write the initial line and the header.
        ctx.writeAndFlush(response);

        // Write the content.
        ChannelFuture writeFuture;
        ChannelFuture lastContentFuture;
        if (ctx.pipeline().get(SslHandler.class) != null) {
          // Cannot use zero-copy with HTTPS.
          lastContentFuture = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)));
        } else {
          // No encryption - use zero-copy.
          final FileRegion region = new DefaultFileRegion(raf.getChannel(), 0, fileLength);
          writeFuture = ctx.writeAndFlush(region);
          lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
          writeFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total)
                throws Exception {
              LOG.trace(String.format("%s: %d / %d", path, progress, total));
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) throws Exception {
              region.release();
            }
          });
        }

        // Decide whether to close the connection or not.
        if (!HttpHeaders.isKeepAlive(request)) {
          // Close the connection when the whole content is written out.
          lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
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

  private static String sanitizeUri(String uri) {
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
    if (uri.contains(File.separator + '.') ||
        uri.contains('.' + File.separator) ||
        uri.startsWith(".") || uri.endsWith(".")) {
      return null;
    }

    return uri;
  }

  private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
        Unpooled.copiedBuffer("Failure: " + status.toString() + "\r\n",
        CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

    // Close the connection as soon as the error message is sent.
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Sets the content type header for the HTTP Response
   *
   * @param response
   *            HTTP response
   */
  private static void setContentTypeHeader(HttpResponse response) {
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
  }

}
