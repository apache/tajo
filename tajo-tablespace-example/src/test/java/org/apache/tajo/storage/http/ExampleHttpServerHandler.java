/*
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

package org.apache.tajo.storage.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.util.CharsetUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class ExampleHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

  private static final Log LOG = LogFactory.getLog(ExampleHttpServerHandler.class);

  @Override
  protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {

    if (request.getMethod().equals(HttpMethod.HEAD)) {

      processHead(context, request);

    } else if (request.getMethod().equals(HttpMethod.GET)) {

      processGet(context, request);

    } else {
      // error
      String msg = "Not supported method: " + request.getMethod();
      LOG.error(msg);
      context.writeAndFlush(getBadRequest(msg));
    }
  }

  private void processHead(ChannelHandlerContext context, FullHttpRequest request) {
    HttpHeaders headers = request.headers();
    FullHttpResponse response = null;

    if (headers.contains(Names.CONTENT_LENGTH)) {

      try {
        File file = getRequestedFile(request.getUri());

        response = new DefaultFullHttpResponse(
            HTTP_1_1,
            request.getDecoderResult().isSuccess() ? OK : BAD_REQUEST
        );

        HttpHeaders.setContentLength(response, file.length());


      } catch (FileNotFoundException | URISyntaxException e) {
        response = getBadRequest(e.getMessage());
      }
    }

    context.writeAndFlush(response);
  }

  private void processGet(ChannelHandlerContext context, FullHttpRequest request) {
    try {
      File file = getRequestedFile(request.getUri());

      RandomAccessFile raf = new RandomAccessFile(file, "r");
      long fileLength = raf.length();

      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      HttpHeaders.setContentLength(response, fileLength);
      setContentTypeHeader(response, file);

      context.write(response);

      context.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength));

      // Write the end marker.
      ChannelFuture future = context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
      future.addListener(ChannelFutureListener.CLOSE);

    } catch (IOException | URISyntaxException e) {
      context.writeAndFlush(getBadRequest(e.getMessage()));
    }
  }

  private static File getRequestedFile(String uri) throws FileNotFoundException, URISyntaxException {
    String path = URI.create(uri).getPath();
    URL url = ClassLoader.getSystemResource("dataset/" + path);

    if (url == null) {
      throw new FileNotFoundException(uri);
    }
    return new File(url.toURI());
  }

  private static FullHttpResponse getBadRequest(String message) {
    return new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST,
        Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    LOG.error(cause.getMessage(), cause);
    if (context.channel().isOpen()) {
      context.channel().close();
    }
  }

  /**
   * Sets the content type header for the HTTP Response
   * @param response HTTP response
   * @param file file to extract content type
   */
  private static void setContentTypeHeader(HttpResponse response, File file) {
    MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
    response.headers().set(CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
  }
}
