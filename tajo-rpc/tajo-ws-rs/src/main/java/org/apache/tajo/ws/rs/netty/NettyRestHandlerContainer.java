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

package org.apache.tajo.ws.rs.netty;

import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.*;
import org.glassfish.jersey.server.internal.ConfigHelper;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.glassfish.jersey.server.spi.ContainerResponseWriter;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Jersy Container implementation on Netty
 */
@Sharable
public class NettyRestHandlerContainer extends ChannelDuplexHandler implements Container {

  private static Log LOG = LogFactory.getLog(NettyRestHandlerContainer.class);

  private String rootPath;

  private ApplicationHandler applicationHandler;
  private ContainerLifecycleListener lifecycleListener;

  NettyRestHandlerContainer(Application application) {
    this(new ApplicationHandler(application));
  }

  NettyRestHandlerContainer(Application application, ServiceLocator parentLocator) {
    this(new ApplicationHandler(application, null, parentLocator));
  }

  NettyRestHandlerContainer(ApplicationHandler appHandler) {
    applicationHandler = appHandler;
    lifecycleListener = ConfigHelper.getContainerLifecycleListener(applicationHandler);
  }

  @Override
  public ResourceConfig getConfiguration() {
    return applicationHandler.getConfiguration();
  }

  @Override
  public void reload() {
    reload(getConfiguration());
  }

  @Override
  public void reload(ResourceConfig configuration) {
    lifecycleListener.onShutdown(this);
    applicationHandler = new ApplicationHandler(configuration);
    lifecycleListener = ConfigHelper.getContainerLifecycleListener(applicationHandler);
    lifecycleListener.onReload(this);
    lifecycleListener.onStartup(this);

    if (LOG.isDebugEnabled()) {
      LOG.debug("NettyRestHandlerContainer reloaded.");
    }
  }

  public void setRootPath(String rootPath) {
    String tempRootPath = rootPath;
    if (tempRootPath == null || tempRootPath.isEmpty()) {
      tempRootPath = "/";
    } else if (tempRootPath.charAt(tempRootPath.length() - 1) != '/') {
      tempRootPath += "/";
    }
    this.rootPath = tempRootPath;
  }
  
  private URI getBaseUri(ChannelHandlerContext ctx, FullHttpRequest request) {
    URI baseUri;
    String scheme;
    
    if (ctx.pipeline().get(SslHandler.class) == null) {
      scheme = "http";
    } else {
      scheme = "https";
    }
    
    List<String> hosts = request.headers().getAll(HttpHeaders.Names.HOST);
    try {
      if (hosts != null && hosts.size() > 0) {
        baseUri = new URI(scheme + "://" + hosts.get(0) + rootPath);
      } else {
        InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
        baseUri = new URI(scheme, null, localAddress.getHostName(), localAddress.getPort(),
                    rootPath, null, null);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    
    return baseUri;
  }

  protected void messageReceived(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
    URI baseUri = getBaseUri(ctx, request);
    URI requestUri = baseUri.resolve(request.getUri());
    ByteBuf responseContent = PooledByteBufAllocator.DEFAULT.buffer();
    FullHttpResponse response = 
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, responseContent);
    
    NettyRestResponseWriter responseWriter = new NettyRestResponseWriter(ctx, response);
    ContainerRequest containerRequest = new ContainerRequest(baseUri, requestUri, 
        request.getMethod().name(), getSecurityContext(), new MapPropertiesDelegate());
    containerRequest.setEntityStream(new ByteBufInputStream(request.content()));
    
    HttpHeaders httpHeaders = request.headers();
    for (String headerName: httpHeaders.names()) {
      List<String> headerValues = httpHeaders.getAll(headerName);
      containerRequest.headers(headerName, headerValues);
    }
    containerRequest.setWriter(responseWriter);
    try {
      applicationHandler.handle(containerRequest);
    } finally {
      responseWriter.releaseConnection();
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    boolean needRelease = true;
    try {
      if (msg instanceof FullHttpRequest) {
        FullHttpRequest request = (FullHttpRequest) msg;
        messageReceived(ctx, request);
      } else {
        needRelease = false;
        ctx.fireChannelRead(msg);
      }
    } finally {
      if (needRelease) {
        ReferenceCountUtil.release(msg);
      }
    }
  }

  private SecurityContext getSecurityContext() {
    return new SecurityContext() {

      @Override
      public boolean isUserInRole(String role) {
        return false;
      }

      @Override
      public boolean isSecure() {
        return false;
      }

      @Override
      public Principal getUserPrincipal() {
        return null;
      }

      @Override
      public String getAuthenticationScheme() {
        return null;
      }
    };
  }

  /**
   * Internal class for writing content on REST service.
   */
  static class NettyRestResponseWriter implements ContainerResponseWriter {

    private final ChannelHandlerContext ctx;
    private final FullHttpResponse response;
    private final AtomicBoolean closed;

    public NettyRestResponseWriter(ChannelHandlerContext ctx, FullHttpResponse response) {
      this.ctx = ctx;
      this.response = response;
      this.closed = new AtomicBoolean(false);
    }

    @Override
    public void commit() {
      if (closed.compareAndSet(false, true)) {
        ctx.write(response);
        sendLastHttpContent();
      }
    }

    @Override
    public boolean enableResponseBuffering() {
      return false;
    }

    @Override
    public void failure(Throwable error) {
      try {
        sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, error);
      } finally {
        if (ctx.channel().isActive()) {
          ctx.close();
        }
      }
    }
    
    private void sendError(HttpResponseStatus status, final Throwable error) {
      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
          Unpooled.copiedBuffer(error.getMessage(), CharsetUtil.UTF_8));
      response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
      ChannelPromise promise = ctx.newPromise();
      promise.addListener(new GenericFutureListener<ChannelFuture>() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            throw new ContainerException(error);
          }
        }
      });

      ctx.writeAndFlush(response, promise);
    }

    @Override
    public void setSuspendTimeout(long timeOut, TimeUnit timeUnit) throws IllegalStateException {
      throw new UnsupportedOperationException("setSuspendTimeout is not supported on this container.");
    }

    @Override
    public boolean suspend(long timeOut, TimeUnit timeUnit, TimeoutHandler timeoutHandler) {
      throw new UnsupportedOperationException("suspend is not supported on this container.");
    }

    @Override
    public OutputStream writeResponseStatusAndHeaders(long contentLength, ContainerResponse context)
        throws ContainerException {
      MultivaluedMap<String, String> responseHeaders = context.getStringHeaders();
      HttpHeaders nettyHeaders = response.headers();

      for (Entry<String, List<String>> entry: responseHeaders.entrySet()) {
        nettyHeaders.add(entry.getKey(), entry.getValue());
      }

      int status = context.getStatus();

      response.setStatus(HttpResponseStatus.valueOf(status));
      return new ByteBufOutputStream(response.content());
    }

    private void sendLastHttpContent() {
      ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
            .addListener(ChannelFutureListener.CLOSE);
    }
    
    private void releaseConnection() {
      if (closed.compareAndSet(false, true)) {
        String warnMessage = "ResponseWriter did not be commited.";
        LOG.warn(warnMessage);
        failure(new IllegalStateException(warnMessage));
      }
    }

  }

}
