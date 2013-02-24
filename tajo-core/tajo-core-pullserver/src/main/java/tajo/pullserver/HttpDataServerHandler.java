/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.pullserver;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedFile;
import org.jboss.netty.util.CharsetUtil;
import tajo.SubQueryId;
import tajo.pullserver.retriever.DataRetriever;
import tajo.pullserver.retriever.FileChunk;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * @author Hyunsik Choi
 */
public class HttpDataServerHandler extends SimpleChannelUpstreamHandler {
  private final static Log LOG = LogFactory.getLog(HttpDataServerHandler.class);

  Map<SubQueryId, DataRetriever> retrievers = new ConcurrentHashMap<SubQueryId, DataRetriever>();
  private String userName;
  private String appId;

  public HttpDataServerHandler(String userName, String appId) {
    this.userName= userName;
    this.appId = appId;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception {
    HttpRequest request = (HttpRequest) e.getMessage();
    if (request.getMethod() != GET) {
      sendError(ctx, METHOD_NOT_ALLOWED);
      return;
    }

    String base =
        ContainerLocalizer.USERCACHE + "/" + userName + "/"
            + ContainerLocalizer.APPCACHE + "/"
            + appId + "/output" + "/";

    final Map<String, List<String>> params =
        new QueryStringDecoder(request.getUri()).getParameters();

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
//    try {
//      file = retriever.handle(ctx, request);
//    } catch (FileNotFoundException fnf) {
//      LOG.error(fnf);
//      sendError(ctx, NOT_FOUND);
//      return;
//    } catch (IllegalArgumentException iae) {
//      LOG.error(iae);
//      sendError(ctx, BAD_REQUEST);
//      return;
//    } catch (FileAccessForbiddenException fafe) {
//      LOG.error(fafe);
//      sendError(ctx, FORBIDDEN);
//      return;
//    } catch (IOException ioe) {
//      LOG.error(ioe);
//      sendError(ctx, INTERNAL_SERVER_ERROR);
//      return;
//    }

    // Write the content.
    Channel ch = e.getChannel();
    if (file == null) {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NO_CONTENT);
      ch.write(response);
      if (!isKeepAlive(request)) {
        ch.close();
      }
    }  else {
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
        writeFuture = sendFile(ctx, ch, chunk);
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
                                 FileChunk file) throws IOException {
    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(file.getFile(), "r");
    } catch (FileNotFoundException fnfe) {
      return null;
    }

    ChannelFuture writeFuture;
    if (ch.getPipeline().get(SslHandler.class) != null) {
      // Cannot use zero-copy with HTTPS.
      writeFuture = ch.write(new ChunkedFile(raf, file.startOffset(),
          file.length(), 8192));
    } else {
      // No encryption - use zero-copy.
      final FileRegion region = new DefaultFileRegion(raf.getChannel(),
          file.startOffset(), file.length());
      writeFuture = ch.write(region);
      writeFuture.addListener(new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
          region.releaseExternalResources();
        }
      });
    }

    return writeFuture;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    Channel ch = e.getChannel();
    Throwable cause = e.getCause();
    if (cause instanceof TooLongFrameException) {
      sendError(ctx, BAD_REQUEST);
      return;
    }

    cause.printStackTrace();
    if (ch.isConnected()) {
      sendError(ctx, INTERNAL_SERVER_ERROR);
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
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
    response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
    response.setContent(ChannelBuffers.copiedBuffer(
        "Failure: " + status.toString() + "\r\n", CharsetUtil.UTF_8));

    // Close the connection as soon as the error message is sent.
    ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
  }

  private List<String> splitMaps(List<String> qids) {
    if (null == qids) {
      LOG.error("QueryUnitId is EMPTY");
      return null;
    }

    final List<String> ret = new ArrayList<String>();
    for (String qid : qids) {
      Collections.addAll(ret, qid.split(","));
    }
    return ret;
  }
}
