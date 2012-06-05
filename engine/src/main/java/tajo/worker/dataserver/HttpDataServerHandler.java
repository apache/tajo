package tajo.worker.dataserver;

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelFutureProgressListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.FileRegion;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedFile;
import org.jboss.netty.util.CharsetUtil;

import tajo.worker.dataserver.retriever.DataRetriever;
import tajo.worker.dataserver.retriever.FileChunk;

/**
 * @author Hyunsik Choi
 */
public class HttpDataServerHandler extends SimpleChannelUpstreamHandler {
  private final static Log LOG = LogFactory.getLog(HttpDataServer.class);
  private final DataRetriever retriever;

  public HttpDataServerHandler(DataRetriever retriever) {
    this.retriever = retriever;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception {
    HttpRequest request = (HttpRequest) e.getMessage();
    if (request.getMethod() != GET) {
      sendError(ctx, METHOD_NOT_ALLOWED);
      return;
    }

    FileChunk file;
    try {
      file = retriever.handle(ctx, request);
    } catch (FileNotFoundException fnf) {
      LOG.error(fnf);
      sendError(ctx, NOT_FOUND);
      return;
    } catch (IllegalArgumentException iae) {
      LOG.error(iae);
      sendError(ctx, BAD_REQUEST);
      return;
    } catch (FileAccessForbiddenException fafe) {
      LOG.error(fafe);
      sendError(ctx, FORBIDDEN);
      return;
    } catch (IOException ioe) {
      LOG.error(ioe);
      sendError(ctx, INTERNAL_SERVER_ERROR);
      return;
    }
    
    LOG.info("GET " + file.getFile().getAbsolutePath());

    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(file.getFile(), "r");
    } catch (FileNotFoundException fnfe) {
      sendError(ctx, NOT_FOUND);
      return;
    }
    long fileLength = file.length();

    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
    setContentLength(response, fileLength);

    Channel ch = e.getChannel();

    // Write the initial line and the header.
    ch.write(response);
    // Write the content.
    ChannelFuture writeFuture;
    if (ch.getPipeline().get(SslHandler.class) != null) {
      // Cannot use zero-copy with HTTPS.
      writeFuture = ch.write(new ChunkedFile(raf, 0, fileLength, 8192));
    } else {
      // No encryption - use zero-copy.
      final FileRegion region = new DefaultFileRegion(raf.getChannel(), 0,
          fileLength);
      writeFuture = ch.write(region);
      writeFuture.addListener(new ChannelFutureProgressListener() {
        public void operationComplete(ChannelFuture future) {
          region.releaseExternalResources();
        }

        public void operationProgressed(ChannelFuture future, long amount,
            long current, long total) {
          System.out.printf("%s: %d / %d (+%d)%n", "file", current, total,
              amount);
        }
      });
    }

    // Decide whether to close the connection or not.
    if (!isKeepAlive(request)) {
      // Close the connection when the whole content is written out.
      writeFuture.addListener(ChannelFutureListener.CLOSE);
    }
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
}
