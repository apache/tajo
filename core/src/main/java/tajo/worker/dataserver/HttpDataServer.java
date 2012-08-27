package tajo.worker.dataserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import tajo.worker.dataserver.retriever.DataRetriever;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * @author Hyunsik Choi
 */
public class HttpDataServer {
  private final static Log LOG = LogFactory.getLog(HttpDataServer.class);

  private final InetSocketAddress addr;
  private InetSocketAddress bindAddr;
  private ServerBootstrap bootstrap = null;
  private ChannelFactory factory = null;
  private ChannelGroup channelGroup = null;

  public HttpDataServer(final InetSocketAddress addr, 
      final DataRetriever retriever) {
    this.addr = addr;
    this.factory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool(),
        Runtime.getRuntime().availableProcessors() * 2);

    // Configure the server.
    this.bootstrap = new ServerBootstrap(factory);
    // Set up the event pipeline factory.
    this.bootstrap.setPipelineFactory(
        new HttpDataServerPipelineFactory(retriever));    
    this.channelGroup = new DefaultChannelGroup();
  }

  public HttpDataServer(String bindaddr, DataRetriever retriever) {
    this(NetUtils.createSocketAddr(bindaddr), retriever);
  }

  public void start() {
    // Bind and start to accept incoming connections.
    Channel channel = bootstrap.bind(addr);
    channelGroup.add(channel);    
    this.bindAddr = (InetSocketAddress) channel.getLocalAddress();
    LOG.info("HttpDataServer starts up ("
        + this.bindAddr.getAddress().getHostAddress() + ":" + this.bindAddr.getPort()
        + ")");
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }

  public void stop() {
    ChannelGroupFuture future = channelGroup.close();
    future.awaitUninterruptibly();
    factory.releaseExternalResources();

    LOG.info("HttpDataServer shutdown ("
        + this.bindAddr.getAddress().getHostAddress() + ":"
        + this.bindAddr.getPort() + ")");
  }
}
