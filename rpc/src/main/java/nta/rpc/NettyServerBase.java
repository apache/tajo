package nta.rpc;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class NettyServerBase extends Thread {
  private static final Log LOG = LogFactory.getLog(NettyServerBase.class);

  protected final InetSocketAddress bindAddress;
  protected ChannelFactory factory;
  protected ChannelPipelineFactory pipelineFactory;
  protected ServerBootstrap bootstrap;
  private Channel channel;
  private final ChannelGroup allChannels;

  protected volatile boolean stopped = false;

  public NettyServerBase(InetSocketAddress bindAddr) {
    this.bindAddress = bindAddr;
    this.allChannels = new DefaultChannelGroup();
  }

  public void init(ChannelPipelineFactory pipeline) {
    this.factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());

    pipelineFactory = pipeline;
    bootstrap = new ServerBootstrap(factory);
    bootstrap.setPipelineFactory(pipelineFactory);
    // TODO - should be configurable
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", false);
    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setOption("child.connectTimeoutMillis", 10000);
    bootstrap.setOption("child.connectResponseTimeoutMillis", 10000);
    bootstrap.setOption("child.receiveBufferSize", 1048576);
  }

  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  public void run() {
    LOG.info("RpcServer on " + this.bindAddress);
    this.channel = bootstrap.bind(bindAddress);
    this.allChannels.add(channel);

    while (!this.stopped) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
    }

    LOG.info("RpcServer shutdown");
  }

  public Channel getChannel() {
    return this.channel;
  }

  public void shutdown() {
    this.stopped = true;
    try {
      this.join();
    } catch (InterruptedException e) {
      e.getCause().printStackTrace();
      LOG.error(e.getCause());
    }
  }
}
