package nta.rpc;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class NettyClientBase {
  private static Log LOG = LogFactory.getLog(NettyClientBase.class);

  private ClientSocketChannelFactory factory;
  private ClientBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private Channel channel;
  private InetSocketAddress addr;

  public NettyClientBase() {
  }

  public NettyClientBase(InetSocketAddress addr,
      ChannelPipelineFactory pipeFactory) {
    init(addr, pipeFactory);
  }

  public void init(InetSocketAddress addr, ChannelPipelineFactory pipeFactory) {
    this.addr = addr;

    this.factory =
        new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());

    this.bootstrap = new ClientBootstrap(factory);
    this.bootstrap.setPipelineFactory(pipeFactory);
    // TODO - should be configurable
    this.bootstrap.setOption("connectTimeoutMillis", 10000);
    this.bootstrap.setOption("connectResponseTimeoutMillis", 10000);
    this.bootstrap.setOption("receiveBufferSize", 1048576);
    this.bootstrap.setOption("tcpNoDelay", false);
    this.bootstrap.setOption("keepAlive", true);

    this.channelFuture = bootstrap.connect(addr);
    this.channelFuture.awaitUninterruptibly();
    if (!channelFuture.isSuccess()) {
      channelFuture.getCause().printStackTrace();
      throw new RuntimeException(channelFuture.getCause());
    }
    this.channel = channelFuture.getChannel();
  }

  public InetSocketAddress getAddress() {
    return this.addr;
  }

  public Channel getChannel() {
    return this.channel;
  }

  public void shutdown() {
    this.channel.close().awaitUninterruptibly();
    this.bootstrap.releaseExternalResources();
  }
}
