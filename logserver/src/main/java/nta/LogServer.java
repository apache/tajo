package nta;
import java.io.IOException;

import nta.conf.NtaConf;
import nta.rpc.LogRPCProtocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class LogServer implements LogRPCProtocol {
	private static final Log LOG = LogFactory.getLog(LogServer.class);

	private final NtaConf conf;
	private FileSystem fs = null;
	private final Path logDir;
	private FileStatus[] filelist;
	private FSDataInputStream in;
	private BytesWritable value;
	private byte[] reader;
	private int index = 0;
	private Server server;
	
	public static final String LOG_DIR="logserver.dir";
	public static final String LOG_BIND_ADDR="logserver.addr";
	public static final String DEFAULT_LOGSERVER_BIND_ADDR="localhost";
	public static final String LOG_BIND_PORT="logserver.port";
	public static final int DEFAULT_BIND_PORT=22222;
	public final String bindAddr;
	public final int bindPort;
	
	public LogServer(NtaConf conf) {
		this.conf = conf;
		this.logDir = new Path(conf.get(LOG_DIR, "/logdir"));
		this.bindAddr = conf.get(LOG_BIND_ADDR, DEFAULT_LOGSERVER_BIND_ADDR);
		this.bindPort = conf.getInt(LOG_BIND_PORT, DEFAULT_BIND_PORT);
	}
	
	public void init() throws IOException {
		fs = FileSystem.get(conf);
		this.server = RPC.getServer(this, bindAddr, bindPort, conf);
		LOG.info("LogServer is initialized");
	}
	
	public void start() {
		this.server.start();
	}
	
	public void stop() {
		this.server.stop();
	}
	
	public String getBindAddress() {
		return this.bindAddr;
	}
	
	public int getBindPort() {
		return this.bindPort;
	}
	
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {	
		return versionID;
	}

	public BytesWritable getLogs() throws IOException {
		filelist = fs.listStatus(logDir);
		
		if(index >= filelist.length)
			index = 0;		
		
		in = new FSDataInputStream(fs.open(filelist[index].getPath()));
		reader = new byte[(int)filelist[index].getLen()];
		in.readFully(0, reader);
		
		value = new BytesWritable();
		value.set(reader, 0, reader.length);
		in.close();

		index++;
		
		LOG.info("Called getLogs()");
		return value;
	}

	public static void main(String[] args) throws Exception {
		NtaConf conf = new NtaConf();
		conf.set(LOG_DIR, args[0]);
		conf.set(LOG_BIND_ADDR, args[1]);
		LogServer s = new LogServer(conf);

		s.init();
		s.start();
	}
}