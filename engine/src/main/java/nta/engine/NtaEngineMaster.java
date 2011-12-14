/**
 * 
 */
package nta.engine;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.conf.NtaConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;

/**
 * @author hyunsik
 *
 */
public class NtaEngineMaster extends Thread {
	private static final Log LOG = LogFactory.getLog(NtaEngineMaster.class);	

	private final Configuration conf;

	private final InetSocketAddress isa;

	private volatile boolean stopped = false;
	
	private final String serverName;

	public NtaEngineMaster(final Configuration conf) throws IOException {
		this.conf = conf;

		// Server to handle client requests.
		String hostname = DNS.getDefaultHost(
			conf.get("nta.master.dns.interface", "default"),
			conf.get("nta.master.dns.nameserver", "default"));
		int port = conf.getInt(NtaEngineConstants.MASTER_PORT, NtaEngineConstants.MASTER_PORT_DEFAULT);
		// Creation of a HSA will force a resolve.
		InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
		
		this.isa = initialIsa;
		
		this.serverName = this.isa.getHostName()+":"+this.isa.getPort();		
	}
	
	public void run() {
		LOG.info("NtaEngineMaster startup");
		try {
			if(!this.stopped) {
				while(!this.stopped) {					
					Thread.sleep(1000);
					
				}
			}	
		} catch (Throwable t) {
			LOG.fatal("Unhandled exception. Starting shutdown.", t);
		} finally {
			// TODO - adds code to stop all services and clean resources 
		}
		
		LOG.info("NtaEngineMaster main thread exiting");
	}
	
	public String getServerName() {
		return this.serverName;
	}	
	
	public boolean isMasterRunning() {
		return !this.stopped;
	}
	
	public static void main(String [] args) throws IOException {
		NtaConf conf = new NtaConf();
		NtaEngineMaster master = new NtaEngineMaster(conf);
		
		master.start();
	}
}