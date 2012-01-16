package nta.engine;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.catalog.TableDescImpl;
import nta.engine.ipc.QueryEngineInterface;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;

public class NtaClient {
	private Configuration conf = null;
	private QueryEngineInterface protocol = null;
	
	public NtaClient(String ip, int port) {
		this(new Configuration(), ip, port);
	}
	
	public NtaClient(Configuration conf, String ip, int port) {
		init(conf, ip, port);
	}
	
	/**
	 * NTA 마스터 생성 및 초기화
	 * 
	 * @param conf
	 */
	public void init(Configuration conf, String ip, int port) {
		this.conf = conf;
		InetSocketAddress addr = new InetSocketAddress(ip, port);
		try {
			this.protocol = (QueryEngineInterface) RPC.waitForProxy(QueryEngineInterface.class, 0l, addr, conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * NTA 마스터 종료
	 */
	public void close() {
	}
	
	public ResultSetWritable executeQuery(String query) {
		return protocol.executeQuery(query);
	}
	
	public QueryResponse executeQueryAsync(String query) {
		return protocol.executeQueryAsync(query);
	}
	
	public void createTable(TableDescImpl meta) {
		protocol.createTable(meta);
	}
	
	public void dropTable(String name) {
		protocol.dropTable(name);
	}
	
	public void attachTable(String name, Path path) {
		protocol.attachTable(name, path);
	}
	
	public void detachTable(String name) {
		protocol.detachTable(name);
	}
	
	public boolean existsTable(String name) {
		return protocol.existsTable(name);
	}
}
