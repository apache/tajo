package nta;

import java.io.IOException;
import java.util.Timer;

import org.apache.hadoop.conf.Configuration;

import nta.conf.NtaConf;
import nta.engine.NtaEngineInterface;


public class ETL {
	private static final long DEFAULT_FIRST_DELAY = 100;
	private static final long DEFAULT_RECV_INTERVAL = 10000;

	private TableUpdator receiver;
	private Timer scheduler;
	
	public ETL(NtaConf conf, NtaEngineInterface engine) throws IOException {
		receiver = new TableUpdator(conf, engine);
		scheduler = new Timer();
	}
	
	public void start() {
		scheduler.scheduleAtFixedRate(receiver, ETL.DEFAULT_FIRST_DELAY, ETL.DEFAULT_RECV_INTERVAL);
	}

	public void start(long firstDelay, long recvInterval) {
		scheduler.scheduleAtFixedRate(receiver, firstDelay, recvInterval);
	}
	
	public void stop() {
		scheduler.cancel();
	}	
}
