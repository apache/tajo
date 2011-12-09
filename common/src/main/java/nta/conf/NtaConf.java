package nta.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class NtaConf extends Configuration {
	private static Log LOG = LogFactory.getLog(NtaConf.class);
	
	static {
		NtaConf.addDefaultResource("engine-default.xml");
	}
	
	public NtaConf() {
		super();
	}
	
	public NtaConf(Configuration conf) {
		super(conf);
	}
}
