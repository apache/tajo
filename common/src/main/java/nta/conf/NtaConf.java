package nta.conf;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class NtaConf extends Configuration {
	
  static {
    NtaConf.addDefaultResource("hdfs-default.xml");
    NtaConf.addDefaultResource("hdfs-site.xml");
    NtaConf.addDefaultResource("tajo-default.xml");
    NtaConf.addDefaultResource("tajo-site.xml");
  }
	
	public NtaConf() {
		super();
	}
	
	public NtaConf(Configuration conf) {
		super(conf);
	}
	
  public static NtaConf create() {
    return new NtaConf();
  }

  public static NtaConf create(Configuration conf) {
    return new NtaConf(conf);
  }
}
