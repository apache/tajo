package tajo.util;

import com.maxmind.geoip.LookupService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;

import java.io.IOException;

public class GeoUtil {
  private static final Log LOG = LogFactory.getLog(GeoUtil.class);
  private static LookupService lookup;

  static {
    try {
      TajoConf conf = new TajoConf();
      lookup = new LookupService(conf.getVar(ConfVars.GEOIP_DATA),
          LookupService.GEOIP_MEMORY_CACHE);
    } catch (IOException e) {
      LOG.error("Cannot open the geoip data", e);
    }
  }

  public static String getCountryCode(String host) {
    return lookup.getCountry(host).getCode();
  }
}
