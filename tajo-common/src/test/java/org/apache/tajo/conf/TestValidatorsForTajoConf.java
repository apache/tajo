package org.apache.tajo.conf;

import static org.junit.Assert.*;

import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.validation.ConstraintViolationException;
import org.junit.Test;

public class TestValidatorsForTajoConf {

  @Test(expected=ConstraintViolationException.class)
  public void testPathValidatorsForTajoConf_setVar() {
    TajoConf conf = new TajoConf();
    
    TajoConf.setVar(conf, ConfVars.ROOT_DIR, "Invalid path");
    fail();
  }
  
  @Test
  public void testValidPathForTajoConf() {
    TajoConf conf = new TajoConf();
    
    TajoConf.setVar(conf, ConfVars.ROOT_DIR, "file:///tmp/tajo-${user.name}/");
  }

}
