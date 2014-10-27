/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.conf;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.validation.ConstraintViolationException;
import org.junit.Before;
import org.junit.Test;

public class TestValidatorsForTajoConf {
  
  @Before
  public void setUp() throws Exception {
    CopyOnWriteArrayList<String> defaultResources = new CopyOnWriteArrayList<String>(
        new String[] {"catalog-default.xml", "catalog-site.xml",
            "storage-default.xml", "storage-site.xml",
            "tajo-default.xml", "tajo-site.xml"});
    Field defaultResourcesField = Configuration.class.getDeclaredField("defaultResources");
    defaultResourcesField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(defaultResourcesField, defaultResourcesField.getModifiers() & ~Modifier.FINAL);
    defaultResourcesField.set(null, defaultResources);
  }

  @Test(expected=ConstraintViolationException.class)
  public void testPathValidatorsForTajoConf_setVar() {
    TajoConf conf = new TajoConf();
    
    TajoConf.setVar(conf, ConfVars.ROOT_DIR, "Invalid path");
    fail("ConstraintViolationException is expected but it is not occurred.");
  }
  
  @Test
  public void testValidPathForTajoConf() {
    TajoConf conf = new TajoConf();
    
    try {
      TajoConf.setVar(conf, ConfVars.ROOT_DIR, "file:///tmp/tajo-${user.name}/");
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected ConstraintViolationException has been occurred.");
    }
  }
  
  @Test(expected=ConstraintViolationException.class)
  public void testAddDefaultResourceValidation1() {
    TajoConf.addDefaultResource("org/apache/tajo/conf/InvalidConf1.xml");
    TajoConf conf = new TajoConf();
    
    conf.getVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
    fail("Validation check has failed");
  }
  
  @Test(expected=ConstraintViolationException.class)
  public void testAddDefaultResourceValidation2() {
    TajoConf.addDefaultResource("org/apache/tajo/conf/InvalidConf2.xml");
    TajoConf conf = new TajoConf();
    
    conf.getBoolVar(ConfVars.TAJO_MASTER_HA_ENABLE);
    fail("Validation check has failed");
  }
  
  @Test(expected=ConstraintViolationException.class)
  public void testSetConf1() {
    TajoConf conf = new TajoConf();
    
    conf.setVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS, "9440");
    fail("Validation check has failed");
  }
  
  @Test(expected=ConstraintViolationException.class)
  public void testSetConf2() {
    TajoConf conf = new TajoConf();
    
    conf.set(ConfVars.TAJO_MASTER_HA_ENABLE.keyname(), "yes");
    fail("Validation check has failed");
  }

}
