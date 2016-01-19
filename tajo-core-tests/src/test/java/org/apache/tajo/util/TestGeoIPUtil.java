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

package org.apache.tajo.util;

import static org.junit.Assert.assertNotNull;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import com.maxmind.geoip.LookupService;

public class TestGeoIPUtil {
  @Test
  public void testCountryCode() throws Exception {
    new GeoIPUtil(); // initialize static field.

    LookupService lookup = new LookupService(getGeoIpData().getPath(), LookupService.GEOIP_MEMORY_CACHE);
    System.out.println(GeoIPUtil.class.getDeclaredField("LOG"));
    Field lookupField = GeoIPUtil.class.getDeclaredField("lookup");
    lookupField.setAccessible(true);
    lookupField.set(null, lookup);
    assertNotNull(GeoIPUtil.getCountryCode("154.73.88.82"));
    assertNotNull(GeoIPUtil.getCountryCode("154.73.89.143"));
    assertNotNull(GeoIPUtil.getCountryCode("154.76.101.156"));
  }

  private File getGeoIpData() throws Exception {
    File tmpFile = File.createTempFile("GeoIP", ".dat");

    try (InputStream is = new URL("http://www.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz")
        .openConnection().getInputStream();
        OutputStream out = new FileOutputStream(tmpFile)) {
      IOUtils.copy(new GZIPInputStream(is), out);
    }
    return tmpFile;
  }
}
