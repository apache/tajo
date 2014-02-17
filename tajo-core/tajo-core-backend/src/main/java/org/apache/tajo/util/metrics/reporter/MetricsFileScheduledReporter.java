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

package org.apache.tajo.util.metrics.reporter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class MetricsFileScheduledReporter extends MetricsStreamScheduledReporter {
  private static final Log LOG = LogFactory.getLog(MetricsFileScheduledReporter.class);
  public static final String REPORTER_NAME = "file";

  protected String getReporterName() {
    return REPORTER_NAME;
  }

  @Override
  protected void afterInit() {
    String fileName = metricsProperties.get(metricsPropertyKey + "filename");
    if(fileName == null) {
      LOG.warn("No " + metricsPropertyKey + "filename property in tajo-metrics.properties");
      return;
    }
    try {
      File file = new File(fileName);
      File parentFile = file.getParentFile();
      if(parentFile != null && !parentFile.exists()) {
        if(!parentFile.mkdirs()) {
          LOG.warn("Can't create dir for tajo metrics:" + parentFile.getAbsolutePath());
        }
      }
      this.setOutput(new FileOutputStream(fileName, true));
      this.setDateFormat(null);
    } catch (FileNotFoundException e) {
      LOG.warn("Can't open metrics file:" + fileName);
      this.close();
    }
  }
}
