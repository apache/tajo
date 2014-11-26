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

package org.apache.tajo.storage.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.StorageConstants;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class AvroUtil {
  public static Schema getAvroSchema(TableMeta meta, Configuration conf)
      throws IOException {

    boolean isSchemaLiteral = meta.containsOption(StorageConstants.AVRO_SCHEMA_LITERAL);
    boolean isSchemaUrl = meta.containsOption(StorageConstants.AVRO_SCHEMA_URL);
    if (!isSchemaLiteral && !isSchemaUrl) {
      throw new RuntimeException("No Avro schema for table.");
    }
    if (isSchemaLiteral) {
      String schema = meta.getOption(StorageConstants.AVRO_SCHEMA_LITERAL);
      return new Schema.Parser().parse(schema);
    }

    String schemaURL = meta.getOption(StorageConstants.AVRO_SCHEMA_URL);
    if (schemaURL.toLowerCase().startsWith("http")) {
      return getAvroSchemaFromHttp(schemaURL);
    } else {
      return getAvroSchemaFromFileSystem(schemaURL, conf);
    }
  }

  public static Schema getAvroSchemaFromHttp(String schemaURL) throws IOException {
    InputStream inputStream = new URL(schemaURL).openStream();

    try {
      return new Schema.Parser().parse(inputStream);
    } finally {
      IOUtils.closeStream(inputStream);
    }
  }

  public static Schema getAvroSchemaFromFileSystem(String schemaURL, Configuration conf) throws IOException {
    Path schemaPath = new Path(schemaURL);
    FileSystem fs = schemaPath.getFileSystem(conf);
    FSDataInputStream inputStream = fs.open(schemaPath);

    try {
      return new Schema.Parser().parse(inputStream);
    } finally {
      IOUtils.closeStream(inputStream);
    }
  }
}
