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

package org.apache.tajo.storage.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;

public class TestConstants {
  private static final String[] TEST_DATA = { "1|abc|0.2", "2|def|0.4", "3|ghi|0.6", "4|jkl|0.8", "5|mno|1.0" };

  static final String TEST_JSON_DATA = "{\"col1\":1, \"col2\":\"abc\", \"col3\":0.2}";
  static final int DEFAULT_TEST_PARTITION_NUM = 3;

  static void sendTestData(Producer<String, String> producer, String topic) throws Exception {
    producer.send(new ProducerRecord<String, String>(topic, TEST_DATA[0]));
    producer.send(new ProducerRecord<String, String>(topic, TEST_DATA[1]));
    producer.send(new ProducerRecord<String, String>(topic, TEST_DATA[2]));
    producer.send(new ProducerRecord<String, String>(topic, TEST_DATA[3]));
    producer.send(new ProducerRecord<String, String>(topic, TEST_DATA[4]));
  }

  static boolean equalTestData(Collection<String> receivedDataSet) {
    if (receivedDataSet.size() != TEST_DATA.length) {
      return false;
    }

    for (String td : TEST_DATA) {
      if (!receivedDataSet.contains(td)) {
        return false;
      }
    }

    return true;
  }
}
