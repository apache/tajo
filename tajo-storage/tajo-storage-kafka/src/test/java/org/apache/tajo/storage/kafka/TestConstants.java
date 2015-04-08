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

public class TestConstants {
  final static int kafka_partition_num = 3;
  final static String test_topic = "test-topic";
  final static String[] test_data = { "1|abc|0.2", "2|def|0.4", "3|ghi|0.6", "4|jkl|0.8", "5|mno|1.0" };
  final static String test_json_data = "{\"col1\":1, \"col2\":\"abc\", \"col3\":0.2}";
}
