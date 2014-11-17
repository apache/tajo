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

package org.apache.tajo.storage.elasticsearch;

/**
 * Created by blrunner on 2014. 11. 10..
 */
public class ElasticSearchConstants {
  String ES_HOST = "es.host";
  String ES_NODES = "es.nodes";
  String ES_NODES_DEFAULT = "localhost";
  String ES_NODES_DISCOVERY = "es.nodes.discovery";
  String ES_NODES_DISCOVERY_DEFAULT = "true";
  String ES_PORT = "es.port";
  String ES_PORT_DEFAULT = "9200";
  String ES_RESOURCE = "es.resource";
  String ES_RESOURCE_READ = "es.resource.read";
  String ES_RESOURCE_WRITE = "es.resource.write";
  String ES_QUERY = "es.query";
  String ES_BATCH_SIZE_BYTES = "es.batch.size.bytes";
  String ES_BATCH_SIZE_BYTES_DEFAULT = "1mb";
  String ES_BATCH_SIZE_ENTRIES = "es.batch.size.entries";
  String ES_BATCH_SIZE_ENTRIES_DEFAULT = "1000";
  String ES_BATCH_WRITE_REFRESH = "es.batch.write.refresh";
  String ES_BATCH_WRITE_REFRESH_DEFAULT = "true";
  String ES_BATCH_WRITE_RETRY_COUNT = "es.batch.write.retry.count";
  String ES_BATCH_WRITE_RETRY_COUNT_DEFAULT = "3";
  String ES_BATCH_WRITE_RETRY_WAIT = "es.batch.write.retry.wait";
  String ES_BATCH_WRITE_RETRY_WAIT_DEFAULT = "10s";
  String ES_BATCH_WRITE_RETRY_POLICY = "es.batch.write.retry.policy";
  String ES_BATCH_WRITE_RETRY_POLICY_NONE = "none";
  String ES_BATCH_WRITE_RETRY_POLICY_SIMPLE = "simple";
  String ES_BATCH_WRITE_RETRY_POLICY_DEFAULT = "simple";
  String ES_HTTP_TIMEOUT = "es.http.timeout";
  String ES_HTTP_TIMEOUT_DEFAULT = "1m";
  String ES_HTTP_RETRIES = "es.http.retries";
  String ES_HTTP_RETRIES_DEFAULT = "3";
  String ES_SCROLL_KEEPALIVE = "es.scroll.keepalive";
  String ES_SCROLL_KEEPALIVE_DEFAULT = "5m";
  String ES_SCROLL_SIZE = "es.scroll.size";
  String ES_SCROLL_SIZE_DEFAULT = "50";
  String ES_SCROLL_FIELDS = "es.scroll.fields";
  String ES_HEART_BEAT_LEAD = "es.action.heart.beat.lead";
  String ES_HEART_BEAT_LEAD_DEFAULT = "15s";
  String ES_SERIALIZATION_WRITER_VALUE_CLASS = "es.ser.writer.value.class";
  String ES_SERIALIZATION_WRITER_BYTES_CLASS = "es.ser.writer.bytes.class";
  String ES_SERIALIZATION_READER_VALUE_CLASS = "es.ser.reader.value.class";
  String ES_INPUT_JSON = "es.input.json";
  String ES_INPUT_JSON_DEFAULT = "no";
  String ES_FIELD_READ_EMPTY_AS_NULL = "es.field.read.empty.as.null";
  String ES_FIELD_READ_EMPTY_AS_NULL_DEFAULT = "yes";
  String ES_FIELD_READ_VALIDATE_PRESENCE = "es.field.read.validate.presence";
  String ES_FIELD_READ_VALIDATE_PRESENCE_DEFAULT = "warn";
  String ES_INDEX_AUTO_CREATE = "es.index.auto.create";
  String ES_INDEX_AUTO_CREATE_DEFAULT = "yes";
  String ES_INDEX_READ_MISSING_AS_EMPTY = "es.index.read.missing.as.empty";
  String ES_INDEX_READ_MISSING_AS_EMPTY_DEFAULT = "false";
  String ES_MAPPING_DEFAULT_EXTRACTOR_CLASS = "es.mapping.default.extractor.class";
  String ES_MAPPING_ID = "es.mapping.id";
  String ES_MAPPING_ID_EXTRACTOR_CLASS = "es.mapping.id.extractor.class";
  String ES_MAPPING_PARENT = "es.mapping.parent";
  String ES_MAPPING_PARENT_EXTRACTOR_CLASS = "es.mapping.parent.extractor.class";
  String ES_MAPPING_VERSION = "es.mapping.version";
  String ES_MAPPING_VERSION_EXTRACTOR_CLASS = "es.mapping.version.extractor.class";
  String ES_MAPPING_ROUTING = "es.mapping.routing";
  String ES_MAPPING_ROUTING_EXTRACTOR_CLASS = "es.mapping.routing.extractor.class";
  String ES_MAPPING_TTL = "es.mapping.ttl";
  String ES_MAPPING_TTL_EXTRACTOR_CLASS = "es.mapping.ttl.extractor.class";
  String ES_MAPPING_TIMESTAMP = "es.mapping.timestamp";
  String ES_MAPPING_TIMESTAMP_EXTRACTOR_CLASS = "es.mapping.timestamp.extractor.class";
  String ES_MAPPING_INDEX_EXTRACTOR_CLASS = "es.mapping.index.extractor.class";
  String ES_MAPPING_DEFAULT_INDEX_EXTRACTOR_CLASS;
  String ES_MAPPING_INDEX_FORMATTER_CLASS = "es.mapping.index.formatter.class";
  String ES_MAPPING_DEFAULT_INDEX_FORMATTER_CLASS;
  String ES_MAPPING_PARAMS_EXTRACTOR_CLASS = "es.mapping.params.extractor.class";
  String ES_MAPPING_PARAMS_DEFAULT_EXTRACTOR_CLASS;
  String ES_WRITE_OPERATION = "es.write.operation";
  String ES_OPERATION_INDEX = "index";
  String ES_OPERATION_CREATE = "create";
  String ES_OPERATION_UPDATE = "update";
  String ES_OPERATION_UPSERT = "upsert";
  String ES_OPERATION_DELETE = "delete";
  String ES_WRITE_OPERATION_DEFAULT = "index";
  String ES_UPDATE_RETRY_ON_CONFLICT = "es.update.retry.on.conflict";
  String ES_UPDATE_RETRY_ON_CONFLICT_DEFAULT = "0";
  String ES_UPDATE_SCRIPT = "es.update.script";
  String ES_UPDATE_SCRIPT_LANG = "es.update.script.lang";
  String ES_UPDATE_SCRIPT_PARAMS = "es.update.script.params";
  String ES_UPDATE_SCRIPT_PARAMS_JSON = "es.update.script.params.json";
  String ES_NET_PROXY_HTTP_HOST = "es.net.proxy.http.host";
  String ES_NET_PROXY_HTTP_PORT = "es.net.proxy.http.port";
  String ES_NET_PROXY_HTTP_USER = "es.net.proxy.http.user";
  String ES_NET_PROXY_HTTP_PASS = "es.net.proxy.http.pass";
  String ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS = "es.net.proxy.http.use.system.props";
  String ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS_DEFAULT = "yes";
  String ES_NET_PROXY_SOCKS_HOST = "es.net.proxy.socks.host";
  String ES_NET_PROXY_SOCKS_PORT = "es.net.proxy.socks.port";
  String ES_NET_PROXY_SOCKS_USER = "es.net.proxy.socks.user";
  String ES_NET_PROXY_SOCKS_PASS = "es.net.proxy.socks.pass";
  String ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS = "es.net.proxy.socks.use.system.props";
  String ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS_DEFAULT = "yes";
}
