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

package org.apache.tajo.storage.s3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.*;

public class S3TableSpace extends FileTablespace {
  public S3TableSpace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }
}
