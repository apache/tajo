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

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import static org.apache.tajo.storage.s3.TajoS3Constants.ACCESS_KEY;
import static org.apache.tajo.storage.s3.TajoS3Constants.SECRET_KEY;

/**
 * <p>
 * Extracts AWS credentials from the filesystem URI or configuration. (borrowed from hadoop-aws package)
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TajoS3Credentials {

  private String accessKey;
  private String secretAccessKey;

  /**
   * @throws IllegalArgumentException if credentials for S3 cannot be
   * determined.
   */
  public void initialize(URI uri, Configuration conf) {
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("Invalid hostname in URI " + uri);
    }

    String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      int index = userInfo.indexOf(':');
      if (index != -1) {
        accessKey = userInfo.substring(0, index);
        secretAccessKey = userInfo.substring(index + 1);
      } else {
        accessKey = userInfo;
      }
    }

    String scheme = uri.getScheme();
    String accessKeyProperty = scheme.startsWith("s3a") ? ACCESS_KEY : String.format("fs.%s.awsAccessKeyId", scheme);
    String secretAccessKeyProperty = scheme.startsWith("s3a") ? SECRET_KEY : String.format("fs.%s.awsSecretAccessKey",
      scheme);

    if (accessKey == null) {
      accessKey = conf.getTrimmed(accessKeyProperty);
    }
    if (secretAccessKey == null) {
      secretAccessKey = conf.getTrimmed(secretAccessKeyProperty);
    }
    if (accessKey == null && secretAccessKey == null) {
      throw new IllegalArgumentException("AWS Access Key ID and Secret Access Key must be specified as the " +
        "username or password (respectively) of a " + scheme + " URL, or by setting the " +
        accessKeyProperty + " or " + secretAccessKeyProperty + " properties (respectively).");
    } else if (accessKey == null) {
      throw new IllegalArgumentException("AWS Access Key ID must be specified as the username of a " + scheme +
        " URL, or by setting the " + accessKeyProperty + " property.");
    } else if (secretAccessKey == null) {
      throw new IllegalArgumentException("AWS Secret Access Key must be specified as the password of a " +
        scheme + " URL, or by setting the " + secretAccessKeyProperty + " property.");
    }

  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }
}