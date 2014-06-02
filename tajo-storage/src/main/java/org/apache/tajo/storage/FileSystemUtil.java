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

package org.apache.tajo.storage;


import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;

public class FileSystemUtil {
  private static final Log LOG = LogFactory.getLog(FileSystemUtil.class);

  /**
   * Returns instance of DistributedFileSystem object pointing to secure hadoop using
   * hadoop delegation token.
   * @param path Path for which fileSystem is requested
   * @param systemConf TajoConf
   * @param rootUriParam URI string param pointing to root of filesytem
   * @return Returns filesystem object
   *
   */
  private static FileSystem getDFSUsingDelegationToken(Path path, TajoConf systemConf,
                                                       String rootUriParam) throws Exception {
    String delegationToken = systemConf.getVar(ConfVars.HADOOP_DFS_DELEGATION_TOKEN);
    LOG.info("Delegation token :" + delegationToken);
    if(delegationToken.equals("null"))
      throw new Exception("Hadoop DFS delegationToken is null, It should have been set in TajoMaster");

    Token<?> dfsToken =
      new Token<org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier>();
    dfsToken.decodeFromUrlString(delegationToken);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    Configuration conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf );
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);

    if(ugi.addToken(dfsToken))
      LOG.info("DFS Token added to hadoop usergroup information");
    final String kerberosPrincipal = systemConf.getVar(ConfVars.HADOOP_DFS_NAMENODE_KERBEROS_PRINCIPAL);
    if(kerberosPrincipal.equals("null"))
      throw new Exception("Wrong value for "+ConfVars.HADOOP_DFS_NAMENODE_KERBEROS_PRINCIPAL+" : null");

    final String rootUri = rootUriParam;
    FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          Configuration conf = new HdfsConfiguration();
          conf.set("fs.defaultFs", rootUri);
          conf.set("dfs.namenode.kerberos.principal",kerberosPrincipal);
          return FileSystem.get(conf);
        }
      });
    return fs;
  }

  /**
   * Returns instance of DistributedFileSystem object of secure hadoop cluster using
   * user principal and keytab file.
   *
   * @param path Path for which fileSystem is requested
   * @param systemConf TajoConf
   * @param rootUriParam URI string param pointing to root of filesytem
   * @return Returns filesystem object
   * @throws Exception Throws exception for wrong config values.
   */
  private static FileSystem getDFSUsingKeyTab(Path path, TajoConf systemConf,
                                              String rootUriParam) throws Exception {
    Configuration conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf );
    UserGroupInformation.setConfiguration(conf);
    final String kerberosPrincipal = systemConf.getVar(ConfVars.HADOOP_DFS_NAMENODE_KERBEROS_PRINCIPAL);
    final String kerberosKeyTabLocation = systemConf.getVar(ConfVars.HADOOP_DFS_NAMENODE_KERBEROS_KEYTAB_LOC);
    if(kerberosPrincipal.equals("null"))
      throw new Exception("Wrong value for "+ConfVars.HADOOP_DFS_NAMENODE_KERBEROS_PRINCIPAL+" : null");
    if(kerberosKeyTabLocation.equals("null"))
      throw new Exception("Wrong value for "+ConfVars.HADOOP_DFS_NAMENODE_KERBEROS_KEYTAB_LOC+" : null");
    UserGroupInformation ugi = UserGroupInformation.
      loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeyTabLocation);
    final String rootUri = rootUriParam;
    FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          Configuration conf = new HdfsConfiguration();
          conf.set("fs.defaultFs", rootUri);
          conf.set("dfs.namenode.kerberos.principal",kerberosPrincipal);
          return FileSystem.get(conf);
        }
      });
    Token<?> dfsToken = fs.getDelegationToken(ugi.getShortUserName());
    String dfsTokenString = dfsToken.encodeToUrlString();
    systemConf.setVar(ConfVars.HADOOP_DFS_DELEGATION_TOKEN, dfsTokenString);
    return fs;
  }

  /**
   * This method is helper method to be called from worker code.
   *
   */
  public static FileSystem getFileSystem(Path path, TajoConf systemConf
                                         ) throws Exception {
    return getFileSystem(path, systemConf, false);
  }

  /**
   * Returns file system object from the path.
   * Does the authentication with underlying filesystem if it is required
   * (Eg. Hadoop kerberose/tokens)
   *
   * @param path Input path for which FileSystem object is requested
   * @param systemConf TajoConf object
   * @param callFromTajoMaster boolean value indicating if this call is being made form master
   * @return Returns filesystem object corresponding to input path
   *
   */
  public static FileSystem getFileSystem(Path path, TajoConf systemConf,
                                         boolean callFromTajoMaster
                                         ) throws Exception {
    path = new Path(path.toString().toLowerCase());
    if(path.toUri().getScheme() != null && path.toUri().getScheme().equals("hdfs")) {
      String hdfsSecurtyType = systemConf.getVar(ConfVars.HADOOP_SECURTY_AUTH_TYPE);
      LOG.info("HDFS securty type "+ hdfsSecurtyType);
      if(hdfsSecurtyType.equals("simple")) {
        return path.getFileSystem(systemConf);
      }
      if(hdfsSecurtyType.equals("kerberos") == false) {
        throw new Exception("Unsupported value for " + ConfVars.HADOOP_SECURTY_AUTH_TYPE +
                            " , Supported values : kerberos, simple");
      }

      if(path.toUri().getPort() == -1) {
        throw new Exception("Port missing in hdfs path :"+path);
      }
      if(path.toUri().getHost() == null) {
        throw new Exception("Host missing/malformed in hdfs path :" + path);
      }
      String rootUri = path.toUri().getScheme() + "://" + path.toUri().getHost() + ":"
        + path.toUri().getPort();
      if(callFromTajoMaster)
        return getDFSUsingKeyTab(path, systemConf, rootUri);
      return getDFSUsingDelegationToken(path, systemConf, rootUri);
    }
    return path.getFileSystem(systemConf);
  }
}
