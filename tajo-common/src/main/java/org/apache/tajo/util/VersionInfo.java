package org.apache.tajo.util;

/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ClassUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class returns build information about Tajo components.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class VersionInfo {
  private static final Log LOG = LogFactory.getLog(VersionInfo.class);

  private Properties info;

  protected VersionInfo(String component) {
    info = new Properties();
    String versionInfoFile = component + "-version-info.properties";
    try {
      InputStream is = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(versionInfoFile);
      if (is == null) {
        throw new IOException("Resource not found");
      }
      info.load(is);
    } catch (IOException ex) {
      LogFactory.getLog(getClass()).warn("Could not read '" +
          versionInfoFile + "', " + ex.toString(), ex);
    }
  }

  protected String _getVersion() {
    return info.getProperty("version", "Unknown");
  }

  protected String _getRevision() {
    return info.getProperty("revision", "Unknown");
  }

  protected String _getBranch() {
    return info.getProperty("branch", "Unknown");
  }

  protected String _getDate() {
    return info.getProperty("date", "Unknown");
  }

  protected String _getUser() {
    return info.getProperty("user", "Unknown");
  }

  protected String _getUrl() {
    return info.getProperty("url", "Unknown");
  }

  protected String _getSrcChecksum() {
    return info.getProperty("srcChecksum", "Unknown");
  }

  protected String _getBuildVersion(){
    return getVersion() +
        " from " + _getRevision() +
        " by " + _getUser() +
        " source checksum " + _getSrcChecksum();
  }

  protected String _getProtocVersion() {
    return info.getProperty("protocVersion", "Unknown");
  }

  private static VersionInfo TAJO_VERSION_INFO = new VersionInfo("tajo");
  /**
   * Get the Tajo version.
   * @return the Tajo version string, eg. "0.9.0-SNAPSHOT"
   */
  public static String getVersion() {
    return TAJO_VERSION_INFO._getVersion();
  }

  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return TAJO_VERSION_INFO._getRevision();
  }

  /**
   * Get the branch on which this originated.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  public static String getBranch() {
    return TAJO_VERSION_INFO._getBranch();
  }

  /**
   * The date that Tajo was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return TAJO_VERSION_INFO._getDate();
  }

  /**
   * The user that compiled Tajo.
   * @return the username of the user
   */
  public static String getUser() {
    return TAJO_VERSION_INFO._getUser();
  }

  /**
   * Get the git repository URL for the root Tajo directory.
   */
  public static String getUrl() {
    return TAJO_VERSION_INFO._getUrl();
  }

  /**
   * Get the checksum of the source files from which Tajo was
   * built.
   **/
  public static String getSrcChecksum() {
    return TAJO_VERSION_INFO._getSrcChecksum();
  }

  /**
   * Returns the buildVersion which includes version,
   * revision, user and date.
   */
  public static String getBuildVersion(){
    return TAJO_VERSION_INFO._getBuildVersion();
  }

  /**
   * Returns the protoc version used for the build.
   */
  public static String getProtocVersion(){
    return TAJO_VERSION_INFO._getProtocVersion();
  }

  /**
   * Returns the display version including all information.
   * @return
   */
  public static String getDisplayVersion() {
    StringBuilder displayVersion = new StringBuilder("Tajo ")
        .append(VersionInfo.getVersion()).append(" (")
        .append("rev. " + VersionInfo.getRevision().substring(0, 7))
        .append(" source checksum ").append(VersionInfo.getSrcChecksum().substring(0, 7))
        .append(" compiled by ").append(VersionInfo.getUser()).append(" ")
        .append(VersionInfo.getDate()).append(")");
    return displayVersion.toString();
  }

  public static void main(String[] args) {
    LOG.debug("version: "+ getVersion());
    System.out.println("Tajo " + getVersion());
    System.out.println("Git " + getUrl() + " -r " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("Compiled with protoc " + getProtocVersion());
    System.out.println("From source with checksum " + getSrcChecksum());
    System.out.println("This command was run using " + ClassUtil.findContainingJar(VersionInfo.class));
  }
}

