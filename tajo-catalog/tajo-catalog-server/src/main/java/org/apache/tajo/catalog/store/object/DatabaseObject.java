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

package org.apache.tajo.catalog.store.object;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
public class DatabaseObject implements Comparable<DatabaseObject> {
  
  @XmlAttribute(name="order")
  private int order=-1;
  @XmlAttribute(name="type",required=true)
  private DatabaseObjectType type;
  @XmlAttribute(name="name",required=true)
  private String name;
  @XmlAttribute(name="dependsOn")
  private String dependsOn;
  @XmlElement(name="sql",namespace="http://tajo.apache.org/catalogstore")
  private String sql;
  
  public int getOrder() {
    return order;
  }
  public void setOrder(int order) {
    this.order = order;
  }
  public DatabaseObjectType getType() {
    return type;
  }
  public void setType(DatabaseObjectType type) {
    this.type = type;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String getDependsOn() {
    return dependsOn;
  }
  public void setDependsOn(String dependsOn) {
    this.dependsOn = dependsOn;
  }
  public String getSql() {
    return sql;
  }
  public void setSql(String sqlStatement) {
    this.sql = sqlStatement;
  }
  @Override
  public String toString() {
    return "DatabaseObject [order=" + order + ", type=" + type + ", name=" + name + ", dependsOn=" + dependsOn
        + ", sql=" + sql + "]";
  }
  @Override
  public int compareTo(DatabaseObject o) {
    return (int) Math.signum(getOrder() - o.getOrder());
  }
  
}
