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
public class SQLObject {

  @XmlAttribute
  private DatabaseObjectType type;
  @XmlElement(name="sql",namespace="http://tajo.apache.org/catalogstore")
  private String sql;
  
  public DatabaseObjectType getType() {
    return type;
  }
  public void setType(DatabaseObjectType type) {
    this.type = type;
  }
  public String getSql() {
    return sql;
  }
  public void setSql(String sql) {
    this.sql = sql;
  }
  
  @Override
  public String toString() {
    return "ExistQuery [type=" + type + ", sql=" + sql + "]";
  }
  
}
