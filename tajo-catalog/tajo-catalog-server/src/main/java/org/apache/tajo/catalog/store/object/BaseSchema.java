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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

@XmlAccessorType(XmlAccessType.FIELD)
public class BaseSchema implements Comparable<BaseSchema> {

  @XmlAttribute(name="version",required=true)
  private int version;
  @XmlAttribute(name="schemaname")
  private String schemaName;
  @XmlElementWrapper(name="objects",namespace="http://tajo.apache.org/catalogstore")
  @XmlElement(name="Object",namespace="http://tajo.apache.org/catalogstore")
  private final List<DatabaseObject> objects = new ArrayList<>();
  
  public int getVersion() {
    return version;
  }
  public void setVersion(int version) {
    this.version = version;
  }
  public String getSchemaName() {
    return schemaName;
  }
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }
  public List<DatabaseObject> getObjects() {
    return objects;
  }
  public void addObject(DatabaseObject object) {
    this.objects.add(object);
  }
  public void addObjects(Collection<DatabaseObject> objects) {
    this.objects.addAll(objects);
  }
  public void clearObjects() {
    this.objects.clear();
  }
  
  @Override
  public int compareTo(BaseSchema o) {
    return (int) Math.signum(getVersion()-o.getVersion());
  }
  @Override
  public String toString() {
    return "BaseSchema [version=" + version + ", schemaName=" + schemaName + ", objects=" + objects + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BaseSchema that = (BaseSchema) o;

    if (version != that.version) return false;
    if (objects != null ? !objects.equals(that.objects) : that.objects != null) return false;
    if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = version;
    result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
    result = 31 * result + (objects != null ? objects.hashCode() : 0);
    return result;
  }
}
