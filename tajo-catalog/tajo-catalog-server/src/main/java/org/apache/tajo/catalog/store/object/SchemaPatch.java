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
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

@XmlAccessorType(XmlAccessType.FIELD)
public class SchemaPatch implements Comparable<SchemaPatch> {

  @XmlAttribute
  private int priorVersion;
  @XmlAttribute
  private int nextVersion;
  @XmlElementWrapper(name="objects",namespace="http://tajo.apache.org/catalogstore")
  @XmlElement(name="Object",namespace="http://tajo.apache.org/catalogstore")
  private final List<DatabaseObject> objects = new ArrayList<>();

  public int getPriorVersion() {
    return priorVersion;
  }
  public void setPriorVersion(int priorVersion) {
    this.priorVersion = priorVersion;
  }
  public int getNextVersion() {
    return nextVersion;
  }
  public void setNextVersion(int nextVersion) {
    this.nextVersion = nextVersion;
  }
  public List<DatabaseObject> getObjects() {
    return objects;
  }
  public void addObject(DatabaseObject object) {
    this.objects.add(object);
  }
  public void addObjects(List<DatabaseObject> objects) {
    this.objects.addAll(objects);
  }
  public void clearObjects() {
    this.objects.clear();
  }
  @Override
  public int compareTo(SchemaPatch o) {
    int result = (int) Math.signum(getPriorVersion()-o.getPriorVersion());
    if (result == 0) {
      result = (int) Math.signum(getNextVersion()-o.getNextVersion());
    }
    return result;
  }
  @Override
  public String toString() {
    return "SchemaPatch [priorVersion=" + priorVersion + ", nextVersion=" + nextVersion + ", objects=" + objects + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SchemaPatch that = (SchemaPatch) o;

    if (nextVersion != that.nextVersion) return false;
    if (priorVersion != that.priorVersion) return false;
    if (objects != null ? !objects.equals(that.objects) : that.objects != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = priorVersion;
    result = 31 * result + nextVersion;
    result = 31 * result + (objects != null ? objects.hashCode() : 0);
    return result;
  }
}
