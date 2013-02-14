/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package tajo.catalog;

public class CatalogConstants {
  public static final String STORE_CLASS="tajo.catalog.store.class";
  
  public static final String JDBC_DRIVER="tajo.catalog.jdbc.driver";
  public static final String DEFAULT_JDBC_DRIVER="org.apache.derby.jdbc.EmbeddedDriver";
  
  public static final String JDBC_URI="tajo.catalog.jdbc.uri";

  private CatalogConstants() {}
}
