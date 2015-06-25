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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

/**
 * This scan iterator performs full scan.
 */
public class FullScanIterator implements ScanIterator {
  private final Scanner scanner;
  private Tuple currentTuple;

  public FullScanIterator(Scanner scanner) {
    this.scanner = scanner;
  }

  @Override
  public boolean hasNext() throws IOException {
    return (currentTuple = scanner.next()) != null;
  }

  @Override
  public Tuple next() {
    return currentTuple;
  }
}

