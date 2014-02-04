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

package org.apache.tajo.util;

public class NumberUtil {

	public static long unsigned32(int n) {
		return n & 0xFFFFFFFFL;
	}
	
	public static int unsigned16(short n) {
		return n & 0xFFFF;
	}

  public static byte[] toAsciiBytes(Number i){
    return Bytes.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(short i){
    return Bytes.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(int i){
    return Bytes.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(long i){
    return Bytes.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(float i){
    return Bytes.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(double i){
    return Bytes.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  private static void benchmark(int num){
    System.out.println("Start benchmark. # of :" + num);
    long start =  System.currentTimeMillis();

    byte[] bytes;
    long size = 0;
    for (int i = 0; i < num; i ++){
      bytes = String.valueOf(i).getBytes();
      size += bytes.length;
    }

    long end =  System.currentTimeMillis();
    System.out.println("JDK getBytes() \t\t\t\t" + (end - start) + " ms, " + "Total: " + size / (1024 * 1024) + "MB");
    size = 0;

    for (int i = 0; i < num; i ++){
      bytes = toAsciiBytes(i);
      size += bytes.length;
    }
    System.out.println( "NumberUtil toByte() \t" + (System.currentTimeMillis() - end)
        + " ms, " + "Total: " + size / (1024 * 1024) + "MB");
  }

  public static void main(String[] args) throws Exception {
    benchmark(1024 * 1024 * 10);
  }
}
