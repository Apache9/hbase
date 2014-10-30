/**
 *
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
package com.xiaomi.infra.hbase.salted;


/**
 * A slater will prepend some hashed prefix before the row,
 * So that the row key distribution is more even to avoid hotspoting.
 *
 */
public interface KeySalter {

  /**
   * Get salt length of this salter
   * @return
   */
  public int getSaltLength();

  /**
   * get all possible salts prefix
   * The returned array should be sorted.
   * @return
   */
  public byte [][] getAllSalts();
  
  // return the next salt of parameter 'salt', or null if 'salt' is the last one
  public byte[] nextSalt(byte[] salt);
  
  // return the last salt of parameter 'salt', or null if 'salt' is the first one
  public byte[] lastSalt(byte[] salt);

  /**
   * Salt a rowkey
   * @param rowKey
   * @return
   */
  byte[] salt(byte[] rowKey);
  
  byte[] getSalt(byte[] rowKey);

  /**
   * revert the salted row key to original row key
   * @param row
   * @return
   */
  public byte[] unSalt(byte[] row);
}
