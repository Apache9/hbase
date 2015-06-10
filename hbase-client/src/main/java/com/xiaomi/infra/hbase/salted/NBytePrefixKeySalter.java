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
 * This will prepend N byte before the rowkey
 *
 */
public abstract class NBytePrefixKeySalter implements KeySalter{
  protected int prefixLength;

  /**
   *
   * @param prefixLength, how many salts is allowed.
   */
  public NBytePrefixKeySalter(int prefixLength) {
    this.prefixLength = prefixLength;
  }

  @Override
  public int getSaltLength() {
    return prefixLength;
  }

  @Override
  public byte[] unSalt(byte[] row) {
    byte[] newRow = new byte[row.length - prefixLength];
    System.arraycopy(row, prefixLength, newRow, 0, newRow.length);
    return newRow;
  }

  @Override
  public byte[] salt(byte[] key) {
    return concat(hash(key), key);
  }
  
  @Override
  public byte[] getSalt(byte[] key) {
    return hash(key);
  }
  
  protected abstract byte[] hash(byte[] key);

  private byte[] concat(byte[] prefix, byte[] row) {
    if (null == prefix || prefix.length == 0) {
      return row;
    }
    if (null == row || row.length == 0) {
      return prefix;
    }
    byte[] newRow = new byte[row.length + prefix.length];
    if (row.length != 0) {
      System.arraycopy(row, 0, newRow, prefix.length, row.length);
    }
    if (prefix.length != 0) {
      System.arraycopy(prefix, 0, newRow, 0, prefix.length);
    }
    return newRow;
  }
}
