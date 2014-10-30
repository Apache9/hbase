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

import java.util.Arrays;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

// This will prepend at most 3 bytes before the rowkey
public class NBytePrefixKeySalter implements KeySalter {
  private final static int MAX_SLOTS_COUNT = 16777216; // 256 * 256 * 256 is big enough
  protected int prefixLength;
  protected final TreeSet<byte[]> saltsTree = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  protected byte[][] allSalts;
  protected int slots;
  
  public NBytePrefixKeySalter(int slotsCount) {
    if (slotsCount > MAX_SLOTS_COUNT || slotsCount <= 0) {
      throw new RuntimeException(
          "slots count illegal, slots count must be positive and maximum slots count of OneBytePrefixKeySalter is "
              + MAX_SLOTS_COUNT + ", current slots count:" + slotsCount);
    }
    
    this.prefixLength = getPrefixLength(slotsCount);
    this.slots = slotsCount;
    this.allSalts = computeAllSalts();
    for (int i = 0; i < allSalts.length; ++i) {
      saltsTree.add(allSalts[i]);
    }        
  }
  
  protected static int getPrefixLength(int slotsCount) {
    if (slotsCount <= 256) {
      return 1;
    } else if (slotsCount <= 65536) {
      return 2;
    } else {
      return 3;
    }
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
    return concat(getSalt(key), key);
  }
  
  @Override
  public byte[] getSalt(byte[] key) {
    int hash = 1;
    if (key == null || key.length == 0) {
      return hashValueToSalt(0, prefixLength);
    }
    for (int i = 0; i < key.length; i++) {
      hash = 31 * hash + (int)(key[i]);
    }
    hash = hash & 0x7fffffff;
    return hashValueToSalt(hash % slots, prefixLength);
  }

  private byte[] concat(byte[] prefix, byte[] row) {
    if (null == prefix || prefix.length == 0) {
      return row;
    }
    if (null == row || row.length == 0) {
      return prefix;
    }
    byte[] newRow = new byte[row.length + prefix.length];
    System.arraycopy(row, 0, newRow, prefix.length, row.length);
    System.arraycopy(prefix, 0, newRow, 0, prefix.length);
    return newRow;
  }
  
  protected byte[][] computeAllSalts() {
    byte[][] salts = new byte[slots][];
    for (int i = 0; i < salts.length; i++) {
      salts[i] = hashValueToSalt(i, prefixLength);
    }
    Arrays.sort(salts, Bytes.BYTES_RAWCOMPARATOR);
    return salts;    
  }
  
  protected static byte[] hashValueToSalt(int slotIndex, int prefixLength) {
    byte[] salt = new byte[prefixLength];
    for (int i = 0; i < salt.length; ++i) {
      salt[i] = (byte)(slotIndex % 256);
      slotIndex /= 256;
    }
    return salt;
  }
  
  @Override
  public byte[][] getAllSalts() {
    return allSalts;
  }
  
  @Override
  public byte[] nextSalt(byte[] salt) {
    return saltsTree.higher(salt);
  }

  @Override
  public byte[] lastSalt(byte[] salt) {
    return saltsTree.lower(salt);
  }
}