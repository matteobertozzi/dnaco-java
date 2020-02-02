/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.collections;

import java.util.Arrays;

import tech.dnaco.util.BitUtil;

public class HashIndexedArray<K> {
  private final int[] buckets;
  private final int[] hashes;
  private final int[] next;
  private final K[] keys;

  public HashIndexedArray(final K[] keys) {
    this.keys = keys;

    this.buckets = new int[tableSizeFor(keys.length + 8)];
    Arrays.fill(this.buckets, -1);
    this.hashes = new int[keys.length];
    this.next = new int[keys.length];

    final int nBuckets = buckets.length - 1;
    for (int i = 0, n = keys.length; i < n; ++i) {
      final int hashCode = hash(keys[i]);
      final int targetBucket = hashCode & nBuckets;
      this.hashes[i] = hashCode;
      this.next[i] = buckets[targetBucket];
      this.buckets[targetBucket] = i;
    }
  }

  public int size() {
    return keys.length;
  }

  public K[] keySet() {
    return keys;
  }

  public K get(final int index) {
    return keys[index];
  }

  public boolean contains(final K key) {
    return getIndex(key) >= 0;
  }

  public int getIndex(final K key) {
    final int hashCode = hash(key);
    int index = buckets[hashCode & (buckets.length - 1)];
    while (index >= 0) {
      if (hashCode == hashes[index] && keys[index].equals(key)) {
        return index;
      }
      index = next[index];
    }
    return -1;
  }

  private static int hash(final Object key) {
    int h = key.hashCode() & 0x7fffffff;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = (h >>> 16) ^ h;
    return h & 0x7fffffff;
  }

  private static int tableSizeFor(final int cap) {
    final int MAXIMUM_CAPACITY = 1 << 30;
    final int n = BitUtil.nextPow2(cap);
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
  }

  @Override
  public String toString() {
    return Arrays.toString(keys);
  }

  public static void main(final String[] args) throws Exception {
    new HashIndexedArray<>(new String[] {"paramname", "paramvalue"});
    new HashIndexedArray<>(new String[] {"idqg", "descrizione", "img", "idqlist", "flag1", "flag2", "flag3", "flag4", "flag5"});
    new HashIndexedArray<>(new String[] {"idq", "descrizione", "enable", "lang", "codiceq"});
    new HashIndexedArray<>(new String[] {"idq", "idd", "descrizione", "tipo", "idri", "qorder", "valdef", "required", "condizioneval", "condizione", "condizionep1", "condizionep2", "azione", "azioneelse", "azionep1", "azionep2", "azioneelsep1", "azioneelsep2", "codiced"});
    new HashIndexedArray<>(new String[] {"idq", "idd", "type", "data01", "data02", "data03", "data04", "data05"});
    new HashIndexedArray<>(new String[] {"idri", "idi", "itemorder", "varreturn", "descrizione"});
    new HashIndexedArray<>(new String[] {"idq", "idd", "ida", "action", "actionp1", "actionp2", "descrizione", "img", "custom1", "custom2", "custom3", "custom4", "custom5", "custom6", "custom7", "custom8", "custom9", "custom10"});
    new HashIndexedArray<>(new String[] {"tabname", "tabdesc", "idxvalkey", "descval01", "descval02", "descval03", "descval04", "descval05", "descval06", "descval07", "descval08", "descval09", "descval10", "descval11", "descval12", "descval13", "descval14", "descval15", "descval16", "descval17", "descval18", "descval19", "descval20", "flag1", "flag2", "flag3", "flag4", "flag5"});
    new HashIndexedArray<>(new String[] {"tabname", "reckey", "defvalue", "idd", "flag1", "flag2", "flag3", "flag4", "flag5"});
    new HashIndexedArray<>(new String[] {"idgracq", "idacq", "acqorder", "descrizione", "tipo", "custom1", "custom2", "custom3", "custom4", "custom5", "custom6", "custom7", "custom8", "custom9", "custom10", "readonly"});
    new HashIndexedArray<>(new String[] {"idq", "img", "flag1", "flag2", "flag3", "flag4", "flag5"});
    new HashIndexedArray<>(new String[] {"idq", "img", "flag1", "flag2", "flag3", "flag4", "flag5"});
    new HashIndexedArray<>(new String[] {"tabname", "reckey", "rectype", "isupdated", "val01", "val02", "val03", "val04", "val05", "val06", "val07", "val08", "val09", "val10"});
    new HashIndexedArray<>(new String[] {"id", "idk", "idt", "tipo", "lang", "translation", "custom1", "custom2", "custom3", "custom4", "custom5"});
    new HashIndexedArray<>(new String[] {"idq", "idd", "type", "idref", "script", "custom1", "custom2", "custom3", "custom4", "custom5"});
    new HashIndexedArray<>(new String[] {"tabname", "reckey", "val01", "val02", "val03", "val04", "val05", "val06", "val07", "val08", "val09", "val10", "val11", "val12", "val13", "val14", "val15", "val16", "val17", "val18", "val19", "val20", "sortorder"});
  }
}
