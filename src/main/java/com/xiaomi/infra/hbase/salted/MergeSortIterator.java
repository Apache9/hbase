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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This iterator will do the merge sort. When each iterator is sorted from min to max.
 * The min value is outputed first.
 *
 */
public class MergeSortIterator<T> implements Iterator<T> {

  private PriorityQueue<IterWrapper<T>> heap = null;

  public MergeSortIterator(List<? extends Iterator<T>> iters, Comparator<T> comparator) {
    if (null == iters || iters.size() == 0) {
      throw new IllegalArgumentException("The iterator list is empty");
    }
    this.heap = new PriorityQueue<IterWrapper<T>>(iters.size());
    for (int i = 0; i < iters.size(); i++) {
      IterWrapper<T> wrapper = new IterWrapper<T>(iters.get(i), comparator);
      if (wrapper.next()) {
        this.heap.add(wrapper);
      }
    }
  }

  @Override
  public boolean hasNext() {
    return heap != null && heap.size() != 0;
  }

  @Override
  public T next() {
    if (heap == null) {
      return null;
    }
    IterWrapper<T> iter = heap.poll();
    if (null == iter) {
      return null;
    }
    T current = iter.getCurrent();
    if (iter.next()) {
      heap.add(iter);
    }
    return current;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private static class IterWrapper<T> implements  Comparable<IterWrapper<T>>{
    private T current = null;
    private Iterator<T> iter;
    private Comparator<T> comparator;

    public IterWrapper(Iterator<T> iter, Comparator<T> comparator) {
      this.iter = iter;
      this.comparator = comparator;
    }

    public int compareTo(IterWrapper<T> other) {
      T my = this.getCurrent();
      T they = other.getCurrent();
      return comparator.compare(my, they);
    }

    public T getCurrent() {
      return current;
    }

    public boolean next() {
      if (iter.hasNext()) {
        current = iter.next();
        return null != current;
      }
      return false;
    }
  }
}
