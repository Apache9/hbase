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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Get, remove and modify table descriptors.
 * <p/>
 * Used by servers to host descriptors.
 */
@InterfaceAudience.Private
public interface MutableTableDescriptors extends TableDescriptors {

  /**
   * Add or update descriptor
   * @param htd Descriptor to set into TableDescriptors
   */
  void add(final TableDescriptor htd) throws IOException;

  /**
   * @param tablename
   * @return Instance of table descriptor or none if not found.
   */
  Optional<TableDescriptor> remove(final TableName tablename) throws IOException;

  /**
   * Enables the tabledescriptor cache
   */
  void setCacheOn() throws IOException;

  /**
   * Disables the tabledescriptor cache
   */
  void setCacheOff() throws IOException;
}
