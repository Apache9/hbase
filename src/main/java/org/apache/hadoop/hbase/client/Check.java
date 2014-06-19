/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

/**
 * Interface for the single row check.
 */
public interface Check extends Writable {
    /**
     * @return the row of the check
     */
    public byte[] getRow();
    
    /**
     * @return the families for the check
     */
    public Set<byte []> getFamilies();

    /**
     * @return the family map for the check
     */
    public Map<byte [], NavigableSet<byte []>> getFamilyMap();
    
    /**
     * check the row
     * @param result true if the check successes
     * @return
     */
    public boolean check(Result result);
}
