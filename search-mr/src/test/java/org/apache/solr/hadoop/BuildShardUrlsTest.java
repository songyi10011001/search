/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class BuildShardUrlsTest extends Assert {
  
  @Test
  public void testBuildShardUrls() throws Exception {
    // 2x3
    Integer numShards = 2;
    List<Object> urls = new ArrayList<>();
    urls.add("shard1");
    urls.add("shard2");
    urls.add("shard3");
    urls.add("shard4");
    urls.add("shard5");
    urls.add("shard6");
    List<List<String>> shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 2, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(3, u.size());
    }
    
    // 1x6
    numShards = 1;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 1, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(6, u.size());
    }
    
    // 6x1
    numShards = 6;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 6, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(1, u.size());
    }
    
    // 3x2
    numShards = 3;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 3, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(2, u.size());
    }
    
    // null shards, 6x1
    numShards = null;
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);
    
    assertEquals(shardUrls.toString(), 6, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(1, u.size());
    }
    
    // null shards 3x1
    numShards = null;
    
    urls = new ArrayList<>();
    urls.add("shard1");
    urls.add("shard2");
    urls.add("shard3");
    
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);

    assertEquals(shardUrls.toString(), 3, shardUrls.size());
    
    for (List<String> u : shardUrls) {
      assertEquals(1, u.size());
    }
    
    // 2x(2,3) off balance
    numShards = 2;
    urls = new ArrayList<>();
    urls.add("shard1");
    urls.add("shard2");
    urls.add("shard3");
    urls.add("shard4");
    urls.add("shard5");
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);

    assertEquals(shardUrls.toString(), 2, shardUrls.size());
    
    Set<Integer> counts = new HashSet<>();
    counts.add(shardUrls.get(0).size());
    counts.add(shardUrls.get(1).size());
    
    assertTrue(counts.contains(2));
    assertTrue(counts.contains(3));
  }
    
}
