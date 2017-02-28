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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import com.google.common.base.Charsets;

@ThreadLeakAction({Action.WARN})
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(Consequence.CONTINUE)
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class, BadMrClusterThreadsFilter.class // hdfs currently leaks thread(s)
})
@ThreadLeakScope(Scope.NONE)
@SuppressCodecs({"Lucene3x", "Lucene40"})
@SuppressSSL // SSL does not work with this test for currently unknown reasons
@Slow
public class MorphlineGoLiveMiniMRTest extends MiniMRBase {
  
  private static final boolean TEST_NIGHTLY = false;
  private static final int RECORD_COUNT = 2104;

  public MorphlineGoLiveMiniMRTest() {
    super();
  }
  
  @Test
  public void testBuildShardUrls() throws Exception {
    // 2x3
    Integer numShards = 2;
    List<Object> urls = new ArrayList<Object>();
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
    
    urls = new ArrayList<Object>();
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
    urls = new ArrayList<Object>();
    urls.add("shard1");
    urls.add("shard2");
    urls.add("shard3");
    urls.add("shard4");
    urls.add("shard5");
    shardUrls = MapReduceIndexerTool.buildShardUrls(urls , numShards);

    assertEquals(shardUrls.toString(), 2, shardUrls.size());
    
    Set<Integer> counts = new HashSet<Integer>();
    counts.add(shardUrls.get(0).size());
    counts.add(shardUrls.get(1).size());
    
    assertTrue(counts.contains(2));
    assertTrue(counts.contains(3));
  }
  
  private String[] prependInitialArgs(String[] args) {
    String[] head = new String[] {
        "--morphline-file=" + tempDir + "/test-morphlines/solrCellDocumentTypes.conf",
        "--morphline-id=morphline1",
    };
    return concat(head, args); 
  }
  
  @Override
  public void doTest() throws Exception {
    
    waitForRecoveriesToFinish(false);
    
    FileSystem fs = dfsCluster.getFileSystem();
    Path inDir = fs.makeQualified(new Path(
        "/user/testing/testMapperReducer/input"));
    fs.delete(inDir, true);
    String DATADIR = "/user/testing/testMapperReducer/data";
    Path dataDir = fs.makeQualified(new Path(DATADIR));
    fs.delete(dataDir, true);
    Path outDir = fs.makeQualified(new Path(
        "/user/testing/testMapperReducer/output"));
    fs.delete(outDir, true);
    
    assertTrue(fs.mkdirs(inDir));
    Path INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile1);
    
    JobConf jobConf = getJobConf();
    jobConf.set("jobclient.output.filter", "ALL");
    // enable mapred.job.tracker = local to run in debugger and set breakpoints
    // jobConf.set("mapred.job.tracker", "local");
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.setJar(SEARCH_ARCHIVES_JAR);

    MapReduceIndexerTool tool;
    int res;
    QueryResponse results;
    HttpSolrServer server = new HttpSolrServer(cloudJettys.get(0).url);
    String[] args = new String[]{};

    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--log4j=" + getFile("log4j.properties").getAbsolutePath(),
        "--mappers=3",
        random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(),  
        "--go-live-threads", Integer.toString(random().nextInt(15) + 1),
        "--verbose",
        "--go-live"
    };
    args = prependInitialArgs(args);
    List<String> argList = new ArrayList<String>();
    getShardUrlArgs(argList);
    args = concat(args, argList.toArray(new String[0]));
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      results = server.query(new SolrQuery("*:*"));
      assertEquals(20, results.getResults().getNumFound());
    }    
    
    fs.delete(inDir, true);   
    fs.delete(outDir, true);  
    fs.delete(dataDir, true); 
    assertTrue(fs.mkdirs(inDir));
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile2);

    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--mappers=3",
        "--verbose",
        "--go-live",
        random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(), 
        "--go-live-threads", Integer.toString(random().nextInt(15) + 1)
    };
    args = prependInitialArgs(args);
    argList = new ArrayList<String>();
    getShardUrlArgs(argList);
    args = concat(args, argList.toArray(new String[0]));
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());      
      results = server.query(new SolrQuery("*:*"));
      
      assertEquals(22, results.getResults().getNumFound());
    }    
    
    // try using zookeeper
    String collection = "collection1";
    if (random().nextBoolean()) {
      // sometimes, use an alias
      createAlias("updatealias", "collection1");
      collection = "updatealias";
    }
    
    fs.delete(inDir, true);   
    fs.delete(outDir, true);  
    fs.delete(dataDir, true);    
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);

    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());      

    args = new String[] {
        "--output-dir=" + outDir.toString(),
        "--mappers=3",
        "--reducers=12",
        "--fanout=2",
        "--verbose",
        "--go-live",
        random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(), 
        "--zk-host", zkServer.getZkAddress(), 
        "--collection", collection
    };
    args = prependInitialArgs(args);

    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      SolrDocumentList resultDocs = executeSolrQuery(cloudClient, "*:*");      
      assertEquals(RECORD_COUNT, resultDocs.getNumFound());
      assertEquals(RECORD_COUNT, resultDocs.size());
      
      // perform updates
      for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);
          SolrInputDocument update = new SolrInputDocument();
          for (Map.Entry<String, Object> entry : doc.entrySet()) {
              update.setField(entry.getKey(), entry.getValue());
          }
          update.setField("user_screen_name", "Nadja" + i);
          update.removeField("_version_");
          cloudClient.add(update);
      }
      cloudClient.commit();
      
      // verify updates
      SolrDocumentList resultDocs2 = executeSolrQuery(cloudClient, "*:*");   
      assertEquals(RECORD_COUNT, resultDocs2.getNumFound());
      assertEquals(RECORD_COUNT, resultDocs2.size());
      for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);
          SolrDocument doc2 = resultDocs2.get(i);
          assertEquals(doc.getFirstValue("id"), doc2.getFirstValue("id"));
          assertEquals("Nadja" + i, doc2.getFirstValue("user_screen_name"));
          assertEquals(doc.getFirstValue("text"), doc2.getFirstValue("text"));
          
          // perform delete
          cloudClient.deleteById((String)doc.getFirstValue("id"));
      }
      cloudClient.commit();
      
      // verify deletes
      assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    }    
    
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());      
    server.shutdown();
    
    // try using zookeeper with replication
    String replicatedCollection = "replicated_collection";
    if (TEST_NIGHTLY) {
      createCollection(replicatedCollection, 11, 3, 11);
    } else {
      createCollection(replicatedCollection, 2, 3, 2);
    }
    waitForRecoveriesToFinish(false);
    waitForRecoveriesToFinish(replicatedCollection, false);
    cloudClient.setDefaultCollection(replicatedCollection);
    fs.delete(inDir, true);   
    fs.delete(outDir, true);  
    fs.delete(dataDir, true);  
    assertTrue(fs.mkdirs(dataDir));
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);
    
    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--mappers=3",
        "--reducers=22",
        "--fanout=2",
        "--verbose",
        "--go-live",
        "--zk-host", zkServer.getZkAddress(), 
        "--collection", replicatedCollection, dataDir.toString()
    };
    args = prependInitialArgs(args);
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      SolrDocumentList resultDocs = executeSolrQuery(cloudClient, "*:*");   
      assertEquals(RECORD_COUNT, resultDocs.getNumFound());
      assertEquals(RECORD_COUNT, resultDocs.size());
      
      checkConsistency(replicatedCollection);
      
      // perform updates
      for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);          
          SolrInputDocument update = new SolrInputDocument();
          for (Map.Entry<String, Object> entry : doc.entrySet()) {
              update.setField(entry.getKey(), entry.getValue());
          }
          update.setField("user_screen_name", "@Nadja" + i);
          update.removeField("_version_");
          cloudClient.add(update);
      }
      cloudClient.commit();
      
      // verify updates
      SolrDocumentList resultDocs2 = executeSolrQuery(cloudClient, "*:*");   
      assertEquals(RECORD_COUNT, resultDocs2.getNumFound());
      assertEquals(RECORD_COUNT, resultDocs2.size());
      for (int i = 0; i < RECORD_COUNT; i++) {
          SolrDocument doc = resultDocs.get(i);
          SolrDocument doc2 = resultDocs2.get(i);
          assertEquals(doc.getFieldValues("id"), doc2.getFieldValues("id"));
          assertEquals(1, doc.getFieldValues("id").size());
          assertEquals(Arrays.asList("@Nadja" + i), doc2.getFieldValues("user_screen_name"));
          assertEquals(doc.getFieldValues("text"), doc2.getFieldValues("text"));
          
          // perform delete
          cloudClient.deleteById((String)doc.getFirstValue("id"));
      }
      cloudClient.commit();
      
      // verify deletes
      assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    }
    
    // try using solr_url with replication
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").getNumFound());
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    fs.delete(inDir, true);    
    fs.delete(dataDir, true);
    assertTrue(fs.mkdirs(dataDir));
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);
    
    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--shards", "2",
        "--mappers=3",
        "--verbose",
        "--go-live", 
        "--go-live-threads", Integer.toString(random().nextInt(15) + 1),  dataDir.toString()
    };
    args = prependInitialArgs(args);

    argList = new ArrayList<String>();
    getShardUrlArgs(argList, replicatedCollection);
    args = concat(args, argList.toArray(new String[0]));
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      checkConsistency(replicatedCollection);
      
      assertEquals(RECORD_COUNT, executeSolrQuery(cloudClient, "*:*").size());
    }
    
    // delete collection
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").getNumFound());
    
    args = new String[] {
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--shards", "2",
        "--mappers=3",
        "--verbose",
        "--go-live", 
        "--go-live-threads", Integer.toString(random().nextInt(15) + 1),  dataDir.toString()
    };
    args = prependInitialArgs(args);

    argList = new ArrayList<String>();
    getShardUrlArgs(argList, replicatedCollection);
    args = concat(args, argList.toArray(new String[0]));
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      checkConsistency(replicatedCollection);
      
      assertEquals(RECORD_COUNT, executeSolrQuery(cloudClient, "*:*").size());
    }

    
    // run with INJECT_FOLLOWER_MERGE_FAILURES
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    args = new String[] {
        "--output-dir=" + outDir.toString(),
        "--mappers=1",
        "--verbose",
        "--go-live",
        random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(), 
        "--zk-host", zkServer.getZkAddress(), 
        "--collection", replicatedCollection
    };
    args = prependInitialArgs(args);

    if (true) {
      System.setProperty(GoLive.INJECT_FOLLOWER_MERGE_FAILURES, "true");
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(-1, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
    }
    
    // run with reduced go-live-min-replication-factor and INJECT_FOLLOWER_MERGE_FAILURES
    if (true) {
      args = concat(args, new String[]{"--go-live-min-replication-factor=1"});           
      System.setProperty(GoLive.INJECT_FOLLOWER_MERGE_FAILURES, "true");
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
    }
  }

  private void getShardUrlArgs(List<String> args) {
    for (int i = 0; i < shardCount; i++) {
      args.add("--shard-url");
      args.add(cloudJettys.get(i).url);
    }
  }
  
  private SolrDocumentList executeSolrQuery(SolrServer collection, String queryString) throws SolrServerException {
    SolrQuery query = new SolrQuery(queryString).setRows(2 * RECORD_COUNT).addSort("id", ORDER.asc);
    QueryResponse response = collection.query(query);
    return response.getResults();
  }

  private void checkConsistency(String replicatedCollection)
      throws SolrServerException {
    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState()
        .getSlices(replicatedCollection);
    for (Slice slice : slices) {
      Collection<Replica> replicas = slice.getReplicas();
      long found = -1;
      for (Replica replica : replicas) {
        HttpSolrServer client = new HttpSolrServer(
            new ZkCoreNodeProps(replica).getCoreUrl());
        SolrQuery query = new SolrQuery("*:*");
        query.set("distrib", false);
        QueryResponse replicaResults = client.query(query);
        long count = replicaResults.getResults().getNumFound();
        if (found != -1) {
          assertEquals(slice.getName() + " is inconsistent "
              + new ZkCoreNodeProps(replica).getCoreUrl(), found, count);
        }
        found = count;
        client.shutdown();
      }
    }
  }
  
  private void getShardUrlArgs(List<String> args, String replicatedCollection) {
    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getSlices(replicatedCollection);
    for (Slice slice : slices) {
      Collection<Replica> replicas = slice.getReplicas();
      for (Replica replica : replicas) {
        args.add("--shard-url");
        args.add(new ZkCoreNodeProps(replica).getCoreUrl());
      }
    }
  }
  
  private Path upAvroFile(FileSystem fs, Path inDir, String DATADIR,
      Path dataDir, String localFile) throws IOException, UnsupportedEncodingException {
    Path INPATH = new Path(inDir, "input.txt");
    OutputStream os = fs.create(INPATH);
    Writer wr = new OutputStreamWriter(os, Charsets.UTF_8);
    wr.write(DATADIR + File.separator + localFile);
    wr.close();
    
    assertTrue(fs.mkdirs(dataDir));
    fs.copyFromLocalFile(new Path(DOCUMENTS_DIR, localFile), dataDir);
    return INPATH;
  }

  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(solrHome.getAbsolutePath(),
        context, 0, solrConfigOverride, schemaOverride, true, null);

    jetty.setShards(shardList);
    
    if (System.getProperty("collection") == null) {
      System.setProperty("collection", "collection1");
    }
    
    jetty.start();
    
    System.clearProperty("collection");
    
    return jetty;
  }

  private NamedList<Object> createAlias(String alias, String collections) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collections", collections);
    params.set("name", alias);
    params.set("action", CollectionAction.CREATEALIAS.toString());
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    return cloudClient.request(request);
  }

}
