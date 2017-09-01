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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateAlias;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.apache.solr.util.BadMrClusterThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;

@ThreadLeakAction({Action.WARN})
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(Consequence.CONTINUE)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class, BadMrClusterThreadsFilter.class // hdfs currently leaks thread(s)
})
@SuppressSSL // SSL does not work with this test for currently unknown reasons
@Slow
//@Nightly
public class MorphlineGoLiveMiniMRTest extends AbstractFullDistribZkTestBase {
  
  private static final boolean TEST_NIGHTLY = false;
  private static final int RECORD_COUNT = 2104;
  private static final String RESOURCES_DIR = getFile("morphlines-core.marker").getParent();  
  private static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  private static final File MINIMR_INSTANCE_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  private static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  
  private static String SEARCH_ARCHIVES_JAR;
  
  private static MiniDFSCluster dfsCluster = null;
  private static MiniMRClientCluster mrCluster = null;
  private static String tempDir;
 
  private final String inputAvroFile1;
  private final String inputAvroFile2;
  private final String inputAvroFile3;

  private static File solrHomeDirectory;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  @Override
  public String getSolrHome() {
    return solrHomeDirectory.getPath();
  }
  
  public MorphlineGoLiveMiniMRTest() {
    this.inputAvroFile1 = "sample-statuses-20120521-100919.avro";
    this.inputAvroFile2 = "sample-statuses-20120906-141433.avro";
    this.inputAvroFile3 = "sample-statuses-20120906-141433-medium.avro";

    sliceCount = TEST_NIGHTLY ? 5 : 3;
    fixShardCount(sliceCount);
  }
  
  @BeforeClass
  public static void setupClass() throws Exception {
    log.info("stepA1");
    System.setProperty("solr.tests.cloud.cm.enabled", "false"); // disable Solr ChaosMonkey
    System.setProperty("solr.hdfs.blockcache.global", Boolean.toString(LuceneTestCase.random().nextBoolean()));
    System.setProperty("solr.hdfs.blockcache.enabled", Boolean.toString(LuceneTestCase.random().nextBoolean()));
    System.setProperty("solr.hdfs.blockcache.blocksperbank", "2048");
    
    solrHomeDirectory = createTempDir().toFile();

    assumeFalse("HDFS tests were disabled by -Dtests.disableHdfs",
        Boolean.parseBoolean(System.getProperty("tests.disableHdfs", "false")));
    
    assumeFalse("FIXME: This test does not work with Windows because of native library requirements", Constants.WINDOWS);
    
    log.info("stepAMINIMR_INSTANCE_DIR="+MINIMR_INSTANCE_DIR.getAbsolutePath());
    log.info("stepASolrHomeDirectory="+solrHomeDirectory.getAbsolutePath());
    AbstractZkTestCase.SOLRHOME = solrHomeDirectory;
    FileUtils.copyDirectory(MINIMR_INSTANCE_DIR, AbstractZkTestCase.SOLRHOME);
    tempDir = createTempDir().toFile().getAbsolutePath();

    new File(tempDir).mkdirs();

    FileUtils.copyFile(new File(RESOURCES_DIR + "/custom-mimetypes.xml"), new File(tempDir + "/custom-mimetypes.xml"));
    
    UtilsForTests.setupMorphline(tempDir, "test-morphlines/solrCellDocumentTypes", true, RESOURCES_DIR);
    
    System.setProperty("hadoop.log.dir", new File(tempDir, "logs").getAbsolutePath());
    
    int dataNodes = 2;
    
    JobConf conf = new JobConf();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("dfs.namenode.acls.enabled", "true");
//    see https://community.hortonworks.com/questions/27153/getting-ioexception-failed-to-replace-a-bad-datano.html
//    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS");
//    conf.set("dfs.client.block.write.replace-datanode-on-failure.best-effort", "true");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("mapreduce.jobhistory.minicluster.fixed.ports", "false");
    conf.set("mapreduce.jobhistory.admin.address", "0.0.0.0:0");
    
    new File(tempDir + File.separator +  "nm-local-dirs").mkdirs();
    
    System.setProperty("test.build.dir", tempDir + File.separator + "hdfs" + File.separator + "test-build-dir");
    System.setProperty("test.build.data", tempDir + File.separator + "hdfs" + File.separator + "build");
    System.setProperty("test.cache.data", tempDir + File.separator + "hdfs" + File.separator + "cache");

    // Initialize AFTER test.build.dir is set, JarFinder uses it.
    SEARCH_ARCHIVES_JAR = JarFinder.getJar(MapReduceIndexerTool.class);
    
    log.info("stepA2");
    MiniDFSCluster.Builder miniDFSClusterBuilder = new MiniDFSCluster.Builder(conf).numDataNodes(dataNodes).format(true).racks(null);
    try {
      dfsCluster = miniDFSClusterBuilder.build();
    } catch (FileNotFoundException e) {
      if (!e.getMessage().contains("No valid image files found")) {
        throw e;
      }
      
      // retry build() to workaround a spurious race in MiniDFSCluster startup
      log.warn("Retrying mostly harmless spurious exception", e);
      dfsCluster = miniDFSClusterBuilder.build(); 
    }
    
    log.info("stepA3");
    FileSystem fileSystem = dfsCluster.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
    fileSystem.setPermission(new Path("/tmp"),
        FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/user"),
        FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/hadoop/mapred/system"),
        FsPermission.valueOf("-rwx------"));
    
    log.info("stepA4");
    mrCluster = MiniMRClientClusterFactory.create(MorphlineGoLiveMiniMRTest.class, 1, conf);
    log.info("stepA5");

    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }
  
  @Override
  public void distribSetUp() throws Exception {
    log.info("stepB1");
    super.distribSetUp();
    log.info("stepB2");
    System.setProperty("host", "127.0.0.1");
    System.setProperty("numShards", Integer.toString(sliceCount));
    URI uri = dfsCluster.getFileSystem().getUri();
    System.setProperty("solr.hdfs.home",  uri.toString() + "/" + this.getClass().getName());
    uploadConfFiles();
    log.info("stepB3");
  }
  
  @Override
  public void distribTearDown() throws Exception {
    log.info("stepX1");
    super.distribTearDown();
    System.clearProperty("host");
    System.clearProperty("numShards");
    System.clearProperty("solr.hdfs.home");
    log.info("stepX2");
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    log.info("stepY1");
    System.clearProperty("solr.hdfs.blockcache.global");
    System.clearProperty("solr.hdfs.blockcache.blocksperbank");
    System.clearProperty("solr.hdfs.blockcache.enabled");
    System.clearProperty("hadoop.log.dir");
    System.clearProperty("test.build.dir");
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
    
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
    log.info("stepY2");
    if (dfsCluster != null) {
      dfsCluster.shutdown(false, false);
      dfsCluster = null;
    }
    log.info("stepY3");
//    FileSystem.closeAll();
  }
  
  private JobConf getJobConf() throws IOException {
    JobConf jobConf = new JobConf(mrCluster.getConfig());
    return jobConf;
  }
  
  private String[] prependInitialArgs(String[] args) {
    String[] head = new String[] {
        "--morphline-file=" + tempDir + "/test-morphlines/solrCellDocumentTypes.conf",
        "--morphline-id=morphline1",
    };
    return concat(head, args); 
  }

  @Test
  public void test() throws Exception {
    log.info("stepT1");
    waitForRecoveriesToFinish(false);
    log.info("stepT2");
    
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
    String[] args = new String[]{};
    List<String> argList = new ArrayList<>();

    try (HttpSolrClient server = getHttpSolrClient(cloudJettys.get(0).url)) {

      args = new String[]{
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
      getShardUrlArgs(argList);
      args = concat(args, argList.toArray(new String[0]));

      if (true) {
        log.info("stepT3");
        tool = new MapReduceIndexerTool();
        res = ToolRunner.run(jobConf, tool, args);
        log.info("stepT4");
        assertEquals(0, res);
        log.info("stepT4a");
        assertTrue(tool.job.isComplete());
        assertTrue(tool.job.isSuccessful());
        results = server.query(new SolrQuery("*:*"));
        assertEquals(20, results.getResults().getNumFound());
      }

      log.info("stepT5");
      fs.delete(inDir, true);
      fs.delete(outDir, true);
      fs.delete(dataDir, true);
      assertTrue(fs.mkdirs(inDir));
      INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile2);

      args = new String[]{
          "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
          "--output-dir=" + outDir.toString(),
          "--mappers=3",
          "--verbose",
          "--go-live",
          random().nextBoolean() ? "--input-list=" + INPATH.toString() : dataDir.toString(),
          "--go-live-threads", Integer.toString(random().nextInt(15) + 1)
      };
      args = prependInitialArgs(args);

      getShardUrlArgs(argList);
      args = concat(args, argList.toArray(new String[0]));

      if (true) {
        log.info("stepT6a");
        tool = new MapReduceIndexerTool();
        res = ToolRunner.run(jobConf, tool, args);
        log.info("stepT6b");
        assertEquals(0, res);
        log.info("stepT6c");
        assertTrue(tool.job.isComplete());
        assertTrue(tool.job.isSuccessful());
        results = server.query(new SolrQuery("*:*"));

        assertEquals(22, results.getResults().getNumFound());
      }

      // try using zookeeper
      String collection = "collection1";
      if (random().nextBoolean()) {
        // sometimes, use an alias
        String aliasName = "updatealias";
        CreateAlias request = CollectionAdminRequest.createAlias(aliasName, "collection1");
        RequestStatusState state = request.processAndWait(cloudClient, 20);
        assertEquals(RequestStatusState.COMPLETED, state);
        collection = aliasName;
      }

      fs.delete(inDir, true);
      fs.delete(outDir, true);
      fs.delete(dataDir, true);
      INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);

      cloudClient.deleteByQuery("*:*");
      cloudClient.commit();
      assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());

      args = new String[]{
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
        log.info("stepT7a");
        tool = new MapReduceIndexerTool();
        res = ToolRunner.run(jobConf, tool, args);
        log.info("stepT7b");
        assertEquals(0, res);
        log.info("stepT7c");
        assertTrue(tool.job.isComplete());
        assertTrue(tool.job.isSuccessful());
        log.info("stepT7d");

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
          cloudClient.deleteById((String) doc.getFirstValue("id"));
        }
        cloudClient.commit();

        // verify deletes
        assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
      }

      cloudClient.deleteByQuery("*:*");
      cloudClient.commit();
      assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    log.info("stepT7e");
    
    // try using zookeeper with replication
    String replicatedCollection = "replicated_collection";
    CollectionAdminResponse rsp;
    if (TEST_NIGHTLY) {
      rsp = createCollection(replicatedCollection, "conf1", 11, 3, 11);
    } else {
      rsp = createCollection(replicatedCollection, "conf1", 2, 3, 2);
    }
    assertTrue(rsp.isSuccess());
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
      log.info("stepT8a");
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      log.info("stepT8b");
      assertEquals(0, res);
      log.info("stepT8c");
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
      log.info("stepT8d");
      
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
      log.info("stepT8e");
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

    argList = new ArrayList<>();
    getShardUrlArgs(argList, replicatedCollection);
    args = concat(args, argList.toArray(new String[0]));
    
    if (true) {
      log.info("stepT9a");
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      log.info("stepT9b");
      assertEquals(0, res);
      log.info("stepT9c");
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      checkConsistency(replicatedCollection);
      
      assertEquals(RECORD_COUNT, executeSolrQuery(cloudClient, "*:*").size());
    }
    
    // delete collection
    log.info("stepT9d");
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").size());
    assertEquals(0, executeSolrQuery(cloudClient, "*:*").getNumFound());
    log.info("stepT9e");
    
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

    argList = new ArrayList<>();
    getShardUrlArgs(argList, replicatedCollection);
    args = concat(args, argList.toArray(new String[0]));
    
    if (true) {
      tool = new MapReduceIndexerTool();
      log.info("stepT10a");
      res = ToolRunner.run(jobConf, tool, args);
      log.info("stepT10b");
      assertEquals(0, res);
      log.info("stepT10c");
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      checkConsistency(replicatedCollection);
      
      assertEquals(RECORD_COUNT, executeSolrQuery(cloudClient, "*:*").size());
      log.info("stepT10d");
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
      log.info("stepT11a");
      res = ToolRunner.run(jobConf, tool, args);
      log.info("stepT11b");
      assertEquals(-1, res);
      log.info("stepT11c");
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
    }
    
    // run with reduced go-live-min-replication-factor and INJECT_FOLLOWER_MERGE_FAILURES
    if (true) {
      args = concat(args, new String[]{"--go-live-min-replication-factor=1"});           
      System.setProperty(GoLive.INJECT_FOLLOWER_MERGE_FAILURES, "true");
      tool = new MapReduceIndexerTool();
      log.info("stepT12a");
      res = ToolRunner.run(jobConf, tool, args);
      log.info("stepT12b");
      assertEquals(0, res);
      log.info("stepT12c");
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
    }
  }

  private void getShardUrlArgs(List<String> args) {
    for (int i = 0; i < getShardCount(); i++) {
      args.add("--shard-url");
      args.add(cloudJettys.get(i).url);
    }
  }
  
  private SolrDocumentList executeSolrQuery(SolrClient collection, String queryString) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery(queryString).setRows(2 * RECORD_COUNT).addSort("id", ORDER.asc);
    QueryResponse response = collection.query(query);
    return response.getResults();
  }

  private void checkConsistency(String replicatedCollection)
      throws Exception {
    log.info("stepTcheckconsistency1");
    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState()
        .getCollection(replicatedCollection).getSlices();
    for (Slice slice : slices) {
      Collection<Replica> replicas = slice.getReplicas();
      long found = -1;
      for (Replica replica : replicas) {
        try (HttpSolrClient client = getHttpSolrClient(new ZkCoreNodeProps(replica).getCoreUrl())) {
          SolrQuery query = new SolrQuery("*:*");
          query.set("distrib", false);
          QueryResponse replicaResults = client.query(query);
          long count = replicaResults.getResults().getNumFound();
          if (found != -1) {
            assertEquals(slice.getName() + " is inconsistent "
                + new ZkCoreNodeProps(replica).getCoreUrl(), found, count);
          }
          found = count;
        }
      }
    }
    log.info("stepTcheckconsistency2");
  }
  
  private void getShardUrlArgs(List<String> args, String replicatedCollection) {
    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getCollection(replicatedCollection).getSlices();
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
    Writer wr = new OutputStreamWriter(os, StandardCharsets.UTF_8);
    wr.write(DATADIR + File.separator + localFile);
    wr.close();
    
    assertTrue(fs.mkdirs(dataDir));
    fs.copyFromLocalFile(new Path(DOCUMENTS_DIR, localFile), dataDir);
    return INPATH;
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String name) throws Exception {
    putConfig(zkClient, solrhome, name, name);
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String srcName, String destName)
      throws Exception {
    
    File file = new File(solrhome, "conf" + File.separator + srcName);
    if (!file.exists()) {
      // LOG.info("skipping " + file.getAbsolutePath() +
      // " because it doesn't exist");
      return;
    }
    
    String destPath = "/configs/conf1/" + destName;
    // LOG.info("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
  }
  
  private void uploadConfFiles() throws Exception {
    // upload our own config files
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000);
    putConfig(zkClient, new File(RESOURCES_DIR + "/solr/solrcloud"),
        "solrconfig.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "schema.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "elevate.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_en.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ar.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_bg.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ca.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_cz.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_da.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_el.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_es.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_eu.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_de.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fa.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fi.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fr.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ga.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_gl.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hi.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hu.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hy.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_id.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_it.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ja.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_lv.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_nl.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_no.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_pt.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ro.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ru.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_sv.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_th.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_tr.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_ca.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_fr.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_ga.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_it.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stemdict_nl.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/hyphenations_ga.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "stopwords.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "protwords.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "currency.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "open-exchange-rates.json");
    putConfig(zkClient, MINIMR_CONF_DIR, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "old_synonyms.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "synonyms.txt");
    zkClient.close();
  }
  
  protected static <T> T[] concat(T[]... arrays) {
    if (arrays.length <= 0) {
      throw new IllegalArgumentException();
    }
    Class clazz = null;
    int length = 0;
    for (T[] array : arrays) {
      clazz = array.getClass();
      length += array.length;
    }
    T[] result = (T[]) Array.newInstance(clazz.getComponentType(), length);
    int pos = 0;
    for (T[] array : arrays) {
      System.arraycopy(array, 0, result, pos, array.length);
      pos += array.length;
    }
    return result;
  }
  
}
