/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crunch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.test.TemporaryPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.crunch.CrunchIndexerToolOptions.PipelineType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.solr.AbstractSolrMorphlineZkTest;
import org.kitesdk.morphline.stdlib.DropRecordBuilder;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ObjectArrays;

//@ThreadLeakAction({Action.WARN})
//@ThreadLeakAction({Action.INTERRUPT})
//@ThreadLeakLingering(linger = 0)
//@ThreadLeakZombies(Consequence.CONTINUE)
//@ThreadLeakZombies(Consequence.IGNORE_REMAINING_TESTS)
@ThreadLeakScope(Scope.NONE)
//@ThreadLeakScope(Scope.TEST)
@SuppressCodecs({"Lucene3x", "Lucene40"})
public class MemoryCrunchIndexerToolTest extends AbstractSolrMorphlineZkTest {
  
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Rule
  public Timeout globalTimeout= new Timeout(180*1000);

  private PipelineType pipelineType;
  private boolean isRandomizingWithDoFn;
  private boolean isDryRun;
  private int numExpectedFailedRecords;
  private int numExpectedExceptionRecords;
  private int parallelMorphlineInits;

  private static final String SCHEMA_FILE = "src/test/resources/test-documents/string.avsc";
  private static final String NOP = "nop.conf";
  private static final String LOAD_SOLR_LINE = "loadSolrLine.conf";
  private static final String EXTRACT_AVRO_PATH = "extractAvroPath.conf";  
  private static final String READ_AVRO_PARQUET_FILE = "readAvroParquetFile.conf";  
  private static final String FAIL_FILE = "fail.conf";  
  private static final String THROW_EXCEPTION_FILE = "throwException.conf";  
  private static final String RESOURCES_DIR = "target/test-classes";
  
  
  public MemoryCrunchIndexerToolTest() {
    this(PipelineType.memory, false);
  }
  
  protected MemoryCrunchIndexerToolTest(PipelineType pipelineType, boolean isDryRun) {
    this.pipelineType = pipelineType;
    this.isDryRun = isDryRun;
  }
    
  @Before
  public void setUp() throws Exception {
    System.setProperty("solr.tests.cloud.cm.enabled", "false"); // disable Solr ChaosMonkey
    super.setUp();
    cluster.getSolrClient().setDefaultCollection("collection1");
  }

  @Test
  public void doTest() throws Exception {
    resetTest();
    testStreamTextInputFile();
    resetTest();
    testSplittableTextInputFile();    
    resetTest();
    testSplittableAvroFile();    
    resetTest();
    testSplittableAvroFileWithReaderSchema();
    resetTest();
    testSplittableAvroFileWithDryRun();
    resetTest();
    testSplittableAvroParquetFile();
    resetTest();
    testStreamAvroParquetFile();
    resetTest();
    testStreamTextInputFiles();
    resetTest();
    testStreamTextInputFileWithNoCommit();
    resetTest();
    testFileList();
    resetTest();
    testFileListWithScheme();
    resetTest();
    testRecursiveInputDir();
    resetTest();
    testRandomizeInputFiles();
    resetTest();
    testHelp();
    resetTest();
    testHelpWithoutArgs();
    resetTest();
    testCommandThatFails();
    if (pipelineType != PipelineType.spark) { // FIXME
      resetTest();
      testCommandThatThrowsException();
      resetTest();
      testIllegalCommandLineArgument();
      resetTest();
      testIllegalCommandLineClassNameArgument();
    }
    cluster.getSolrClient().close();
  }
  
  @Override
  protected void commit() throws Exception {
    morphline = new DropRecordBuilder().build(null, null, null, null); // just a dummy to make the superclass happy
    super.commit();
  }
  
  private void resetTest() throws SolrServerException, IOException {
    //tmpDir.delete();
    isRandomizingWithDoFn = false;
    numExpectedFailedRecords = 0;
    numExpectedExceptionRecords = 0;
    parallelMorphlineInits = MorphlineInitRateLimiter.UNLIMITED_PARALLEL_MORPHLINE_INITS;
    
    cluster.getSolrClient().deleteByQuery("*:*"); // delete everything!
    cluster.getSolrClient().commit();
  }
  
  private String[] getInitialArgs(String morphlineConfigFile) {
    String[] args = new String[] { 
        "--log4j=" + RESOURCES_DIR + "/log4j.properties",
        "--chatty",
        "--pipeline-type=" + pipelineType,
        "--parallel-morphline-inits=" + parallelMorphlineInits,
    };
    if (morphlineConfigFile != null) {
      args = ObjectArrays.concat(args, "--morphline-file=" + RESOURCES_DIR + "/test-morphlines/" + morphlineConfigFile);
      args = ObjectArrays.concat(args, "--morphline-id=morphline1");
    }
    if (isDryRun) {
      args = ObjectArrays.concat(args, "--dry-run");      
    }
    return args;
  }

  private void testStreamTextInputFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(LOAD_SOLR_LINE);
    args = ObjectArrays.concat(args, inputPath);    
    PipelineResult pipelineResult = runIntoSolr(args, expected);
    Assert.assertTrue(pipelineResult.getStageResults().get(0).getCounterValue("morphline", "morphline.app.numRecords") > 0);
  }
  
  private void testSplittableTextInputFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs("readSplittableLines.conf");
    args = ObjectArrays.concat(args, "--input-file-format=text");
    args = ObjectArrays.concat(args, inputPath);
    runIntoSolr(args, expected);
  }

  private void testSplittableAvroFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.avro");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(EXTRACT_AVRO_PATH);
    args = ObjectArrays.concat(args, "--input-file-format=avro");
    args = ObjectArrays.concat(args, inputPath);    
    runIntoSolr(args, expected);    
  }
  
  private void testSplittableAvroFileWithReaderSchema() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.avro");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(EXTRACT_AVRO_PATH);
    args = ObjectArrays.concat(args, "--input-file-format=avro");
    args = ObjectArrays.concat(args, "--input-file-reader-schema=" + SCHEMA_FILE);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoSolr(args, expected);    
  }
  
  private void testSplittableAvroFileWithDryRun() throws Exception {
    boolean oldValue = isDryRun;
    isDryRun = true;
    try {
      testSplittableAvroFile();
    } finally {
      isDryRun = oldValue;
    }
  }
  
  private void testSplittableAvroParquetFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.parquet");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(EXTRACT_AVRO_PATH);
    args = ObjectArrays.concat(args, "--input-file-format=avroParquet");
    args = ObjectArrays.concat(args, "--input-file-reader-schema=" + SCHEMA_FILE);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoSolr(args, expected);    
  }
  
  private void testStreamAvroParquetFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/strings-2.parquet");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(READ_AVRO_PARQUET_FILE);
    args = ObjectArrays.concat(args, inputPath);    
    runIntoSolr(args, expected);    
  }
  
  private void testStreamTextInputFiles() throws Exception {
    String inputPath1 = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String inputPath2 = tmpDir.copyResourceFileName("test-documents/hello2.txt");
    
    String[] expected = new String[] {
        "hello foo", 
        "hello world",
        "hello2 file", 
        };
    String[] args = getInitialArgs(LOAD_SOLR_LINE);
    args = ObjectArrays.concat(args, new String[]{inputPath1, inputPath2}, String.class);    
    runIntoSolr(args, expected);
  }
  
  private void testStreamTextInputFileWithNoCommit() throws Exception {
    boolean oldValue = isDryRun;
    isDryRun = true;
    try {
      String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
      String[] expected = new String[] {"hello foo", "hello world"};
      String[] args = getInitialArgs(LOAD_SOLR_LINE);
      args = ObjectArrays.concat(args, inputPath);    
      args = ObjectArrays.concat(args, "--no-commit");    
      PipelineResult pipelineResult = runIntoSolr(args, expected);
      Assert.assertTrue(pipelineResult.getStageResults().get(0).getCounterValue("morphline", "morphline.app.numRecords") > 0);
    } finally {
      isDryRun = oldValue;
    }
  }
  
  private void testFileList() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/filelist1.txt");
    String[] expected = new String[] {"hello foo", "hello world", "hello2 file"};
    parallelMorphlineInits = 1;
    String[] args = getInitialArgs(LOAD_SOLR_LINE);
    args = ObjectArrays.concat(args, "--input-file-list=" + inputPath);    
    runIntoSolr(args, expected);
  }
  
  private void testFileListWithScheme() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/filelist1.txt");
    String[] expected = new String[] {"hello foo", "hello world", "hello2 file"};
    parallelMorphlineInits = 1;
    String[] args = getInitialArgs(LOAD_SOLR_LINE);
    args = ObjectArrays.concat(args, "--input-file-list=file:" + inputPath);    
    runIntoSolr(args, expected);
  }
  
  private void testRecursiveInputDir() throws Exception {
    String[] expected = new String[] {"hello nadja"};
    String[] args = getInitialArgs(LOAD_SOLR_LINE);
    args = ObjectArrays.concat(args, RESOURCES_DIR + "/test-documents/subdir");    
    runIntoSolr(args, expected);
  }
  
  private void testRandomizeInputFiles() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {"hello foo", "hello world"};
    String[] args = getInitialArgs(LOAD_SOLR_LINE);
    args = ObjectArrays.concat(args, inputPath);
    isRandomizingWithDoFn = true;
    runIntoSolr(args, expected);    
  }
  
  private void testCommandThatFails() throws Exception {
    if (pipelineType == PipelineType.memory) {
      return;
    }
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] expected = new String[] {};
    String[] args = getInitialArgs(FAIL_FILE);
    args = ObjectArrays.concat(args, inputPath);    
    numExpectedFailedRecords = 1;
    PipelineResult pipelineResult = runIntoSolr(args, expected);
    Assert.assertTrue(pipelineResult.getStageResults().get(0).getCounterValue("morphline", "morphline.app.numRecords") > 0);
  }
  
  private void testCommandThatThrowsException() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(THROW_EXCEPTION_FILE);
    args = ObjectArrays.concat(args, inputPath);    
    numExpectedExceptionRecords = 1;
    if (pipelineType == PipelineType.mapreduce) {
      runPipeline(args, true);
    } else {
      try {
        runPipeline(args, true);
        Assert.fail();
      } catch (MorphlineRuntimeException e) {
        ; // expected
      }
    }
  }
  
  private void testHelp() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(NOP);
    args = ObjectArrays.concat(args, inputPath);    
    args = ObjectArrays.concat(args, "--help");
    CrunchIndexerTool tool = new CrunchIndexerTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    Assert.assertEquals(0, res);
    Assert.assertNull(tool.pipelineResult);
  }
  
  private void testHelpWithoutArgs() throws Exception {
    String[] args = new String[0];
    CrunchIndexerTool tool = new CrunchIndexerTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    Assert.assertEquals(0, res);
    Assert.assertNull(tool.pipelineResult);
  }
  
  private void testIllegalCommandLineArgument() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(NOP);
    args = ObjectArrays.concat(args, "--illegalParam=foo");
    args = ObjectArrays.concat(args, inputPath);    
    CrunchIndexerTool tool = new CrunchIndexerTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    Assert.assertEquals(1, res);
    Assert.assertNull(tool.pipelineResult);
  }
  
  private void testIllegalCommandLineClassNameArgument() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("test-documents/hello1.txt");
    String[] args = getInitialArgs(NOP);
    args = ObjectArrays.concat(args, "--input-file-format=" + ProcessBuilder.class.getName());
    args = ObjectArrays.concat(args, inputPath);    
    CrunchIndexerTool tool = new CrunchIndexerTool();
    int res = ToolRunner.run(tmpDir.getDefaultConfiguration(), tool, args);
    Assert.assertEquals(1, res);
    Assert.assertNull(tool.pipelineResult);
  }
  
  private PipelineResult runIntoSolr(String[] args, String[] expected) throws Exception {
    PipelineResult pipelineResult = runPipeline(args);    
    if (!isDryRun) {
      List<Map<String, Object>> records;
      records = new ArrayList();
      commit();        
      QueryResponse rsp = cluster.getSolrClient().query(new SolrQuery("*:*").setRows(100000).addSort("text", SolrQuery.ORDER.asc));
      //System.out.println(rsp);
      Iterator<SolrDocument> iter = rsp.getResults().iterator();
      while (iter.hasNext()) {
        SolrDocument doc = iter.next();
        System.out.println("mydoc = "+ doc);
        records.add(ImmutableMap.of("text", doc.getFirstValue("text")));
      }
      records = sort(records);
      
      Assert.assertEquals(expected.length, records.size());
      for (int i = 0; i < expected.length; i++) {
        Assert.assertEquals(ImmutableMap.of("text", expected[i]), records.get(i));
      }
    }    
    return pipelineResult;
  }

  private PipelineResult runPipeline(String[] args) throws Exception {
    return runPipeline(args, false);
  }

  private PipelineResult runPipeline(String[] args, boolean expectFailure) throws Exception {
    CrunchIndexerTool tool = new CrunchIndexerTool();
    Configuration config = tmpDir.getDefaultConfiguration();
    config.set(CrunchIndexerTool.MORPHLINE_VARIABLE_PARAM + ".ZK_HOST", cluster.getZkServer().getZkAddress());
    config.set(CrunchIndexerTool.MORPHLINE_VARIABLE_PARAM + ".myMorphlineVar", "foo");
    if (isRandomizingWithDoFn) {
      config.setInt(CrunchIndexerTool.MAIN_MEMORY_RANDOMIZATION_THRESHOLD, -1); 
    }
    int res = ToolRunner.run(config, tool, args);
    if (expectFailure) {
      Assert.assertEquals(1, res);
      Assert.assertFalse(tool.pipelineResult.succeeded());      
      Assert.assertEquals(0, tool.pipelineResult.getStageResults().size());
    } else {
      if (System.getProperty(LocalSparkIndexerToolTest.SPARK_MASTER) != null) {
        Assert.assertEquals(CrunchIndexerTool.MAGIC_SPARK_SUCCESS_EXIT_CODE, res);
      } else {
        Assert.assertEquals(0, res);
      }
      Assert.assertTrue(tool.pipelineResult.succeeded());      
      Assert.assertEquals(1, tool.pipelineResult.getStageResults().size());
      StageResult stageResult = tool.pipelineResult.getStageResults().get(0);
      Assert.assertEquals(numExpectedFailedRecords, stageResult.getCounterValue("morphline", "morphline.app.numFailedRecords"));
      Assert.assertEquals(numExpectedExceptionRecords, stageResult.getCounterValue("morphline", "morphline.app.numExceptionRecords"));
    }
    return tool.pipelineResult;
  }
  
  private List<Map<String, Object>> sort(List<Map<String, Object>> records) {
    Collections.sort(records, new Comparator<Map>() {

      @Override
      public int compare(Map o1, Map o2) {
        Comparable c1 = Iterables.toArray(o1.values(), Comparable.class)[0];
        Comparable c2 = Iterables.toArray(o2.values(), Comparable.class)[0];
        return c1.compareTo(c2);
      }
      
    });
    return records;    
  }    
}
