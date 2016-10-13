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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.io.text.NLineFileSource;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.avro.AvroInputFormat;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.crunch.CrunchIndexerToolOptions.PipelineType;
import org.apache.solr.security.util.job.JobSecurityUtil;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.TypedSettings;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.solr.LoadSolrBuilder;
import org.kitesdk.morphline.solr.SolrLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import parquet.avro.AvroParquetInputFormat;


/**
 * CrunchIndexerTool is a Spark or MapReduce ETL batch job that pipes data from (splitable or
 * non-splitable) HDFS files into Apache Solr, and along the way runs the data through a Morphline
 * for extraction and transformation. The program is designed for flexible, scalable and
 * fault-tolerant batch ETL pipeline jobs. It is implemented as an Apache Crunch pipeline and as
 * such can run on either the Apache Hadoop MapReduce or Apache Spark execution engine.
 */
public class CrunchIndexerTool extends Configured implements Tool {

  @VisibleForTesting
  PipelineResult pipelineResult = null;
  
  private Path tmpFile = null;
  
  static final String MAIN_MEMORY_RANDOMIZATION_THRESHOLD =
      CrunchIndexerTool.class.getName() + ".mainMemoryRandomizationThreshold";
  
  /**
   * Morphline variables can be passed from CLI to the morphline, e.g.:
   * hadoop ... -D morphlineVariable.zkHost=127.0.0.1:2181/solr
   */
  static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";
  
  /**
   * Token file location passed from CLI via -D option; to support secure delegation.
   */
  static final String TOKEN_FILE_PARAM = "tokenFile";
  
  /**
   * In a secure setup, represents the configuration prop that
   * has the serviceName for which the credentials are valid.
   */
  static final String SECURE_CONF_SERVICE_NAME =
    "org.apache.solr.crunch.security.service.name";
  
  static final String PARALLEL_MORPHLINE_INITS = 
    CrunchIndexerTool.class.getName() + ".parallelMorphlineInits";

  static final String ZK_ENSEMBLE_FOR_PARALLEL_MORPHLINE_INITS = 
    CrunchIndexerTool.class.getName() + ".zkEnsembleForParallelMorphlineInits";
    
  private static final Logger LOG = LoggerFactory.getLogger(CrunchIndexerTool.class);

  /** API for command line clients */
  public static void main(String[] args) throws Exception  {
    int exitCode = ToolRunner.run(new Configuration(), new CrunchIndexerTool(), args);
    System.exit(exitCode);
  }

  public CrunchIndexerTool() {}

  @Override
  public int run(String[] args) throws Exception {
    CrunchIndexerToolOptions opts = new CrunchIndexerToolOptions();
    Integer exitCode = new CrunchIndexerToolArgumentParser().parseArgs(args, getConf(), opts);
    if (exitCode == null) {
      exitCode = run(opts);
    }
    if (exitCode.intValue() != 0 && opts.pipelineType == PipelineType.spark) {
      throw new RuntimeException("CrunchIndexerTool Spark pipeline failed"); // CDH-45888
    }
    return exitCode;
  }

  /** API for Java clients; visible for testing; may become a public API eventually */
  int run(CrunchIndexerToolOptions opts) throws Exception {
    long programStartTime = System.currentTimeMillis();
    
    if (opts.log4jConfigFile != null) {
      Utils.setLogConfigFile(opts.log4jConfigFile, getConf());
    }

    LOG.info("Initializing ...");

    String morphlineFileContents = Files.toString(opts.morphlineFile, Charsets.UTF_8);
    Map<String, String> morphlineVariables = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : getConf()) {
      String variablePrefix = MORPHLINE_VARIABLE_PARAM + ".";
      if (entry.getKey().startsWith(variablePrefix)) {
        morphlineVariables.put(entry.getKey().substring(variablePrefix.length()), entry.getValue());
      }
    }

    Config override = ConfigFactory.parseMap(morphlineVariables);
    Config config = new Compiler().parse(opts.morphlineFile, override);
    Config morphlineConfig = new Compiler().find(opts.morphlineId, config, opts.morphlineFile.getPath());
    CredentialsExecutor credentialsExecutor = null;

    MorphlineInitRateLimiter initRateLimiter = null;
    getConf().setInt(PARALLEL_MORPHLINE_INITS, opts.parallelMorphlineInits);
    if (opts.parallelMorphlineInits <= 0) {
      throw new IllegalArgumentException("parallelMorphlineInits must be a positive number: " + opts.parallelMorphlineInits);
    }
    LOG.info("Using parallelMorphlineInits: {}", opts.parallelMorphlineInits);
    if (opts.parallelMorphlineInits != MorphlineInitRateLimiter.UNLIMITED_PARALLEL_MORPHLINE_INITS) {
      String zkEnsemble = getZkEnsembleForParallelMorphlineInit(morphlineConfig);
      initRateLimiter = new MorphlineInitRateLimiter(getConf(), zkEnsemble, true);
    }
    
    int mappers = 1;
    Pipeline pipeline; // coordinates pipeline creation and execution
    if (opts.pipelineType == PipelineType.memory) {
      pipeline = MemPipeline.getInstance();
      pipeline.setConfiguration(getConf());
    } else if (opts.pipelineType == PipelineType.mapreduce) {
      Configuration mrConf = getConf();
      SolrLocator secureLocator = System.getProperty("java.security.auth.login.config") != null ? 
          getSecureSolrLocator(morphlineConfig) : null;
      if (secureLocator != null) {
        String serviceName = secureLocator.getZkHost() != null ?
          secureLocator.getZkHost() : secureLocator.getServerUrl();
        Job job = new Job(getConf());
        SolrServer solrServer = secureLocator.getSolrServer();
        credentialsExecutor = new JobCredentialsExecutor(solrServer, serviceName, job);
        JobSecurityUtil.initCredentials(solrServer, job, serviceName);
        // pass the serviceName to the DoFn via the conf
        job.getConfiguration().set(SECURE_CONF_SERVICE_NAME, serviceName);
        mrConf = job.getConfiguration();
      }
      pipeline = new MRPipeline(
          getClass(), 
          Utils.getShortClassName(getClass()), 
          mrConf);

      mappers = new JobClient(pipeline.getConfiguration()).getClusterStatus().getMaxMapTasks(); // MR1
      //mappers = job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn only
      LOG.info("Cluster reports {} mapper slots", mappers);
      
      int reducers = new JobClient(pipeline.getConfiguration()).getClusterStatus().getMaxReduceTasks(); // MR1
      //reducers = job.getCluster().getClusterStatus().getReduceSlotCapacity(); // Yarn only      
      LOG.info("Cluster reports {} reduce slots", reducers);
    } else if (opts.pipelineType == PipelineType.spark) {
      String master = System.getProperty("spark.master");
      String appName = System.getProperty("spark.app.name");
      if (appName == null || appName.equals(getClass().getName())) {
        appName = Utils.getShortClassName(getClass());
      }
      String tokenFileParam = getConf().get(TOKEN_FILE_PARAM);
      SolrLocator secureLocator = tokenFileParam != null ? getSecureSolrLocator(morphlineConfig) : null;
      if (secureLocator != null) {
        String serviceName = secureLocator.getZkHost() != null ?
          secureLocator.getZkHost() : secureLocator.getServerUrl();
        credentialsExecutor = new FileCredentialsExecutor(secureLocator.getSolrServer(), serviceName, getConf());
        JobSecurityUtil.initCredentials(new File(tokenFileParam), getConf(), serviceName);
        // pass the serviceName to the DoFn via the conf
        getConf().set(SECURE_CONF_SERVICE_NAME, serviceName);
      }
      pipeline = new SparkPipeline(master, appName, null, getConf());
    } else {
      throw new IllegalArgumentException("Unsupported --pipeline-type: " + opts.pipelineType);
    }

    this.tmpFile = null;    
    try {
      PCollection collection = extractInputCollection(opts, mappers, pipeline);
      if (collection == null) {
        return 0;
      }
      
      Map<String, Object> settings = new HashMap<String, Object>();
      settings.put(TypedSettings.DRY_RUN_SETTING_NAME, opts.isDryRun);
      
      DoFn morphlineFn = new MorphlineFnBuilder().
          morphlineFileContents(morphlineFileContents). 
          morphlineId(opts.morphlineId). 
          morphlineVariables(morphlineVariables).
          morphlineSettings(settings).
          isSplittable(opts.inputFileFormat != null).
          build();
      
      collection = collection.parallelDo(
          "morphline",
          morphlineFn, 
          Avros.nulls() // trick to enable morphline to emit any kind of output data, including non-avro data
          );
      
      collection = collection.parallelDo(
          FilterFns.REJECT_ALL(), // aka dropRecord
          Avros.nulls() // trick to enable morphline to emit any kind of output data, including non-avro data
          );
      
      writeOutput(opts, pipeline, collection);
        
      if (!done(pipeline, opts, morphlineConfig, credentialsExecutor)) {
        return 1; // job failed
      }
    } finally {
      try {
        // FIXME fails for yarn-cluster mode with spark unless hdfs permissions are fixed on tmp dir
        if (tmpFile != null) {
          FileSystem tmpFs = tmpFile.getFileSystem(pipeline.getConfiguration());
          delete(tmpFile, false, tmpFs);
          tmpFile = null;
        }
      } finally {
        try {
          if (initRateLimiter != null) {
            initRateLimiter.close();
          }
        } finally {
          if (credentialsExecutor != null) {
            credentialsExecutor.cleanupCredentials();
          }
        }
      }
    }
    float secs = (System.currentTimeMillis() - programStartTime) / 1000.0f;
    LOG.info("Success. Done. Program took {} secs. Goodbye.", secs);
    return 0;
  }

  private PCollection extractInputCollection(CrunchIndexerToolOptions opts, int mappers, Pipeline pipeline)
      throws IOException {
    
    if (opts.inputFiles.isEmpty() && opts.inputFileLists.isEmpty()) {
      return null;
    }
    
    // handle input files (not Kite datasets)
    tmpFile = new Path(
        pipeline.getConfiguration().get("hadoop.tmp.dir", "/tmp"), 
        getClass().getName() + "-" + UUID.randomUUID().toString());
    FileSystem tmpFs = tmpFile.getFileSystem(pipeline.getConfiguration());          
    LOG.debug("Creating list of input files for mappers: {}", tmpFile);
    long numFiles = addInputFiles(opts.inputFiles, opts.inputFileLists, tmpFile, pipeline.getConfiguration());
    if (numFiles == 0) {
      LOG.info("No input files found - nothing to process");
      return null;
    }
 
    if (opts.inputFileFormat != null) { // handle splitable input files
      LOG.info("Using these parameters: numFiles: {}", numFiles);
      List<Path> filePaths = new ArrayList<Path>();
      for (String file : listFiles(tmpFs, tmpFile)) {
        filePaths.add(new Path(file));
      }
      if (opts.inputFileFormat.isAssignableFrom(AvroInputFormat.class)) { 
        if (opts.inputFileReaderSchema == null) {
          return pipeline.read(From.avroFile(filePaths, pipeline.getConfiguration()));
        } else {
          return pipeline.read(From.avroFile(filePaths, Avros.generics(opts.inputFileReaderSchema)));
        }
      } else if (opts.inputFileFormat.isAssignableFrom(AvroParquetInputFormat.class)) {
        if (opts.inputFileReaderSchema == null) {
          // TODO: for convenience we should extract the schema from the parquet data files. 
          // (i.e. we should do the same as above for avro files).
          throw new IllegalArgumentException(
              "--input-file-reader-schema must be specified when using --input-file-format=avroParquet");
        }
        Schema schema = opts.inputFileReaderSchema;
        Source source = new AvroParquetFileSource(filePaths, Avros.generics(schema), opts.inputFileProjectionSchema);
        return pipeline.read(source);
      } else if (opts.inputFileFormat.isAssignableFrom(TextInputFormat.class)) {
        Source source = From.textFile(filePaths);
        return pipeline.read(source);
      } else {
        // FIXME drop support for this stuff? (doesn't seem to work with spark)
        // TODO: intentionally restrict to only allow org.apache.hadoop.mapreduce.lib.input.TextInputFormat ?
        TableSource source = new FileTableSourceImpl(
            filePaths,
            WritableTypeFamily.getInstance().tableOf(Writables.longs(), Writables.strings()), 
            //AvroTypeFamily.getInstance().tableOf(Avros.nulls(), Avros.nulls()), 
            //AvroTypeFamily.getInstance().tableOf(Avros.longs(), Avros.generics(opts.inputFileSchema)), 
            opts.inputFileFormat);
        return pipeline.read(source).values();                 
      }
    } else { // handle non-splitable input files
      if (opts.mappers == -1) { 
        mappers = 8 * mappers; // better accomodate stragglers
      } else {
        mappers = opts.mappers;
      }
      if (mappers <= 0) {
        throw new IllegalStateException("Illegal number of mappers: " + mappers);
      }
      
      int numLinesPerSplit = (int) ceilDivide(numFiles, mappers);
      if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
        numLinesPerSplit = Integer.MAX_VALUE;
      }
      numLinesPerSplit = Math.max(1, numLinesPerSplit);

      int realMappers = Math.min(mappers, (int) ceilDivide(numFiles, numLinesPerSplit));
      LOG.info("Using these parameters: numFiles: {}, mappers: {}, realMappers: {}",
          new Object[] {numFiles, mappers, realMappers});

      boolean randomizeFewInputFiles = 
          numFiles < pipeline.getConfiguration().getInt(MAIN_MEMORY_RANDOMIZATION_THRESHOLD, 100001);
      if (randomizeFewInputFiles) {
        // If there are few input files reduce latency by directly running main memory randomization 
        // instead of launching a high latency MapReduce job
        randomizeFewInputFiles(tmpFs, tmpFile);
      }
 
      PCollection collection = pipeline.read(new NLineFileSource<String>(tmpFile, Writables.strings(), numLinesPerSplit));

      if (!randomizeFewInputFiles) {
        collection = randomize(collection); // uses a high latency MapReduce job
      }
      collection = collection.parallelDo(new HeartbeatFn(), collection.getPType());
      return collection;
    }
  }
  
  private void writeOutput(CrunchIndexerToolOptions opts, Pipeline pipeline, PCollection collection) {
    if (true) {
      // switch off Crunch lazy evaluation, yet don't write to a Crunch Target
      pipeline.materialize(collection);
//      for (Object item : pipeline.materialize(collection)) {
//        LOG.debug("item: {}", item);
//      }
//    } else {
//      throw new UnsupportedOperationException();
////      Target target = To.formattedFile(opts.outputDir, opts.outputFileFormat);
////      pipeline.write(collection, target, opts.outputWriteMode);
    }
  }
    
  private long addInputFiles(List<Path> inputFiles, List<Path> inputLists, 
      Path fullInputList, Configuration conf) throws IOException {
    
    long numFiles = 0;
    FileSystem fs = fullInputList.getFileSystem(conf);
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new BufferedWriter(new OutputStreamWriter(out, Charsets.UTF_8));
      
      for (Path inputFile : inputFiles) {
        FileSystem inputFileFs = inputFile.getFileSystem(conf);
        if (inputFileFs.exists(inputFile)) {
          PathFilter pathFilter = new PathFilter() {      
            @Override
            public boolean accept(Path path) { // ignore "hidden" files and dirs
              return !(path.getName().startsWith(".") || path.getName().startsWith("_")); 
            }
          };
          numFiles += addInputFilesRecursively(inputFile, writer, inputFileFs, pathFilter);
        }
      }

      for (Path inputList : inputLists) {
        InputStream in;
        if (inputList.toString().equals("-")) {
          in = System.in;
        } else if (inputList.isAbsoluteAndSchemeAuthorityNull()) {
          in = new BufferedInputStream(new FileInputStream(inputList.toString()));
        } else {
          in = inputList.getFileSystem(conf).open(inputList);
        }
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
          String line;
          while ((line = reader.readLine()) != null) {
            writer.write(line + "\n");
            numFiles++;
          }
          reader.close();
        } finally {
          in.close();
        }
      }
      
      writer.close();
    } finally {
      out.close();
    }    
    return numFiles;
  }
  
  /**
   * Add the specified file to the input set, if path is a directory then
   * add the files contained therein.
   */
  private long addInputFilesRecursively(Path path, Writer writer, FileSystem fs, PathFilter pathFilter) throws IOException {
    long numFiles = 0;
    for (FileStatus stat : fs.listStatus(path, pathFilter)) {
      LOG.debug("Adding path {}", stat.getPath());
      if (stat.isDirectory()) {
        numFiles += addInputFilesRecursively(stat.getPath(), writer, fs, pathFilter);
      } else {
        writer.write(stat.getPath().toString() + "\n");
        numFiles++;
      }
    }
    return numFiles;
  }
  
  private List<String> listFiles(FileSystem fs, Path fullInputList) throws IOException {    
    List<String> lines = new ArrayList<String>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(fs.open(fullInputList), Charsets.UTF_8));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    } finally {
      reader.close();
    }
    return lines;
  }

  private void randomizeFewInputFiles(FileSystem fs, Path fullInputList) throws IOException {    
    List<String> lines = listFiles(fs, fullInputList);
    
    Collections.shuffle(lines, new Random(421439783L)); // constant seed for reproducability
    
    FSDataOutputStream out = fs.create(fullInputList);
    Writer writer = new BufferedWriter(new OutputStreamWriter(out, Charsets.UTF_8));
    try {
      for (String line : lines) {
        writer.write(line + "\n");
      } 
    } finally {
      writer.close();
    }
  }

  /** Randomizes the order of the items in the collection via a MapReduce job */
  private static <T> PCollection<T> randomize(PCollection<T> items) {
    PTable<Long, T> table = items.by("randomize", new RandomizeFn<T>(), Writables.longs());
    table = Sort.sort(table, Sort.Order.ASCENDING);
    return table.values();
  }

  private boolean done(Pipeline job, CrunchIndexerToolOptions opts, Config morphlineConfig,
      CredentialsExecutor credentialsExecutor) {
    boolean isVerbose = opts.isVerbose;
    if (isVerbose) {
      job.getConfiguration().setBoolean(RuntimeParameters.LOG_JOB_PROGRESS, true);
    }    
    String name = job.getName();
    LOG.debug("Running pipeline: " + name);
    pipelineResult = job.done();
    boolean success = pipelineResult.succeeded();
    if (success) {      
      if (!opts.isNoCommit) {
        commitSolr(morphlineConfig, opts.isDryRun, credentialsExecutor);
      }
      LOG.info("Succeeded with pipeline: " + name + " " + getJobInfo(pipelineResult, isVerbose));
    } else {
      LOG.error("Pipeline failed: " + name + " " + getJobInfo(pipelineResult, isVerbose));
    }
    return success;
  }

  // Implements CDH-22673 (CrunchIndexerTool should send a commit to Solr on job success)
  private void commitSolr(Config morphlineConfig, boolean isDryRun, CredentialsExecutor credentialsExecutor) {
    Set<Map<String,Object>> solrLocatorMaps = new LinkedHashSet();
    String loadSolrName = new LoadSolrBuilder().getNames().iterator().next();
    Preconditions.checkNotNull(loadSolrName);    
    collectSolrLocators(morphlineConfig.root().unwrapped(), loadSolrName, solrLocatorMaps);
    LOG.debug("Committing Solr at all of: {} ", solrLocatorMaps);
    boolean shouldLoadCredentials = credentialsExecutor != null;
    for (Map<String,Object> solrLocatorMap : solrLocatorMaps) {
      LOG.info("Preparing commit of Solr at {}", solrLocatorMap);
      SolrLocator solrLocator = new SolrLocator(
          ConfigFactory.parseMap(solrLocatorMap),
          new MorphlineContext.Builder().build());
      SolrServer solrServer = solrLocator.getSolrServer();
      float secs;
      try {
        long start = System.currentTimeMillis();
        if (!isDryRun) {
          if (shouldLoadCredentials) {
            credentialsExecutor.loadCredentialsForClients();
          }
          LOG.info("Committing Solr at {}", solrLocatorMap);
          solrServer.commit();
        }
        shouldLoadCredentials = false;
        secs = (System.currentTimeMillis() - start) / 1000.0f;
      } catch (SolrServerException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        solrServer.shutdown();
      }
      
      if (isDryRun) {
        LOG.info("Skipped committing Solr because --dry-run option is specified");          
      } else {
        LOG.info("Done committing Solr. Commit took " + secs + " secs");
      }
    }        
  }
  
  private void collectSolrLocators(Object configNode, String loadSolrName, Set<Map<String,Object>> solrLocators) {
    if (configNode instanceof List) {
      for (Object item : (List)configNode) {
        collectSolrLocators(item, loadSolrName, solrLocators);
      }
    } else if (configNode instanceof Map) {
      for (Map.Entry<String,Object> entry : ((Map<String,Object>) configNode).entrySet()) {
        if (loadSolrName == null || entry.getKey().equals(loadSolrName)) {
          if (entry.getValue() instanceof Map) {
            Map<String,Object> loadSolrMap = (Map<String,Object>)entry.getValue();
            Object solrLocator = loadSolrMap.get(LoadSolrBuilder.SOLR_LOCATOR_PARAM);
            if (solrLocator instanceof Map) {
              solrLocators.add((Map<String,Object>)solrLocator);
            }
          }
          if (loadSolrName == null) {
            collectSolrLocators(entry.getValue(), loadSolrName, solrLocators);
          }
        } else {
          collectSolrLocators(entry.getValue(), loadSolrName, solrLocators);
        }
      }
    }
  }

  private String getZkEnsembleForParallelMorphlineInit(Config morphlineConfig) {
    String zkEnsemble = getConf().get(ZK_ENSEMBLE_FOR_PARALLEL_MORPHLINE_INITS);
    if (zkEnsemble == null) {
      Set<Map<String,Object>> solrLocatorMaps = new LinkedHashSet();
      collectSolrLocators(morphlineConfig.root().unwrapped(), null, solrLocatorMaps);
      for (Map<String,Object> solrLocatorMap : solrLocatorMaps) {
        SolrLocator solrLocator = new SolrLocator(
          ConfigFactory.parseMap(solrLocatorMap),
          new MorphlineContext.Builder().build());
        zkEnsemble = solrLocator.getZkHost();
        if (zkEnsemble != null) {
          break;
        }
      }
      Preconditions.checkNotNull(zkEnsemble, 
        "Missing solrLocator in morphline config file for use with --parallel-morphline-inits");
      getConf().set(ZK_ENSEMBLE_FOR_PARALLEL_MORPHLINE_INITS, zkEnsemble);
    }
    return zkEnsemble;
  }

  private SolrLocator getSecureSolrLocator(Config morphlineConfig) {
    Set<Map<String,Object>> solrLocatorMaps = new LinkedHashSet();
    String loadSolrName = new LoadSolrBuilder().getNames().iterator().next();
    Preconditions.checkNotNull(loadSolrName);
    collectSolrLocators(morphlineConfig.root().unwrapped(), loadSolrName, solrLocatorMaps);
    SolrLocator zkHostLocator = null;
    SolrLocator solrUrlLocator = null;
    for (Map<String,Object> solrLocatorMap : solrLocatorMaps) {
      SolrLocator solrLocator = new SolrLocator(
        ConfigFactory.parseMap(solrLocatorMap),
        new MorphlineContext.Builder().build());
      if (solrLocator.getZkHost() != null) {
        if (zkHostLocator == null) {
          zkHostLocator = solrLocator;
        } else if (!solrLocator.getZkHost().equals(zkHostLocator.getZkHost())) {
          LOG.warn("For a secure job, found SolrLocator that species zkHost: "
            + solrLocator.getZkHost() + " when a previous SolrLocator already "
            + "defined a different zkHost: " + zkHostLocator.getZkHost() + ".  Only specifying "
            + "the same zkHost is supported in secure mode, job may fail.");
        }
      } else if (solrLocator.getServerUrl() != null) {
        if (solrUrlLocator == null) {
          solrUrlLocator = solrLocator;
        } else if (!solrLocator.getServerUrl().equals(solrUrlLocator.getServerUrl())) {
          LOG.warn("For a secure job, found SolrLocator that species serverUrl: "
            + solrLocator.getServerUrl() + " when a previous SolrLocator already "
            + "defined a different serverUrl: " + solrUrlLocator.getServerUrl() + ".  Only specifying "
            + "the same serverUrl is supported in secure mode, job may fail.");
        }
      }
    }
    if (zkHostLocator != null && solrUrlLocator != null) {
      LOG.warn("For a secure job, found SolrLocator that species serverUrl: "
        + solrUrlLocator.getServerUrl() + " when a previous SolrLocator already "
        + "defined a zkHost: " + zkHostLocator.getServerUrl() + ".  Specifying multiple "
        + "SolrLocators, where one specifies serverUrl and another specifies zkHost "
        + "is not supported in secure mode (zkHost is preferred), job may fail");
    }
    return zkHostLocator != null ? zkHostLocator : solrUrlLocator;
  }

  private String getJobInfo(PipelineResult job, boolean isVerbose) {
    StringBuilder buf = new StringBuilder();
    for (StageResult stage : job.getStageResults()) {
      buf.append("\nstageId: " + stage.getStageId() + ", stageName: " + stage.getStageName());
      if (isVerbose) {
        buf.append(", counters: ");
        Map<String, Set<String>> sortedCounterMap = new TreeMap<String, Set<String>>(stage.getCounterNames());
        for (Map.Entry<String, Set<String>> entry : sortedCounterMap.entrySet()) {
          String groupName = entry.getKey();
          buf.append("\n" + groupName);
          Set<String> sortedCounterNames = new TreeSet<String>(entry.getValue());
          for (String counterName : sortedCounterNames) {
            buf.append("\n    " + counterName + " : " + stage.getCounterValue(groupName, counterName));
          }
        }
      }
    }
    return buf.toString();
  }
  
  private boolean delete(Path path, boolean recursive, FileSystem fs) throws IOException {
    boolean success = fs.delete(path, recursive);
    if (!success) {
      LOG.error("Cannot delete " + path);
    }
    return success;
  }

  // same as IntMath.divide(p, q, RoundingMode.CEILING)
  private long ceilDivide(long p, long q) {
    long result = p / q;
    if (p % q != 0) {
      result++;
    }
    return result;
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class RandomizeFn<T> extends MapFn<T, Long> {
    
    private transient Random prng;

    @Override
    public void initialize() {
      // create a good random seed, yet ensure deterministic PRNG sequence for easy reproducability
      long taskId = getContext().getTaskAttemptID().getTaskID().getId(); // taskId = 0, 1, ..., N
      prng = new Random(421439783L * (taskId + 1));
    }

    @Override
    public Long map(T input) {
      return prng.nextLong();
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static abstract class CredentialsExecutor {

    protected SolrServer solrServer;
    protected String serviceName;

    public CredentialsExecutor(SolrServer solrServer, String serviceName) {
      this.solrServer = solrServer;
      this.serviceName = serviceName;
    }

    public abstract void cleanupCredentials() throws IOException, SolrServerException;
    public abstract void loadCredentialsForClients() throws IOException;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class JobCredentialsExecutor extends CredentialsExecutor {
    
    private Job job;

    public JobCredentialsExecutor(SolrServer solrServer, String serviceName, Job job) {
      super(solrServer, serviceName);
      this.job = job;
    }

    @Override
    public void cleanupCredentials() throws IOException, SolrServerException {
      try {
        JobSecurityUtil.cleanupCredentials(solrServer, job, serviceName);
      } finally {
        solrServer.shutdown();
      }
    }

    @Override
    public void loadCredentialsForClients() throws IOException {
      JobSecurityUtil.loadCredentialsForClients(job, serviceName);
    }
  }
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class FileCredentialsExecutor extends CredentialsExecutor {
    
    private Configuration conf;

    public FileCredentialsExecutor(SolrServer solrServer, String serviceName, Configuration conf) {
      super(solrServer, serviceName);
      this.conf = conf;
    }

    @Override
    public void cleanupCredentials() throws IOException, SolrServerException {
       try {
         JobSecurityUtil.cleanupCredentials(solrServer, conf, serviceName);
       } finally {
         solrServer.shutdown();
       }
    }

    @Override
    public void loadCredentialsForClients() throws IOException {
      JobSecurityUtil.loadCredentialsForClients(conf, serviceName);
    }
  }
}
