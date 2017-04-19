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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.crunch.types.avro.AvroInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.crunch.CrunchIndexerToolOptions.PipelineType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.parquet.avro.AvroParquetInputFormat;


/**
 * See http://argparse4j.sourceforge.net and for details see http://argparse4j.sourceforge.net/usage.html
 */
final class CrunchIndexerToolArgumentParser {

  private static final Logger LOG = LoggerFactory.getLogger(CrunchIndexerToolArgumentParser.class);
  
  private static final Map<String, String> INPUT_FORMAT_SUBSTITUTIONS = ImmutableMap.of(
      "text", TextInputFormat.class.getName(),
      "avro", AvroInputFormat.class.getName(),
      "avroParquet", AvroParquetInputFormat.class.getName()
      );

  /**
   * Parses the given command line arguments.
   *
   * @return exitCode null indicates the caller shall proceed with processing,
   *         non-null indicates the caller shall exit the program with the
   *         given exit status code.
   */
  public Integer parseArgs(String[] args, Configuration conf, CrunchIndexerToolOptions opts) {
    assert args != null;
    assert conf != null;
    assert opts != null;

    if (args.length == 0) {
      args = new String[] { "--help" };
    }

    final String descriptionHead = "Spark or MapReduce ETL";
    ArgumentParser parser = ArgumentParsers
        .newArgumentParser("", false)
        .defaultHelp(true)
        .description( 
            descriptionHead + " batch job that pipes data from (splittable or non-splittable) HDFS files "
            + "into Apache Solr, and along the way runs the "
            + "data through a Morphline for extraction and transformation. The program is designed for "
            + "flexible, scalable and fault-tolerant batch ETL pipeline jobs. It is implemented as an Apache Crunch pipeline "
            + "and as such can run on either the Apache Hadoop MapReduce or Apache Spark execution engine.\n"
            + "\n"
            + "The program proceeds in several consecutive phases, as follows: "
            + "\n\n"
            + "1) Randomization phase: This (parallel) phase randomizes the list of HDFS input files in order to spread "
            + "ingestion load more evenly among the mapper tasks of the subsequent phase. This phase is only executed for "
            + "non-splittables files, and skipped otherwise."
            + "\n\n"
            + "2) Extraction phase: This (parallel) phase emits a series of HDFS file input streams (for non-splittable files) "
            + "or a series of input data records (for splittable files). "
            + "\n\n"
            + "3) Morphline phase: This (parallel) phase receives the items of the previous "
            + "phase, and uses a Morphline to extract the relevant content, transform it and load zero or more documents "
            + "into Solr. The ETL functionality is flexible and customizable using chains of arbitrary "
            + "morphline commands that pipe records from one transformation command to another. Commands to parse and "
            + "transform a set of standard data formats such as Avro, Parquet, CSV, Text, HTML, XML, PDF, MS-Office, etc. "
            + "are provided out of the box, and additional custom commands and parsers for additional file or data formats "
            + "can be added as custom morphline commands. Any kind of data format can be "
            + "processed and any kind output format can be generated by any custom Morphline ETL logic. Also, this phase "
            + "can be used to send data directly to a live SolrCloud cluster (via the loadSolr morphline command)."
            + "\n\n"
            + "The program is implemented as a Crunch pipeline and as such Crunch optimizes the logical phases mentioned "
            + "above into an efficient physical execution plan that runs a single mapper-only job, "
            + "or as the corresponding Spark equivalent."
            + "\n\n"
            + "Fault Tolerance: Task attempts are retried on failure per the standard MapReduce or Spark "
            + "semantics. If the whole job fails you can retry simply by rerunning the program again "
            + "using the same arguments."
            + "\n\n"
            + "Comparison with MapReduceIndexerTool: "
            + "\n\n"
            + "1) CrunchIndexerTool can also run on the Spark execution engine, not just on MapReduce. \n"
            + "2) CrunchIndexerTool enables interactive low latency prototyping, in particular in Spark 'local' mode. \n"
            + "3) CrunchIndexerTool supports updates (and deletes) of existing documents in Solr, not just inserts. \n"
            + "4) CrunchIndexerTool can exploit data locality for splittable Hadoop files (text, avro, avroParquet). \n"
            + "We recommend MapReduceIndexerTool for large scale batch ingestion use cases where updates "
            + "(or deletes) of existing documents in Solr are not required, and we recommend CrunchIndexerTool for "
            + "all other use cases."
        );
    
    ArgumentGroup indexerArgGroup = parser.addArgumentGroup("CrunchIndexerOptions");
    
    // trailing positional arguments
    Argument inputFilesArg = indexerArgGroup.addArgument("input-files")
        .metavar("HDFS_URI")
        .type(new PathArgumentType(conf).verifyExists().verifyCanRead())
        .nargs("*")
        .setDefault()
        .help("HDFS URI of file or directory tree to ingest.");

    Argument inputFileListArg = indexerArgGroup.addArgument("--input-file-list", "--input-list")
        .action(Arguments.append())
        .metavar("URI")
        .type(new PathArgumentType(conf).acceptSystemIn().verifyExists().verifyCanRead())
        .help("Local URI or HDFS URI of a UTF-8 encoded file containing a list of HDFS URIs to ingest, " +
            "one URI per line in the file. If '-' is specified, URIs are read from the standard input. " +
            "Multiple --input-file-list arguments can be specified.");

    Argument inputFormatArg = indexerArgGroup.addArgument("--input-file-format")
        .metavar("FQCN")
        .type(String.class)
        .help("The Hadoop FileInputFormat to use for extracting data from splittable HDFS files. Can be a "
            + "fully qualified Java class name or one of ['text', 'avro', 'avroParquet']. If this option "
            + "is present the extraction phase will emit a series of input data records rather than a series "
            + "of HDFS file input streams.");

    Argument inputFileProjectionSchemaArg = indexerArgGroup.addArgument("--input-file-projection-schema")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to an Avro schema file on the local file system. This will be used "
            + "as the projection schema for Parquet input files.");

    Argument inputFileReaderSchemaArg = indexerArgGroup.addArgument("--input-file-reader-schema")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to an Avro schema file on the local file system. This will be used "
            + "as the reader schema for Avro or Parquet input files. "
            + "Example: src/test/resources/test-documents/strings.avsc");

    Argument morphlineFileArg = indexerArgGroup.addArgument("--morphline-file")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .required(true)
        .help("Relative or absolute path to a local config file that contains one or more morphlines. "
            + "The file must be UTF-8 encoded. It will be uploaded to each remote task. "
            + "Example: /path/to/morphline.conf");

    Argument morphlineIdArg = indexerArgGroup.addArgument("--morphline-id")
        .metavar("STRING")
        .type(String.class)
        .help("The identifier of the morphline that shall be executed within the morphline config file "
            + "specified by --morphline-file. If the --morphline-id option is omitted the first (i.e. "
            + "top-most) morphline within the config file is used. Example: morphline1");

    Argument pipelineTypeArg = indexerArgGroup.addArgument("--pipeline-type")
        .metavar("STRING")
        .type(PipelineType.class)
        .setDefault(PipelineType.mapreduce)
        .help("The engine to use for executing the job. Can be 'mapreduce' or 'spark'.");

    ArgumentGroup miscArgGroup = indexerArgGroup; //parser.addArgumentGroup("Misc arguments");

    miscArgGroup.addArgument("--xhelp", "--help", "-help")
        .help("Show this help message and exit")
        .action(new HelpArgumentAction() {
          @Override
          public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
            StringWriter strWriter = new StringWriter();
            parser.printHelp(new PrintWriter(strWriter, true));
            String help = strWriter.toString(); 
            int i = help.indexOf(descriptionHead);
            String description = help.substring(i).trim();
            String usage = help.substring("usage: ".length(), i).trim();
            System.out.println(
                      "MapReduceUsage: export HADOOP_CLASSPATH=$myDependencyJarPaths; hadoop jar $myDriverJar \n" + CrunchIndexerTool.class.getName()
                    + " --libjars $myDependencyJarFiles [MapReduceGenericOptions]...\n"
                    + "        " + usage + "\n"
                    + "\n"
                    + "SparkUsage: spark-submit [SparkGenericOptions]... "
                    + "--master local|yarn --deploy-mode client|cluster\n"
                    + "--jars $myDependencyJarFiles --class " + CrunchIndexerTool.class.getName() + " $myDriverJar\n" 
                    + "        " + usage + "\n"
                    + "\n"
                    + description + "\n"
                    + "\n"
                    + "SparkGenericOptions:     To print all options run 'spark-submit --help'\n"
                    + "\n"
                    + "MapReduceGenericOptions: " + ToolRunnerHelpFormatter.getGenericCommandUsage()
                    );
            System.out.println(
                      "Examples: \n\n" 
                    + "# Prepare - Copy input files into HDFS:\n"
                    + "export myResourcesDir=src/test/resources # for build from git\n"
                    + "export myResourcesDir=/opt/cloudera/parcels/CDH/share/doc/search-*/search-crunch # for CDH with parcels\n"
                    + "export myResourcesDir=/usr/share/doc/search-*/search-crunch # for CDH with packages\n"
                    + "hadoop fs -copyFromLocal $myResourcesDir/test-documents/hello1.txt hdfs:/user/systest/input/\n"
                    + "\n"
                    + "# Prepare variables for convenient reuse:\n"
                    + "export myDriverJarDir=target # for build from git\n"
                    + "export myDriverJarDir=/opt/cloudera/parcels/CDH/lib/solr/contrib/crunch # for CDH with parcels\n"
                    + "export myDriverJarDir=/usr/lib/solr/contrib/crunch # for CDH with packages\n"
                    + "export myDependencyJarDir=target/lib # for build from git\n"
                    + "export myDependencyJarDir=/opt/cloudera/parcels/CDH/lib/search/lib/search-crunch # for CDH with parcels\n"
                    + "export myDependencyJarDir=/usr/lib/search/lib/search-crunch # for CDH with packages\n"
                    + "export myDriverJar=$(find $myDriverJarDir -maxdepth 1 -name 'search-crunch-*.jar' ! -name '*-job.jar' ! -name '*-sources.jar')\n"
                    + "export myDependencyJarFiles=$(find $myDependencyJarDir -name '*.jar' | sort | tr '\\n' ',' | head -c -1)\n"
                    + "export myDependencyJarPaths=$(find $myDependencyJarDir -name '*.jar' | sort | tr '\\n' ':' | head -c -1)\n"
                    + "export myJVMOptions=\"-DmaxConnectionsPerHost=10000 -DmaxConnections=10000 -Djava.io.tmpdir=/my/tmp/dir/\" # connection settings for solrj, also custom tmp dir \n"
                    + "\n"
                    + "# MapReduce on Yarn - Ingest text file line by line into Solr:\n"
                    + "export HADOOP_CLIENT_OPTS=\"$myJVMOptions\"; export HADOOP_CLASSPATH=$myDependencyJarPaths; hadoop \\\n"
                    + "  --config /etc/hadoop/conf.cloudera.YARN-1 \\\n"
                    + "  jar $myDriverJar " + CrunchIndexerTool.class.getName() + " \\\n"
                    + "  --libjars $myDependencyJarFiles \\\n"
                    + "  -D mapreduce.map.java.opts=\"-Xmx500m $myJVMOptions\" \\\n"
                    + "  -D morphlineVariable.ZK_HOST=$(hostname):2181/solr \\\n"
                    + "  --files $myResourcesDir/test-documents/string.avsc \\\n"
                    + "  --morphline-file $myResourcesDir/test-morphlines/loadSolrLine.conf \\\n"
                    + "  --pipeline-type mapreduce \\\n"
                    + "  --chatty \\\n"
                    + "  --log4j $myResourcesDir/log4j.properties \\\n"
                    + "  /user/systest/input/hello1.txt\n"
                    + "\n"
                    + "# Spark in Local Mode (for rapid prototyping) - Ingest into Solr:\n"
                    + "spark-submit \\\n"
                    + "  --master local \\\n"
                    + "  --deploy-mode client \\\n"
                    + "  --jars $myDependencyJarFiles \\\n"
                    + "  --executor-memory 500M \\\n"
                    + "  --conf \"spark.executor.extraJavaOptions=$myJVMOptions\" \\\n"
                    + "  --driver-java-options \"$myJVMOptions\" \\\n"
                    + "  # --driver-library-path /opt/cloudera/parcels/CDH/lib/hadoop/lib/native # for Snappy on CDH with parcels\\\n"
                    + "  # --driver-library-path /usr/lib/hadoop/lib/native # for Snappy on CDH with packages \\\n"
                    + "  --class " + CrunchIndexerTool.class.getName() + " \\\n"
                    + "  $myDriverJar \\\n"
                    + "  -D morphlineVariable.ZK_HOST=$(hostname):2181/solr \\\n"
                    + "  --morphline-file $myResourcesDir/test-morphlines/loadSolrLine.conf \\\n"
                    + "  --pipeline-type spark \\\n"
                    + "  --chatty \\\n"
                    + "  --log4j $myResourcesDir/log4j.properties \\\n"
                    + "  /user/systest/input/hello1.txt\n"
                    + "\n"
                    + "# Spark on Yarn in Client Mode (for testing) - Ingest into Solr:\n"
                    + "Same as above, except replace '--master local' with '--master yarn'\n"
                    + "\n"
                    + "# View the yarn executor log files (there is no GUI yet):\n"
                    + "yarn logs --applicationId $application_XYZ\n"
                    + "\n"
                    + "# Spark on Yarn in Cluster Mode (for production) - Ingest into Solr:\n"
                    + "spark-submit \\\n"
                    + "  --master yarn \\\n"
                    + "  --deploy-mode cluster \\\n"
                    + "  --jars $myDependencyJarFiles \\\n"
                    + "  --executor-memory 500M \\\n"
                    //+ "  --num-executors 4 \\\n"
                    + "  --conf \"spark.executor.extraJavaOptions=$myJVMOptions\" \\\n"
                    + "  --driver-java-options \"$myJVMOptions\" \\\n"
                    + "  --class " + CrunchIndexerTool.class.getName() + " \\\n"
                    + "  --files $(ls $myResourcesDir/log4j.properties),$(ls $myResourcesDir/test-morphlines/loadSolrLine.conf)\\\n"
                    + "  $myDriverJar \\\n"
                    + "  -D hadoop.tmp.dir=/tmp \\\n"
                    + "  -D morphlineVariable.ZK_HOST=$(hostname):2181/solr \\\n"
                    + "  --morphline-file loadSolrLine.conf \\\n"
                    + "  --pipeline-type spark \\\n"
                    + "  --chatty \\\n"
                    + "  --log4j log4j.properties \\\n"
                    + "  /user/systest/input/hello1.txt\n"
                    + "\n"
                    + "# Spark on Yarn in Cluster Mode (for production) - Ingest into Secure (Kerberos-enabled) Solr:\n"
                    + "# Spark requires two additional steps compared to non-secure solr:\n"
                    + "# (NOTE: MapReduce does not require extra steps for communicating with kerberos-enabled Solr)\n"
                    + "# 1) Create a delegation token file\n"
                    + "#    a) kinit as the user who will make solr requests\n"
                    + "#    b) request a delegation token from solr and save it to a file:\n"
                    + "#       e.g. using curl:\n"
                    + "#       \"curl --negotiate -u: http://solr-host:port/solr/?op=GETDELEGATIONTOKEN > tokenFile.txt\"\n"
                    + "# 2) Pass the delegation token file to spark-submit:\n"
                    + "#    a) add the delegation token file via --files\n"
                    + "#    b) pass the file name via -D tokenFile\n"
                    + "#       spark places this file in the cwd of the executor, so only list the file name, no path\n"
                    + "spark-submit \\\n"
                    + "  --master yarn \\\n"
                    + "  --deploy-mode cluster \\\n"
                    + "  --jars $myDependencyJarFiles \\\n"
                    + "  --executor-memory 500M \\\n"
                    //+ "  --num-executors 4 \\\n"
                    + "  --conf \"spark.executor.extraJavaOptions=$myJVMOptions\" \\\n"
                    + "  --driver-java-options \"$myJVMOptions\" \\\n"
                    + "  --class " + CrunchIndexerTool.class.getName() + " \\\n"
                    + "  --files $(ls $myResourcesDir/log4j.properties),$(ls $myResourcesDir/test-morphlines/loadSolrLine.conf),tokenFile.txt\\\n"
                    + "  $myDriverJar \\\n"
                    + "  -D hadoop.tmp.dir=/tmp \\\n"
                    + "  -D morphlineVariable.ZK_HOST=$(hostname):2181/solr \\\n"
                    + "  -DtokenFile=tokenFile.txt \\\n"
                    + "  --morphline-file loadSolrLine.conf \\\n"
                    + "  --pipeline-type spark \\\n"
                    + "  --chatty \\\n"
                    + "  --log4j log4j.properties \\\n"
                    + "  /user/systest/input/hello1.txt\n"
                    + "\n"
            );
            throw new FoundHelpArgument(); // Trick to prevent processing of any remaining arguments
          }
        });

    Argument mappersArg = miscArgGroup.addArgument("--mappers")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(-1, Integer.MAX_VALUE)) // TODO: also support X% syntax where X is an integer
        .setDefault(-1)
        .help("Tuning knob that indicates the maximum number of MR mapper tasks to use. -1 indicates use all map slots " +
            "available on the cluster. This parameter only applies to non-splittable input files");

    Argument parallelMorphlineInits = miscArgGroup.addArgument("--parallel-morphline-inits")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
        .setDefault(MorphlineInitRateLimiter.UNLIMITED_PARALLEL_MORPHLINE_INITS)
        .help("Tuning knob that indicates the maximum number of morphline instances to initialize at the same time. "
            + "This kind of rate limiting on rampup can be useful to avoid overload conditions such as ZooKeeper "
            + "connection limits or DNS lookup limits when using many parallel mapper tasks because each such task "
            + "contains one morphline. 1 indicates initialize each morphline separately. "
            + "This feature is implemented with a distributed semaphore. "
            + "The default is to use no rate limiting");

    Argument noCommitArg = miscArgGroup.addArgument("--no-commit")
        .action(Arguments.storeTrue())
        .help(FeatureControl.SUPPRESS);

    Argument dryRunArg = miscArgGroup.addArgument("--dry-run")
        .action(Arguments.storeTrue())
        .help("Run the pipeline but print documents to stdout instead of loading them into Solr. " +
              "This can be used for quicker turnaround during early trial & debug sessions.");

    Argument log4jConfigFileArg = miscArgGroup.addArgument("--log4j")
        .metavar("FILE")
        .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
        .help("Relative or absolute path to a log4j.properties config file on the local file system. This file " +
            "will be uploaded to each remote task. Example: /path/to/log4j.properties");

    Argument verboseArg = miscArgGroup.addArgument("--chatty")
        .action(Arguments.storeTrue())
        .help("Turn on verbose output.");

    Namespace ns;
    try {
      ns = parser.parseArgs(args);
    } catch (FoundHelpArgument e) {
      return 0;
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return 1;
    }

    opts.log4jConfigFile = (File) ns.get(log4jConfigFileArg.getDest());
    if (opts.log4jConfigFile != null) {
      PropertyConfigurator.configure(opts.log4jConfigFile.getPath());
    }
    LOG.debug("Parsed command line args: {}", ns);

    opts.inputFileLists = getList(ns, inputFileListArg);
    opts.inputFiles = ns.get(inputFilesArg.getDest());
    opts.mappers = (Integer) ns.get(mappersArg.getDest());
    opts.morphlineFile = ns.get(morphlineFileArg.getDest());
    opts.morphlineId = ns.get(morphlineIdArg.getDest());
    opts.pipelineType = ns.get(pipelineTypeArg.getDest());
    opts.parallelMorphlineInits = (Integer) ns.get(parallelMorphlineInits.getDest());
    opts.isNoCommit = (Boolean) ns.get(noCommitArg.getDest());
    opts.isDryRun = (Boolean) ns.get(dryRunArg.getDest());
    opts.isVerbose = (Boolean) ns.get(verboseArg.getDest());

    try {
      opts.inputFileReaderSchema = parseSchema((File)ns.get(inputFileReaderSchemaArg.getDest()), parser);
      opts.inputFileProjectionSchema = parseSchema((File)ns.get(inputFileProjectionSchemaArg.getDest()), parser);
      opts.inputFileFormat = getClass(inputFormatArg, ns, FileInputFormat.class, parser, INPUT_FORMAT_SUBSTITUTIONS);
      
      String sparkMaster = System.getProperty("spark.master");
      if (opts.pipelineType == PipelineType.spark) {
        if (sparkMaster == null) {
          throw new ArgumentParserException("--pipeline-type=" + PipelineType.spark + " must not run as a MapReduce job", parser);
        }
      } else if (opts.pipelineType == PipelineType.mapreduce) {
        if (sparkMaster != null) {
          throw new ArgumentParserException("--pipeline-type=" + PipelineType.mapreduce + " must not run as a Spark job", parser);
        }
      }
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return 1;
    }

    return null;
  }
  
  private <T> T getList(Namespace ns, Argument arg) {
    T list = ns.get(arg.getDest());
    if (list == null) {
      list = (T) new ArrayList();
    }
    return list;
  }
  
  private Schema parseSchema(File file, ArgumentParser parser) throws ArgumentParserException {
    if (file == null) {
      return null;
    }
    try {
      return new Schema.Parser().parse(file);
    } catch (IOException e) {
      throw new ArgumentParserException(e, parser);
    }
  }
  
  private Class getClass(Argument arg, Namespace ns, Class baseClazz, ArgumentParser parser,
      Map<String, String> substitutions) throws ArgumentParserException {
    
    Class clazz = null;
    String className = ns.getString(arg.getDest());
    if (substitutions != null && substitutions.containsKey(className)) {
      className = substitutions.get(className);
      Preconditions.checkNotNull(className);
    }
    if (className != null) {
      try {
        clazz = Class.forName(className);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (!baseClazz.isAssignableFrom(clazz)) {
        throw new ArgumentParserException("--" + arg.getDest().replace('_', '-') + " " + className
            + " must be an instance of class " + baseClazz.getName(), parser);
      }
    }
    return clazz;
  }
  
  /** Marker trick to prevent processing of any remaining arguments once --help option has been parsed */
  private static final class FoundHelpArgument extends RuntimeException {
  }
  
}