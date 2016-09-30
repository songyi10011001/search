# Cloudera Search

Cloudera Search is [Apache Solr](http://lucene.apache.org/solr/) integrated with CDH,
including Apache Lucene, Apache SolrCloud, Apache Flume, Apache Hadoop MapReduce & HDFS,
and Apache Tika. Cloudera Search also includes integrations that make searching
more scalable, easy to use, and optimized for both near-real-time and batch-oriented indexing.

## Maven Modules

The following maven modules currently exist:

### search-mr

This module contains a flexible, scalable, fault tolerant, batch oriented system for processing large numbers of records
contained in files that are stored on HDFS into search indexes stored on HDFS.

`MapReduceIndexerTool` is a MapReduce batch job driver that takes a morphline and creates a set of Solr
index shards from a set of input files and writes the indexes into HDFS in a flexible, scalable,
and fault-tolerant manner. It also supports merging the output shards into a set of live
customer-facing Solr servers, typically a SolrCloud.

### search-contrib

This module contains additional sources to help with search.

### samples

This module contains example configurations and test data files.


## Mailing List

* [Posting](mailto:search-user@cloudera.org)
* [Archive](http://groups.google.com/a/cloudera.org/group/search-user)
* [Subscribe and Unsubcribe](http://groups.google.com/a/cloudera.org/group/search-user/subscribe)

## Documentation

* [Landing Page](http://tiny.cloudera.com/search-v1-docs-landing)
* [FAQ](http://tiny.cloudera.com/search-v1-faq)
* [Installation Guide](http://tiny.cloudera.com/search-v1-install-guide)
* [User Guide](http://tiny.cloudera.com/search-v1-user-guide)
* [Release Notes](http://tiny.cloudera.com/search-v1-release-notes)

## License

Cloudera Search is provided under the Apache Software License 2.0. See the file
`LICENSE.txt` for more information.

## Building

This step builds the software from source.

<pre>
git clone git@github.com:cloudera/search.git
cd search
#git checkout master
mvn clean package
ls search-dist/target/*.tar.gz
</pre>

## Integrating with Eclipse

* This section describes how to integrate the codeline with Eclipse.
* Build the software as described above. Then create Eclipse projects like this:
<pre>
cd search
mvn test -DskipTests eclipse:eclipse
</pre>
* `mvn test -DskipTests eclipse:eclipse` creates several Eclipse projects, one for each maven submodule.
It will also download and attach the jars of all transitive dependencies and their source code to the eclipse
projects, so you can readily browse around the source of the entire call stack.
* Then in eclipse do Menu `File/Import/Maven/Existing Maven Project/` on the root parent
directory `~/search` and select all submodules, then "Next" and "Finish".
* You will see some maven project errors that keep eclipse from building the workspace because
the eclipse maven plugin has some weird quirks and limitations. To work around this, next, disable
the maven "Nature" by clicking on the project in the browser, right clicking on Menu
`Maven/Disable Maven Nature`. This way you get all the niceties of the maven dependency management
without the hassle of the (current) maven eclipse plugin, everything compiles fine from within
Eclipse, and junit works and passes from within Eclipse as well.
* When a pom changes simply rerun `mvn test -DskipTests eclipse:eclipse` and
then run Menu `Eclipse/Refresh Project`. No need to disable the Maven "Nature" again and again.
* To run junit tests from within eclipse click on the project (e.g. `search-core` or `search-mr`, etc)
in the eclipse project explorer, right click, `Run As/JUnit Test`, and, for `search-mr`, additionally
make sure to give it the following VM arguments:
<pre>
-ea -Xmx512m -XX:MaxDirectMemorySize=256m -XX:MaxPermSize=128M
</pre>
