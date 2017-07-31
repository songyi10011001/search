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

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.crunch.CrunchIndexerToolOptions.PipelineType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@ThreadLeakScope(Scope.NONE)
@SuppressCodecs({"Lucene3x", "Lucene40"})
public class LocalSparkIndexerToolTest extends MemoryCrunchIndexerToolTest {

  @Rule
  public Timeout globalTimeout= new Timeout(180*1000);

  private String oldSparkMaster;
  
  static final String SPARK_MASTER = "spark.master";

  public LocalSparkIndexerToolTest() {
    this(false);
  }
  
  protected LocalSparkIndexerToolTest(boolean isDryRun) {
    super(PipelineType.spark, isDryRun);
  }
  
  @Before
  public void setUp() throws Exception {
    oldSparkMaster = System.getProperty(SPARK_MASTER);
    super.setUp();
    System.setProperty(SPARK_MASTER, "local");
  }
  
  @After
  public void tearDown() throws Exception {
    if (oldSparkMaster == null) {
      System.clearProperty(SPARK_MASTER);
    } else {
      System.setProperty(SPARK_MASTER, oldSparkMaster);
    }
    super.tearDown();
  }

}
