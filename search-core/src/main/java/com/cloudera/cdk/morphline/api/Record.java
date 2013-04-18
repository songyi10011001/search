/**
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
package com.cloudera.cdk.morphline.api;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * A record is a set of named fields where each field has a list of one or more values.
 * 
 * A value can be of any type, i.e. any Java Object. That is, a record is a {@link ListMultimap} as
 * in Guava’s {@link ArrayListMultimap}. Note that a field can be multi-valued and that any two
 * records need not use common field names. This flexible data model corresponds exactly to the
 * characteristics of the Solr/Lucene data model (i.e. a record is a SolrInputDocument). A field
 * with zero values is removed from the record - it does not exist as such.
 */
public final class Record {
  
  public static final String ATTACHMENT_BODY = "_attachment_body";
  public static final String ATTACHMENT_MIME_TYPE = "_attachment_mimetype";
  public static final String ATTACHMENT_CHARSET = "_attachment_charset";
  public static final String ATTACHMENT_NAME = "_attachment_name";

  // logstash conventions:
  public static final String ID = "id";
  public static final String TYPE = "type";
  public static final String TIMESTAMP = "timestamp";
  public static final String TAGS = "tags";
  public static final String SOURCE_HOST = "source_host";
  public static final String SOURCE_URI = "source_uri";
  public static final String MESSAGE = "message";
  
  private ListMultimap<String, Object> fields;

  public Record() {
    this(create());
  }
  
  public Record(ListMultimap<String, Object> fields) {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  public Record(Record record) {
    this(ArrayListMultimap.create(record.getFields()));
  }

  public ListMultimap<String, Object> getFields() {
    return fields;
  }

  public Object getFirstValue(String key) {
    List values = fields.get(key);
    return values.size() > 0 ? values.get(0) : null;
  }
  
  public void replaceValues(String key, Object value) {
    fields.replaceValues(key, Collections.singletonList(value)); // TODO optimize?
  }
  
  public void removeAll(String key) {
    fields.removeAll(key);
  }
  
  @Override
  public boolean equals(Object other) {
    if (other instanceof Record) {
      return fields.equals(((Record)other).getFields());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return fields.hashCode();
  }
  
  @Override
  public String toString() {
    return fields.toString();
  }

  private static ListMultimap<String, Object> create() {
    return ArrayListMultimap.create();
  }
  
}