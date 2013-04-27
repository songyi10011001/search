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
package org.apache.solr.morphline.solrcell;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.handler.extraction.SolrContentHandlerFactory;
import org.apache.solr.morphline.SolrMorphlineContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.tika.ConfigurationException;
import org.apache.solr.tika.TrimSolrContentHandlerFactory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.Fields;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineParsingException;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.parser.AbstractParser;
import com.cloudera.cdk.morphline.tika.DetectMimeTypeBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Closeables;
import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
import com.typesafe.config.Config;

/**
 * Command that pipes the first attachment of a record into one of the given Tika parsers, then maps
 * the Tika output back to a record using SolrCell.
 * 
 * The Tika parser is chosen from the configurable list of parsers, depending on the MIME type
 * specified in the input record. Typically, this requires an upstream {@link DetectMimeTypeBuilder}
 * in a prior command.
 */
public final class SolrCellBuilder implements CommandBuilder {

  @Override
  public Set<String> getNames() {
    return Collections.singleton("solrCell");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new SolrCell(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class SolrCell extends AbstractParser {
    
    private final IndexSchema schema;
    private final List<String> dateFormats;
    private final String xpathExpr;
    private final List<Parser> parsers = new ArrayList();
    private final SolrContentHandlerFactory solrContentHandlerFactory;
    private final boolean decompressConcatenated; // TODO
    private AutoDetectParser autoDetectParser;
    
    private final SolrParams solrParams;
    private final Map<MediaType, Parser> mediaTypeToParserMap;
    
    private static final XPathParser PARSER = new XPathParser("xhtml", XHTMLContentHandler.XHTML);
        
    public static final String ADDITIONAL_SUPPORTED_MIME_TYPES = "additionalSupportedMimeTypes";
    
    public SolrCell(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      this.schema = ((SolrMorphlineContext)context).getIndexSchema();
      ListMultimap<String, String> cellParams = ArrayListMultimap.create();
      if (config.hasPath(ExtractingParams.UNKNOWN_FIELD_PREFIX)) {
        cellParams.put(ExtractingParams.UNKNOWN_FIELD_PREFIX, Configs.getString(config, ExtractingParams.UNKNOWN_FIELD_PREFIX));
      }
      for (String capture : Configs.getStringList(config, ExtractingParams.CAPTURE_ELEMENTS, Collections.EMPTY_LIST)) {
        cellParams.put(ExtractingParams.CAPTURE_ELEMENTS, capture);
      }
      if (config.hasPath("fmap")) {
        for (Map.Entry<String, Object> entry : config.getConfig("fmap").root().unwrapped().entrySet()) {
          cellParams.put(ExtractingParams.MAP_PREFIX + entry.getKey(), entry.getValue().toString());
        }
      }
      if (config.hasPath(ExtractingParams.CAPTURE_ATTRIBUTES)) {
        cellParams.put(ExtractingParams.CAPTURE_ATTRIBUTES, Configs.getString(config, ExtractingParams.CAPTURE_ATTRIBUTES));
      }
      if (config.hasPath(ExtractingParams.LOWERNAMES)) {
        cellParams.put(ExtractingParams.LOWERNAMES, Configs.getString(config, ExtractingParams.LOWERNAMES));
      }
      if (config.hasPath(ExtractingParams.DEFAULT_FIELD)) {
        cellParams.put(ExtractingParams.DEFAULT_FIELD, Configs.getString(config, ExtractingParams.DEFAULT_FIELD));
      }
      if (config.hasPath(ExtractingParams.XPATH_EXPRESSION)) {
        xpathExpr = Configs.getString(config, ExtractingParams.XPATH_EXPRESSION);
        cellParams.put(ExtractingParams.XPATH_EXPRESSION, xpathExpr);
      } else {
        xpathExpr = null;
      }
//      cellParams.put("fmap.content-type", "content_type");
      this.dateFormats = Configs.getStringList(config, "dateFormats", new ArrayList<String>(DateUtil.DEFAULT_DATE_FORMATS));
      this.decompressConcatenated = Configs.getBoolean(config, "decompressConcatenated", false);
      
      String handlerStr = Configs.getString(config, "solrContentHandlerFactory", TrimSolrContentHandlerFactory.class.getName());
      Class<? extends SolrContentHandlerFactory> factoryClass;
      try {
        factoryClass = (Class<? extends SolrContentHandlerFactory>)Class.forName(handlerStr);
      } catch (ClassNotFoundException cnfe) {
        throw new ConfigurationException("Could not find class "
          + handlerStr + " to use for " + "solrContentHandlerFactory", cnfe);
      }
      this.solrContentHandlerFactory = getSolrContentHandlerFactory(factoryClass, dateFormats, config);

      this.mediaTypeToParserMap = new HashMap<MediaType, Parser>();
      //MimeTypes mimeTypes = MimeTypes.getDefaultMimeTypes(); // FIXME getMediaTypeRegistry.normalize() 

      List<? extends Config> parserConfigs = Configs.getConfigList(config, "parsers");
      for (Config parserConfig : parserConfigs) {
        String parserClassName = Configs.getString(parserConfig, "parser");
        
        Object obj;
        try {
          obj = Class.forName(parserClassName).newInstance();
        } catch (Exception e) {
          throw new MorphlineParsingException("Cannot instantiate Tika parser", config, e);
        }
        if (!(obj instanceof Parser)) {
          throw new MorphlineParsingException("Tika parser " + obj.getClass().getName()
              + " must be an instance of class " + Parser.class.getName(), config);
        }
        Parser parser = (Parser) obj;
        this.parsers.add(parser);

        List<String> mediaTypes = Configs.getStringList(parserConfig, SUPPORTED_MIME_TYPES, Collections.EMPTY_LIST);
        for (String mediaTypeStr : mediaTypes) {
          MediaType mediaType = parseMediaType(mediaTypeStr).getBaseType();
          addSupportedMimeType(mediaType);
          this.mediaTypeToParserMap.put(mediaType, parser);
        }
        
        if (!parserConfig.hasPath(SUPPORTED_MIME_TYPES)) {
          for (MediaType mediaType : parser.getSupportedTypes(new ParseContext())) {
            mediaType = mediaType.getBaseType();
            addSupportedMimeType(mediaType);
            this.mediaTypeToParserMap.put(mediaType, parser);
          }        
          List<String> extras = Configs.getStringList(parserConfig, ADDITIONAL_SUPPORTED_MIME_TYPES, Collections.EMPTY_LIST);
          for (String mediaTypeStr : extras) {
            MediaType mediaType = parseMediaType(mediaTypeStr).getBaseType();
            addSupportedMimeType(mediaType);
            this.mediaTypeToParserMap.put(mediaType, parser);            
          }
        }
      }
      //LOG.info("mediaTypeToParserMap="+mediaTypeToParserMap);

      autoDetectParser = new AutoDetectParser(parsers.toArray(new Parser[parsers.size()]));

      Map<String, String[]> tmp = new HashMap();
      for (Map.Entry<String,Collection<String>> entry : cellParams.asMap().entrySet()) {
        tmp.put(entry.getKey(), entry.getValue().toArray(new String[entry.getValue().size()]));
      }
      this.solrParams = new MultiMapSolrParams(tmp);
    }

    @Override
    public boolean process(Record record, InputStream inputStream) {
      Parser parser = detectParser(record);
      if (parser == null) {
        return false;
      }
      
      //ParseInfo parseInfo = new ParseInfo(record, this, mediaTypeToParserMap, getContext().getMetricsRegistry()); // ParseInfo is more practical than ParseContext
      ParseContext parseContext = new ParseContext();
      
      // necessary for gzipped files or tar files, etc! copied from TikaCLI
      parseContext.set(Parser.class, parser);
      
      Metadata metadata = new Metadata();
      for (Entry<String, Object> entry : record.getFields().entries()) {
        metadata.add(entry.getKey(), entry.getValue().toString());
      }

      SolrContentHandler handler = solrContentHandlerFactory.createSolrContentHandler(metadata, solrParams, schema);
      
      try {
        inputStream = TikaInputStream.get(inputStream);

        ContentHandler parsingHandler = handler;
        StringWriter debugWriter = null;
        if (LOG.isTraceEnabled()) {
          debugWriter = new StringWriter();
          ContentHandler serializer = new XMLSerializer(debugWriter, new OutputFormat("XML", "UTF-8", true));
          parsingHandler = new TeeContentHandler(parsingHandler, serializer);
        }

        // String xpathExpr =
        // "/xhtml:html/xhtml:body/xhtml:div/descendant:node()";
        if (xpathExpr != null) {
          Matcher matcher = PARSER.parse(xpathExpr);
          parsingHandler = new MatchingContentHandler(parsingHandler, matcher);
        }

        //info.setSolrContentHandler(handler);

        // Standard tika parsers occasionally have trouble with gzip data.
        // To avoid this issue, pass a GZIPInputStream if appropriate.
        InputStreamMetadata  inputStreamMetadata = detectCompressInputStream(inputStream,  metadata);
        inputStream = inputStreamMetadata.inputStream;
        // It is possible the inputStreamMetadata.metadata has a modified RESOURCE_NAME from
        // the original metadata due to how we handle GZIPInputStreams.  Pass this to tika
        // so the correct parser will be invoked (i.e. not the built-in gzip parser).
        // We leave ParseInfo.metdata untouched so it contains the correct, original resourceName.
        metadata = inputStreamMetadata.metadata;

        try {
          parser.parse(inputStream, parsingHandler, metadata, parseContext);
        } catch (IOException e) {
          throw new MorphlineRuntimeException("Cannot parse", e);
        } catch (SAXException e) {
          throw new MorphlineRuntimeException("Cannot parse", e);
        } catch (TikaException e) {
          throw new MorphlineRuntimeException("Cannot parse", e);
        }
        
        LOG.trace("debug XML doc: {}", debugWriter);
        //LOG.trace("debug XML doc: {}", debugWriter);

//          if (info.isMultiDocumentParser()) {
//            return Collections.EMPTY_LIST; // loads were already handled by multidoc parser
//          }

      } finally {
        if (inputStream != null) {
          Closeables.closeQuietly(inputStream);
        }
      }
      
      SolrInputDocument doc = handler.newDocument();
      LOG.debug("solr doc: {}", doc);
      
//        return Collections.singletonList(doc);

      return getChild().process(toRecord(doc));
    }

    private Parser detectParser(Record record) {
      if (!hasAtLeastOneMimeType(record)) {
        return null;
      }
      String mediaTypeStr = (String) record.getFirstValue(Fields.ATTACHMENT_MIME_TYPE); //ExtractingParams.STREAM_TYPE);
      assert mediaTypeStr != null;
//      return autoDetectParser;
      MediaType mediaType = parseMediaType(mediaTypeStr).getBaseType();
      Parser parser = mediaTypeToParserMap.get(mediaType); // fast path
      if (parser != null) {
        return parser;
      }
      // wildcard matching
      for (Map.Entry<MediaType, Parser> entry : mediaTypeToParserMap.entrySet()) {
        if (isMediaTypeMatch(mediaType, entry.getKey())) {
          return entry.getValue();
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("No supported MIME type parser found for " + Fields.ATTACHMENT_MIME_TYPE + "=" + mediaTypeStr);
      }
      return null;
    }
    
    private static SolrContentHandlerFactory getSolrContentHandlerFactory(
        Class<? extends SolrContentHandlerFactory> factoryClass, Collection<String> dateFormats, Config config) {
      try {
        return factoryClass.getConstructor(Collection.class).newInstance(dateFormats);
      } catch (NoSuchMethodException nsme) {
        throw new MorphlineParsingException("Unable to find valid constructor of type "
          + factoryClass.getName() + " for creating SolrContentHandler", config, nsme);
      } catch (Exception e) {
        throw new MorphlineParsingException("Unexpected exception when trying to create SolrContentHandlerFactory of type "
          + factoryClass.getName(), config, e);
      }
    }

    private Record toRecord(SolrInputDocument doc) {
      Record record = new Record();
      for (Entry<String, SolrInputField> entry : doc.entrySet()) {
        record.getFields().replaceValues(entry.getKey(), entry.getValue().getValues());        
      }
      return record;
    }
    
    /**
     * @return an input stream/metadata tuple to use. If appropriate, stream will be capable of
     * decompressing concatenated compressed files.
     */
    private InputStreamMetadata detectCompressInputStream(InputStream inputStream, Metadata metadata) {
      if (decompressConcatenated) {
        String resourceName = metadata.get(Metadata.RESOURCE_NAME_KEY);
        if (resourceName != null && GzipUtils.isCompressedFilename(resourceName)) {
          try {
            CompressorInputStream cis = new GzipCompressorInputStream(inputStream, true);
            Metadata entryData = cloneMetadata(metadata);
            String newName = GzipUtils.getUncompressedFilename(resourceName);
            entryData.set(Metadata.RESOURCE_NAME_KEY, newName);
            return new InputStreamMetadata(cis, entryData);
          } catch (IOException ioe) {
            LOG.warn("Unable to create compressed input stream able to read concatenated stream", ioe);
          }
        }
      }
      return new InputStreamMetadata(inputStream, metadata);
    }

    /**
     * @return a clone of metadata
     */
    private Metadata cloneMetadata(Metadata metadata) {
      Metadata clone = new Metadata();
      for (String name : metadata.names()) {
        String [] str = metadata.getValues(name);
        for (int i = 0; i < str.length; ++i) {
          clone.add(name, str[i]);
        }
      }
      return clone;
    }

    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static final class InputStreamMetadata {
      
      private InputStream inputStream;
      private Metadata metadata;
   
      public InputStreamMetadata(InputStream inputStream, Metadata metadata) {
        this.inputStream = inputStream;
        this.metadata = metadata;
      }
    }
    
  }

}
