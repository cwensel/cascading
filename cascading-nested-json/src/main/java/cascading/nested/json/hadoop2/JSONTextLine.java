/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.nested.json.hadoop2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import cascading.flow.FlowProcess;
import cascading.nested.json.JSONCoercibleType;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 * A JSONTextLine is a type of {@link cascading.scheme.Scheme} for JSON text files. Files are broken into
 * lines, where each line is a JSON object. Either line-feed or carriage-return are used to signal end of line.
 * <p>
 * By default, this scheme returns a {@link Tuple} with one field, "json" with the type {@link JSONCoercibleType}.
 * <p>
 * Any {@link Fields} object passed to the constructor will have the JSONCoercibleType.TYPE type applied.
 * <p>
 * To create a binary JSON file, use the {@link cascading.scheme.hadoop.SequenceFile} Scheme with one or more
 * fields having the JSONCoercibleType type.
 * <p>
 * Note, when supplying a custom {@link ObjectMapper}, the default {@link JSONCoercibleType#TYPE} and ObjectMapper
 * sets the {@link DeserializationFeature#FAIL_ON_READING_DUP_TREE_KEY} Jackson property.
 */
public class JSONTextLine extends TextLine
  {
  public static final Fields DEFAULT_FIELDS = new Fields( "json" ).applyTypes( JSONCoercibleType.TYPE );

  private ObjectMapper mapper = new ObjectMapper();

  {
  // prevents json object from being created with duplicate names at the same level
  mapper.setConfig( mapper.getDeserializationConfig()
    .with( DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY ) );
  }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances returning results
   * with the default field named "json".
   */
  public JSONTextLine()
    {
    this( DEFAULT_FIELDS );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param fields of Fields
   */
  public JSONTextLine( Fields fields )
    {
    this( fields, null, DEFAULT_CHARSET );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param fields      of Fields
   * @param charsetName of String
   */
  public JSONTextLine( Fields fields, String charsetName )
    {
    this( fields, null, charsetName );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param fields          of Fields
   * @param sinkCompression of Compress
   */
  public JSONTextLine( Fields fields, Compress sinkCompression )
    {
    this( fields, sinkCompression, DEFAULT_CHARSET );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param fields          of Fields
   * @param sinkCompression of Compress
   * @param charsetName     of String
   */
  public JSONTextLine( Fields fields, Compress sinkCompression, String charsetName )
    {
    this( null, fields, sinkCompression, charsetName );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param mapper of ObjectMapper
   * @param fields of Fields
   */
  public JSONTextLine( ObjectMapper mapper, Fields fields )
    {
    this( mapper, fields, null, DEFAULT_CHARSET );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param mapper      of ObjectMapper
   * @param fields      of Fields
   * @param charsetName of String
   */
  public JSONTextLine( ObjectMapper mapper, Fields fields, String charsetName )
    {
    this( mapper, fields, null, charsetName );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param mapper          of ObjectMapper
   * @param fields          of Fields
   * @param sinkCompression of Compress
   */
  public JSONTextLine( ObjectMapper mapper, Fields fields, Compress sinkCompression )
    {
    this( mapper, fields, sinkCompression, DEFAULT_CHARSET );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with any of the
   * Hadoop based {@link cascading.flow.FlowConnector} instances.
   *
   * @param mapper          of ObjectMapper
   * @param fields          of Fields
   * @param sinkCompression of Compress
   * @param charsetName     of String
   */
  public JSONTextLine( ObjectMapper mapper, Fields fields, Compress sinkCompression, String charsetName )
    {
    super( sinkCompression );

    if( mapper != null )
      this.mapper = mapper;

    if( fields == null )
      throw new IllegalArgumentException( "fields may not be null" );

    if( !fields.isDefined() )
      throw new IllegalArgumentException( "fields argument must declare a single field" );

    if( fields.size() != 1 )
      throw new IllegalArgumentException( "may only declare a single source/sink field in the fields argument" );

    fields = fields.hasTypes() ? fields : fields.applyTypes( new JSONCoercibleType( this.mapper ) );

    setSinkFields( fields );
    setSourceFields( fields );

    // throws an exception if not found
    setCharsetName( charsetName );
    }

  @Override
  protected void sourceHandleInput( SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    TupleEntry result = sourceCall.getIncomingEntry();

    Object[] context = sourceCall.getContext();

    Text text = (Text) context[ 1 ];
    JsonNode jsonNode = null;

    if( text.getLength() != 0 )
      {
      ByteArrayInputStream inputStream = new ByteArrayInputStream( text.getBytes(), 0, text.getLength() );
      InputStreamReader reader = new InputStreamReader( inputStream, (Charset) context[ 2 ] );
      jsonNode = mapper.readTree( reader );
      }

    result.setObject( 0, jsonNode );
    }

  @Override
  public void sink( FlowProcess<? extends Configuration> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    Text text = (Text) sinkCall.getContext()[ 0 ];
    Charset charset = (Charset) sinkCall.getContext()[ 1 ];

    JsonNode jsonNode = (JsonNode) sinkCall.getOutgoingEntry().getTuple().getObject( 0 );

    if( jsonNode == null )
      {
      text.set( "" );
      }
    else
      {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream( 1024 );
      OutputStreamWriter writer = new OutputStreamWriter( outputStream, charset );

      mapper.writeValue( writer, jsonNode );

      writer.close();

      text.set( outputStream.toByteArray() );
      }

    // it's ok to use NULL here so the collector does not write anything
    sinkCall.getOutput().collect( null, text );
    }
  }
