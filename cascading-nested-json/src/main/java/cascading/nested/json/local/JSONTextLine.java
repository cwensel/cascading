/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.nested.json.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.nested.json.JSONCoercibleType;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.local.Compressors;
import cascading.scheme.local.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A JSONTextLine is a type of {@link cascading.scheme.Scheme} for JSON text files. Files are broken into
 * lines, where each line is a JSON object. Either line-feed or carriage-return are used to signal end of line.
 * <p>
 * By default, this scheme returns a {@link Tuple} with one field, "json" with the type {@link JSONCoercibleType}.
 * <p>
 * Any {@link Fields} object passed to the constructor will have the JSONCoercibleType.TYPE type applied.
 * <p>
 * In order to read or write a compressed files, pass a {@link cascading.scheme.local.CompressorScheme.Compressor}
 * instance to the appropriate constructors. See {@link Compressors} for provided compression algorithms.
 *
 * @see Compressors
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
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with the
   * {@link cascading.flow.local.LocalFlowConnector} returning results with the default field named "json".
   */
  public JSONTextLine()
    {
    this( DEFAULT_FIELDS );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with the
   * {@link cascading.flow.local.LocalFlowConnector}.
   *
   * @param fields of Fields
   */
  public JSONTextLine( Fields fields )
    {
    this( fields, DEFAULT_CHARSET );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with the
   * {@link cascading.flow.local.LocalFlowConnector}.
   *
   * @param fields      of Fields
   * @param charsetName of String
   */
  public JSONTextLine( Fields fields, String charsetName )
    {
    this( fields, null, charsetName );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with the
   * {@link cascading.flow.local.LocalFlowConnector} returning results with the default field named "json".
   *
   * @param compressor of type Compressor, see {@link Compressors}
   */
  public JSONTextLine( Compressor compressor )
    {
    this( DEFAULT_FIELDS, compressor );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with the
   * {@link cascading.flow.local.LocalFlowConnector}.
   *
   * @param fields     of Fields
   * @param compressor of type Compressor, see {@link Compressors}
   */
  public JSONTextLine( Fields fields, Compressor compressor )
    {
    this( fields, compressor, DEFAULT_CHARSET );
    }

  /**
   * Constructor JSONTextLine creates a new JSONTextLine instance for use with the
   * {@link cascading.flow.local.LocalFlowConnector}.
   *
   * @param fields      of Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param charsetName of String
   */
  public JSONTextLine( Fields fields, Compressor compressor, String charsetName )
    {
    if( fields == null )
      throw new IllegalArgumentException( "fields may not be null" );

    if( !fields.isDefined() )
      throw new IllegalArgumentException( "fields argument must declare a single field" );

    if( fields.size() != 1 )
      throw new IllegalArgumentException( "may only declare a single source/sink field in the fields argument" );

    fields = fields.hasTypes() ? fields : fields.applyTypes( JSONCoercibleType.TYPE );

    setSinkFields( fields );
    setSourceFields( fields );

    setCompressor( compressor );

    // throws an exception if not found
    setCharsetName( charsetName );
    }

  @Override
  public boolean source( FlowProcess<? extends Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    String line = sourceCall.getContext().readLine();

    if( line == null )
      return false;

    TupleEntry incomingEntry = sourceCall.getIncomingEntry();

    JsonNode jsonNode = null;

    if( !line.isEmpty() )
      jsonNode = mapper.readTree( line.getBytes() );

    incomingEntry.setObject( 0, jsonNode );

    return true;
    }

  @Override
  public void sink( FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall ) throws IOException
    {
    JsonNode jsonNode = (JsonNode) sinkCall.getOutgoingEntry().getTuple().getObject( 0 );

    if( jsonNode == null )
      {
      sinkCall.getContext().println();
      }
    else
      {
      String string = mapper.writeValueAsString( jsonNode );

      sinkCall.getContext().println( string );
      }
    }
  }
