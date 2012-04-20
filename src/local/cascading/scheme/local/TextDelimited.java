/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.scheme.local;

import java.beans.ConstructorProperties;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class TextDelimited is a sub-class of {@link TextLine}. It provides direct support for delimited text files, like
 * TAB (\t) or COMMA (,) delimited files. It also optionally allows for quoted values.
 * <p/>
 * TextDelimited may also be used to skip the "header" in a file, where the header is defined as the very first line
 * in every input file. That is, if the byte offset of the current line from the input is zero (0), that line will
 * be skipped.
 * <p/>
 * It is assumed if sink/source {@code fields} is set to either {@link Fields#ALL} or {@link Fields#UNKNOWN} and
 * {@code skipHeader} or {@code hasHeader} is {@code true}, the field names will be retrieved from the header of the
 * file and used during planning. The header will parsed with the same rules as the body of the file.
 * <p/>
 * By default headers are not skipped.
 * <p/>
 * TextDelimited may also be used to write a "header" in a file. The fields names for the header are taken directly
 * from the declared fields. Or if the declared fields are {@link Fields#ALL} or {@link Fields#UNKNOWN}, the
 * resolved field names will be used, if any.
 * <p/>
 * By default headers are not written.
 * <p/>
 * If {@code hasHeaders} is set to {@code true} on a constructor, both {@code skipHeader} and {@code writeHeader} will
 * be set to {@code true}.
 * <p/>
 * By default this {@link cascading.scheme.Scheme} is both {@code strict} and {@code safe}.
 * <p/>
 * Strict meaning if a line of text does not parse into the expected number of fields, this class will throw a
 * {@link TapException}. If strict is {@code false}, then {@link Tuple} will be returned with {@code null} values
 * for the missing fields.
 * <p/>
 * Safe meaning if a field cannot be coerced into an expected type, a {@code null} will be used for the value.
 * If safe is {@code false}, a {@link TapException} will be thrown.
 * <p/>
 * Also by default, {@code quote} strings are not searched for to improve processing speed. If a file is
 * COMMA delimited but may have COMMA's in a value, the whole value should be surrounded by the quote string, typically
 * double quotes ({@literal "}).
 * <p/>
 * Note all empty fields in a line will be returned as {@code null} unless coerced into a new type.
 * <p/>
 * This Scheme may source/sink {@link Fields#ALL}, when given on the constructor the new instance will automatically
 * default to strict == false as the number of fields parsed are arbitrary or unknown. A type array may not be given
 * either, so all values will be returned as Strings.
 *
 * @see TextLine
 */
public class TextDelimited extends LocalScheme<LineNumberReader, PrintWriter, Void, StringBuilder>
  {
  private final boolean skipHeader;
  private final boolean writeHeader;
  private final DelimitedParser delimitedParser;

  /**
   * Constructor TextDelimited creates a new TextDelimited instance with TAB as the default delimiter.
   *
   * @param fields of type Fields
   */
  @ConstructorProperties({"fields"})
  public TextDelimited( Fields fields )
    {
    this( fields, "\t", null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param delimiter of type String
   */
  @ConstructorProperties({"fields", "delimiter"})
  public TextDelimited( Fields fields, String delimiter )
    {
    this( fields, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter )
    {
    this( fields, hasHeader, hasHeader, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter )
    {
    this( fields, skipHeader, writeHeader, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param delimiter of type String
   * @param types     of type Class[]
   */
  @ConstructorProperties({"fields", "delimiter", "types"})
  public TextDelimited( Fields fields, String delimiter, Class[] types )
    {
    this( fields, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param types     of type Class[]
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, Class[] types )
    {
    this( fields, hasHeader, hasHeader, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param types       of type Class[]
   */
  @ConstructorProperties({"fields", "skipHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, Class[] types )
    {
    this( fields, skipHeader, writeHeader, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param delimiter of type String
   * @param quote     of type String
   * @param types     of type Class[]
   */
  @ConstructorProperties({"fields", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, String delimiter, String quote, Class[] types )
    {
    this( fields, false, delimiter, quote, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param quote     of type String
   * @param types     of type Class[]
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, hasHeader, hasHeader, delimiter, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param types       of type Class[]
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, skipHeader, writeHeader, delimiter, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param delimiter of type String
   * @param quote     of type String
   * @param types     of type Class[]
   * @param safe      of type boolean
   */
  @ConstructorProperties({"fields", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, false, delimiter, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param quote     of type String
   * @param types     of type Class[]
   * @param safe      of type boolean
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, hasHeader, hasHeader, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param types       of type Class[]
   * @param safe        of type boolean
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, skipHeader, writeHeader, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param delimiter of type String
   * @param quote     of type String
   */
  @ConstructorProperties({"fields", "delimiter", "quote"})
  public TextDelimited( Fields fields, String delimiter, String quote )
    {
    this( fields, false, delimiter, quote, null, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param quote     of type String
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, String quote )
    {
    this( fields, hasHeader, delimiter, quote, null, true );
    }


  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param strict      of type boolean
   * @param quote       of type String
   * @param types       of type Class[]
   * @param safe        of type boolean
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter", "strict", "quote", "types", "safe"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe )
    {
    super( fields, fields );

    // normalizes ALL and UNKNOWN
    setSinkFields( fields );
    setSourceFields( fields );

    this.skipHeader = skipHeader;
    this.writeHeader = writeHeader;

    delimitedParser = new DelimitedParser( delimiter, quote, types, strict, safe, skipHeader, getSourceFields(), getSinkFields() );
    }

  @Override
  public LineNumberReader createInput( FileInputStream inputStream )
    {
    try
      {
      return new LineNumberReader( new InputStreamReader( inputStream, "UTF-8" ) );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new TapException( exception );
      }
    }

  @Override
  public PrintWriter createOutput( FileOutputStream outputStream )
    {
    try
      {
      return new PrintWriter( new OutputStreamWriter( outputStream, "UTF-8" ) );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new TapException( exception );
      }
    }

  @Override
  public boolean isSymmetrical()
    {
    return super.isSymmetrical() && skipHeader == writeHeader;
    }

  @Override
  public Fields retrieveSourceFields( FlowProcess<Properties> process, Tap tap )
    {
    if( !skipHeader || !getSourceFields().isUnknown() )
      return getSourceFields();

    // no need to open them all
    if( tap instanceof CompositeTap )
      tap = (Tap) ( (CompositeTap) tap ).getChildTaps().next();

    tap = new FileTap( new TextLine( new Fields( "line" ) ), tap.getIdentifier() );

    setSourceFields( delimitedParser.parseFirstLine( process, tap ) );

    return getSourceFields();
    }

  @Override
  public void sourceConfInit( FlowProcess<Properties> flowProcess, Tap<FlowProcess<Properties>, Properties, LineNumberReader, PrintWriter> tap, Properties conf )
    {
    }

  @Override
  public boolean source( FlowProcess<Properties> flowProcess, SourceCall<Void, LineNumberReader> sourceCall ) throws IOException
    {
    String line = sourceCall.getInput().readLine();

    if( line == null )
      return false;

    if( skipHeader && sourceCall.getInput().getLineNumber() == 1 ) // todo: optimize this away
      line = sourceCall.getInput().readLine();

    Object[] split = delimitedParser.parseLine( line );

    // assumption it is better to re-use than to construct new
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();

    tuple.clear();
    tuple.addAll( split );

    return true;
    }

  @Override
  public void sinkConfInit( FlowProcess<Properties> flowProcess, Tap<FlowProcess<Properties>, Properties, LineNumberReader, PrintWriter> tap, Properties conf )
    {
    }

  @Override
  public void sinkPrepare( FlowProcess<Properties> flowProcess, SinkCall<StringBuilder, PrintWriter> sinkCall )
    {
    sinkCall.setContext( new StringBuilder( 4 * 1024 ) );

    if( writeHeader )
      {
      Fields fields = sinkCall.getOutgoingEntry().getFields();
      String line = delimitedParser.joinLine( fields, sinkCall.getContext() );

      sinkCall.getOutput().println( line );
      }
    }

  @Override
  public void sink( FlowProcess<Properties> flowProcess, SinkCall<StringBuilder, PrintWriter> sinkCall ) throws IOException
    {
    Tuple tuple = sinkCall.getOutgoingEntry().getTuple();

    String line = delimitedParser.joinLine( tuple, sinkCall.getContext() );

    sinkCall.getOutput().println( line );
    }

  @Override
  public void sinkCleanup( FlowProcess<Properties> flowProcess, SinkCall<StringBuilder, PrintWriter> sinkCall )
    {
    sinkCall.setContext( null );
    }
  }
