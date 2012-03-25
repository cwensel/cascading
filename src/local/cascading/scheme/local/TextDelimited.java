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

import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.DelimitedParser;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class TextDelimited provides direct support for delimited text files, like
 * TAB (\t) or COMMA (,) delimited files. It also optionally allows for quoted values.
 * <p/>
 * TextDelimited may also be used to skip the "header" in a file, where the header is defined as the very first line
 * in every input file. That is, if the byte offset of the current line from the input is zero (0), that line will
 * be skipped.
 * <p/>
 * By default headers are not skipped.
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
 */
public class TextDelimited extends LocalScheme<LineNumberReader, PrintWriter, Void, StringBuilder>
  {
  private final boolean skipHeader;
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
   * @param fields     of type Fields
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   */
  @ConstructorProperties({"fields", "skipHeader", "delimiter"})
  public TextDelimited( Fields fields, boolean skipHeader, String delimiter )
    {
    this( fields, skipHeader, delimiter, null, null );
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
   * @param fields     of type Fields
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   * @param types      of type Class[]
   */
  @ConstructorProperties({"fields", "skipHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, boolean skipHeader, String delimiter, Class[] types )
    {
    this( fields, skipHeader, delimiter, null, types );
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
   * @param fields     of type Fields
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   * @param quote      of type String
   * @param types      of type Class[]
   */
  @ConstructorProperties({"fields", "skipHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, boolean skipHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, skipHeader, delimiter, quote, types, true );
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
   * @param fields     of type Fields
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   * @param quote      of type String
   * @param types      of type Class[]
   * @param safe       of type boolean
   */
  @ConstructorProperties({"fields", "skipHeader", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, boolean skipHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, skipHeader, delimiter, true, quote, types, safe );
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
   * @param fields     of type Fields
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   * @param quote      of type String
   */
  @ConstructorProperties({"fields", "skipHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, boolean skipHeader, String delimiter, String quote )
    {
    this( fields, skipHeader, delimiter, quote, null, true );
    }


  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   * @param strict     of type boolean
   * @param quote      of type String
   * @param types      of type Class[]
   * @param safe       of type boolean
   */
  @ConstructorProperties({"fields", "skipHeader", "delimiter", "strict", "quote", "types", "safe"})
  public TextDelimited( Fields fields, boolean skipHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe )
    {
    super( fields, fields );

    // normalizes ALL and UNKNOWN
    setSinkFields( fields );
    setSourceFields( fields );

    this.skipHeader = skipHeader;

    delimitedParser = new DelimitedParser( delimiter, quote, types, strict, safe, getSourceFields(), getSinkFields() );
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
  public void sourceConfInit( LocalFlowProcess flowProcess, Tap<LocalFlowProcess, Properties, LineNumberReader, PrintWriter> tap, Properties conf )
    {
    }

  @Override
  public boolean source( LocalFlowProcess flowProcess, SourceCall<Void, LineNumberReader> sourceCall ) throws IOException
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
  public void sinkConfInit( LocalFlowProcess flowProcess, Tap<LocalFlowProcess, Properties, LineNumberReader, PrintWriter> tap, Properties conf )
    {
    }

  @Override
  public void sinkPrepare( LocalFlowProcess flowProcess, SinkCall<StringBuilder, PrintWriter> sinkCall )
    {
    sinkCall.setContext( new StringBuilder( 4 * 1024 ) );
    }

  @Override
  public void sink( LocalFlowProcess flowProcess, SinkCall<StringBuilder, PrintWriter> sinkCall ) throws IOException
    {
    Tuple tuple = sinkCall.getOutgoingEntry().getTuple();

    String line = delimitedParser.joinLine( tuple, sinkCall.getContext() );

    sinkCall.getOutput().println( line );
    }

  @Override
  public void sinkCleanup( LocalFlowProcess flowProcess, SinkCall<StringBuilder, PrintWriter> sinkCall )
    {
    sinkCall.setContext( null );
    }
  }
