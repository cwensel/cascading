/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
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
 *
 */
public class TextDelimited extends LocalScheme<LineNumberReader, PrintWriter, Void, StringBuilder>
  {
  private final boolean skipHeader;
  private final DelimitedParser delimitedParser;
  private Object[] buffer;

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
  public void sourceConfInit( LocalFlowProcess flowProcess, Tap tap, Properties conf )
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
  public void sinkConfInit( LocalFlowProcess flowProcess, Tap tap, Properties conf )
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
