/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.scheme.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.DelimitedParser;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Class TextDelimited is a sub-class of {@link TextLine}. It provides direct support for delimited text files, like
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
 *
 * @see TextLine
 */
public class TextDelimited extends TextLine
  {
  /** Field delimitedParser */
  protected final DelimitedParser delimitedParser;
  /** Field skipHeader */
  private final boolean skipHeader;

  /** Class DecoratorTuple just wraps a Tuple. */
  private static class DecoratorTuple extends Tuple
    {
    String string;

    private DecoratorTuple()
      {
      super( (List<Object>) null );
      }

    public void set( Tuple tuple, String string )
      {
      this.elements = (ArrayList<Object>) Tuple.elements( tuple );
      this.string = string;
      }

    @Override
    public String toString()
      {
      return string;
      }
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance with TAB as the default delimiter.
   *
   * @param fields of type Fields
   */
  @ConstructorProperties({"fields"})
  public TextDelimited( Fields fields )
    {
    this( fields, null, "\t", null, null );
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
    this( fields, null, delimiter, null, null );
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
    this( fields, null, skipHeader, delimiter, null, null );
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
    this( fields, null, delimiter, null, types );
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
    this( fields, null, skipHeader, delimiter, null, types );
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
    this( fields, null, delimiter, quote, types );
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
    this( fields, null, skipHeader, delimiter, quote, types );
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
    this( fields, null, delimiter, quote, types, safe );
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
    this( fields, null, skipHeader, delimiter, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param delimiter       of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "delimiter"})
  public TextDelimited( Fields fields, Compress sinkCompression, String delimiter )
    {
    this( fields, sinkCompression, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param delimiter       of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter )
    {
    this( fields, sinkCompression, skipHeader, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param delimiter       of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "delimiter", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, String delimiter, Class[] types )
    {
    this( fields, sinkCompression, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param delimiter       of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter, Class[] types )
    {
    this( fields, sinkCompression, skipHeader, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param delimiter       of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "delimiter", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, String delimiter, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, delimiter, null, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param delimiter       of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, skipHeader, delimiter, null, types, safe );
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
    this( fields, null, delimiter, quote );
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
    this( fields, null, skipHeader, delimiter, quote );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param delimiter       of type String
   * @param quote           of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, Compress sinkCompression, String delimiter, String quote )
    {
    this( fields, sinkCompression, false, delimiter, true, quote, null, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   */
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter, String quote )
    {
    this( fields, sinkCompression, skipHeader, delimiter, true, quote, null, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, String delimiter, String quote, Class[] types )
    {
    this( fields, sinkCompression, false, delimiter, true, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, sinkCompression, skipHeader, delimiter, true, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, false, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, skipHeader, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param delimiter       of type String
   * @param strict          of type boolean
   * @param quote           of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter", "strict", "quote", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe )
    {
    super( sinkCompression );

    // normalizes ALL and UNKNOWN
    setSinkFields( fields );
    setSourceFields( fields );

    this.skipHeader = skipHeader;

    delimitedParser = new DelimitedParser( delimiter, quote, types, strict, safe, getSourceFields(), getSinkFields() );
    }

  @Override
  public boolean source( HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Object[] context = sourceCall.getContext();

    if( !sourceCall.getInput().next( context[ 0 ], context[ 1 ] ) )
      return false;

    if( skipHeader && ( (LongWritable) context[ 0 ] ).get() == 0 )
      {
      if( !sourceCall.getInput().next( context[ 0 ], context[ 1 ] ) )
        return false;
      }

    Object[] split = delimitedParser.parseLine( context[ 1 ].toString() );
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();

    tuple.clear();

    tuple.addAll( split );

    return true;
    }

  @Override
  public void sinkPrepare( HadoopFlowProcess flowProcess, SinkCall<Object[], OutputCollector> sinkCall )
    {
    sinkCall.setContext( new Object[]{new DecoratorTuple(), new StringBuilder( 4 * 1024 )} );
    }

  @Override
  public void sink( HadoopFlowProcess flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    Tuple tuple = sinkCall.getOutgoingEntry().getTuple();
    String line = delimitedParser.joinLine( tuple, (StringBuilder) sinkCall.getContext()[ 1 ] );

    DecoratorTuple decoratorTuple = (DecoratorTuple) sinkCall.getContext()[ 0 ];

    decoratorTuple.set( tuple, line );

    sinkCall.getOutput().collect( null, decoratorTuple );
    }
  }

