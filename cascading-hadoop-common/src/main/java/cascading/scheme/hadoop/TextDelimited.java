/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import java.nio.charset.Charset;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleViews;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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
 * <p/>
 * By default, all text is encoded/decoded as UTF-8. This can be changed via the {@code charsetName} constructor
 * argument.
 * <p/>
 * To override field and line parsing behaviors, sub-class {@link DelimitedParser} or provide a
 * {@link cascading.scheme.util.FieldTypeResolver} implementation.
 * <p/>
 * Note that there should be no expectation that TextDelimited, or specifically {@link DelimitedParser}, can handle
 * all delimited and quoted combinations reliably. Attempting to do so would impair its performance and maintainability.
 * <p/>
 * Further, it can be safely said any corrupted files will not be supported for obvious reasons. Corrupted files may
 * result in exceptions or could cause edge cases in the underlying java regular expression engine.
 * <p/>
 * A large part of Cascading was designed to help users cleans data. Thus the recommendation is to create Flows that
 * are responsible for cleansing large data-sets when faced with the problem
 * <p/>
 * DelimitedParser maybe sub-classed and extended if necessary.
 *
 * @see TextLine
 */
public class TextDelimited extends TextLine
  {
  public static final String DEFAULT_CHARSET = "UTF-8";

  /** Field delimitedParser */
  protected final DelimitedParser delimitedParser;
  /** Field skipHeader */
  private boolean skipHeader;
  private final boolean writeHeader;

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   */
  public TextDelimited()
    {
    this( Fields.ALL, null, "\t", null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param hasHeader of type boolean
   * @param delimiter of type String
   */
  @ConstructorProperties({"hasHeader", "delimiter"})
  public TextDelimited( boolean hasHeader, String delimiter )
    {
    this( Fields.ALL, null, hasHeader, delimiter, null, (Class[]) null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param quote     of type String
   */
  @ConstructorProperties({"hasHeader", "delimiter", "quote"})
  public TextDelimited( boolean hasHeader, String delimiter, String quote )
    {
    this( Fields.ALL, null, hasHeader, delimiter, quote, (Class[]) null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param hasHeader       of type boolean
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"hasHeader", "delimitedParser"})
  public TextDelimited( boolean hasHeader, DelimitedParser delimitedParser )
    {
    this( Fields.ALL, null, hasHeader, hasHeader, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   * <p/>
   * This constructor will set {@code skipHeader} and {@code writeHeader} values to true.
   *
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"delimitedParser"})
  public TextDelimited( DelimitedParser delimitedParser )
    {
    this( Fields.ALL, null, true, true, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param sinkCompression of type Compress
   * @param hasHeader       of type boolean
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"sinkCompression", "hasHeader", "delimitedParser"})
  public TextDelimited( Compress sinkCompression, boolean hasHeader, DelimitedParser delimitedParser )
    {
    this( Fields.ALL, sinkCompression, hasHeader, hasHeader, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   * <p/>
   * This constructor will set {@code skipHeader} and {@code writeHeader} values to true.
   *
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"sinkCompression", "delimitedParser"})
  public TextDelimited( Compress sinkCompression, DelimitedParser delimitedParser )
    {
    this( Fields.ALL, sinkCompression, true, true, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p/>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param sinkCompression of type Compress
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   */
  @ConstructorProperties({"sinkCompression", "hasHeader", "delimiter", "quote"})
  public TextDelimited( Compress sinkCompression, boolean hasHeader, String delimiter, String quote )
    {
    this( Fields.ALL, sinkCompression, hasHeader, delimiter, quote, (Class[]) null );
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
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter )
    {
    this( fields, null, hasHeader, hasHeader, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter )
    {
    this( fields, null, skipHeader, writeHeader, delimiter, null, null );
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
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param types     of type Class[]
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, Class[] types )
    {
    this( fields, null, hasHeader, hasHeader, delimiter, null, types );
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
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, Class[] types )
    {
    this( fields, null, skipHeader, writeHeader, delimiter, null, types );
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
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param quote     of type String
   * @param types     of type Class[]
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, null, hasHeader, hasHeader, delimiter, quote, types );
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
    this( fields, null, skipHeader, writeHeader, delimiter, quote, types );
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
    this( fields, null, hasHeader, hasHeader, delimiter, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param hasHeader   of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param types       of type Class[]
   * @param safe        of type boolean
   * @param charsetName of type String
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "quote", "types", "safe", "charsetName"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, String quote, Class[] types, boolean safe, String charsetName )
    {
    this( fields, null, hasHeader, hasHeader, delimiter, true, quote, types, safe, charsetName );
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
    this( fields, null, skipHeader, writeHeader, delimiter, quote, types, safe );
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
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param writeHeader     of type boolean
   * @param delimiter       of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "writeHeader", "delimiter"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, delimiter, null, null );
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
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, Class[] types )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param writeHeader     of type boolean
   * @param delimiter       of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "writeHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter, Class[] types )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, delimiter, null, types );
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
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, null, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   * @param charsetName     of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter", "types", "safe", "charsetName"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, Class[] types, boolean safe, String charsetName )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, true, null, types, safe, charsetName );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param writeHeader     of type boolean
   * @param delimiter       of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "writeHeader", "delimiter", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, delimiter, null, types, safe );
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
   * @param fields    of type Fields
   * @param hasHeader of type boolean
   * @param delimiter of type String
   * @param quote     of type String
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, String quote )
    {
    this( fields, null, hasHeader, hasHeader, delimiter, quote );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, String quote )
    {
    this( fields, null, skipHeader, writeHeader, delimiter, quote );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param delimiter       of type String
   * @param quote           of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "delimiter", "quote"})
  public TextDelimited( Fields fields, Compress sinkCompression, String delimiter, String quote )
    {
    this( fields, sinkCompression, false, false, delimiter, true, quote, null, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, String quote )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, true, quote, null, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   * @param charsetName     of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter", "quote", "charsetName"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, String quote, String charsetName )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, true, quote, null, true, charsetName );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param writeHeader     of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "writeHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter, String quote )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, delimiter, true, quote, null, true );
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
    this( fields, sinkCompression, false, false, delimiter, true, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, true, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param writeHeader     of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "writeHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, delimiter, true, quote, types, true );
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
    this( fields, sinkCompression, false, false, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param hasHeader       of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "hasHeader", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean hasHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, hasHeader, hasHeader, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param writeHeader     of type boolean
   * @param delimiter       of type String
   * @param quote           of type String
   * @param types           of type Class[]
   * @param safe            of type boolean
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "writeHeader", "delimiter", "quote", "types",
                          "safe"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, delimiter, true, quote, types, safe );
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
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, delimiter, strict, quote, types, safe, DEFAULT_CHARSET );
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
   * @param charsetName     of type String
   */
  @ConstructorProperties({"fields", "sinkCompression", "skipHeader", "delimiter", "strict", "quote", "types", "safe",
                          "charsetName"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe, String charsetName )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, charsetName, new DelimitedParser( delimiter, quote, types, strict, safe ) );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param writeHeader     of type boolean
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimitedParser"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, DelimitedParser delimitedParser )
    {
    this( fields, null, skipHeader, writeHeader, null, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param hasHeader       of type boolean
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"fields", "hasHeader", "delimitedParser"})
  public TextDelimited( Fields fields, boolean hasHeader, DelimitedParser delimitedParser )
    {
    this( fields, null, hasHeader, hasHeader, null, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param writeHeader     of type boolean
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimitedParser"})
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, DelimitedParser delimitedParser )
    {
    this( fields, sinkCompression, skipHeader, writeHeader, null, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param sinkCompression of type Compress
   * @param skipHeader      of type boolean
   * @param writeHeader     of type boolean
   * @param charsetName     of type String
   * @param delimitedParser of type DelimitedParser
   */
  public TextDelimited( Fields fields, Compress sinkCompression, boolean skipHeader, boolean writeHeader, String charsetName, DelimitedParser delimitedParser )
    {
    super( sinkCompression );

    this.delimitedParser = delimitedParser;

    // normalizes ALL and UNKNOWN
    setSinkFields( fields );
    setSourceFields( fields );

    this.skipHeader = skipHeader;
    this.writeHeader = writeHeader;

    // throws an exception if not found
    setCharsetName( charsetName );
    }

  /**
   * Method getDelimiter returns the delimiter used to parse fields from the current line of text.
   *
   * @return a String
   */
  public String getDelimiter()
    {
    return delimitedParser.getDelimiter();
    }

  /**
   * Method getQuote returns the quote string, if any, used to encapsulate each field in a line to delimited text.
   *
   * @return a String
   */
  public String getQuote()
    {
    return delimitedParser.getQuote();
    }

  @Override
  public boolean isSymmetrical()
    {
    return super.isSymmetrical() && skipHeader == writeHeader;
    }

  @Override
  public void setSinkFields( Fields sinkFields )
    {
    super.setSourceFields( sinkFields );
    super.setSinkFields( sinkFields );

    if( delimitedParser != null )
      delimitedParser.reset( getSourceFields(), getSinkFields() );
    }

  @Override
  public void setSourceFields( Fields sourceFields )
    {
    super.setSourceFields( sourceFields );
    super.setSinkFields( sourceFields );

    if( delimitedParser != null )
      delimitedParser.reset( getSourceFields(), getSinkFields() );
    }

  @Override
  public Fields retrieveSourceFields( FlowProcess<JobConf> flowProcess, Tap tap )
    {
    if( !skipHeader || !getSourceFields().isUnknown() )
      return getSourceFields();

    // no need to open them all
    if( tap instanceof CompositeTap )
      tap = (Tap) ( (CompositeTap) tap ).getChildTaps().next();

    // should revert to file:// (Lfs) if tap is Lfs
    tap = new Hfs( new TextLine( new Fields( "line" ), charsetName ), tap.getFullIdentifier( flowProcess.getConfigCopy() ) );

    setSourceFields( delimitedParser.parseFirstLine( flowProcess, tap ) );

    return getSourceFields();
    }

  @Override
  public void presentSourceFields( FlowProcess<JobConf> flowProcess, Tap tap, Fields fields )
    {
    presentSourceFieldsInternal( fields );
    }

  @Override
  public void presentSinkFields( FlowProcess<JobConf> flowProcess, Tap tap, Fields fields )
    {
    presentSinkFieldsInternal( fields );
    }

  @Override
  public void sourcePrepare( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    super.sourcePrepare( flowProcess, sourceCall );

    sourceCall.getIncomingEntry().setTuple( TupleViews.createObjectArray() );
    }

  @Override
  public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Object[] context = sourceCall.getContext();

    if( !sourceCall.getInput().next( context[ 0 ], context[ 1 ] ) )
      return false;

    if( skipHeader && ( (LongWritable) context[ 0 ] ).get() == 0 )
      {
      if( !sourceCall.getInput().next( context[ 0 ], context[ 1 ] ) )
        return false;
      }

    // delegate coercion to delimitedParser for robustness
    Object[] split = delimitedParser.parseLine( makeEncodedString( context ) );
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();

    TupleViews.reset( tuple, split );

    return true;
    }

  @Override
  public void sinkPrepare( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    sinkCall.setContext( new Object[ 3 ] );

    sinkCall.getContext()[ 0 ] = new Text();
    sinkCall.getContext()[ 1 ] = new StringBuilder( 4 * 1024 );
    sinkCall.getContext()[ 2 ] = Charset.forName( charsetName );

    if( writeHeader )
      writeHeader( sinkCall );
    }

  protected void writeHeader( SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    Fields fields = sinkCall.getOutgoingEntry().getFields();

    Text text = (Text) sinkCall.getContext()[ 0 ];
    StringBuilder line = (StringBuilder) sinkCall.getContext()[ 1 ];
    Charset charset = (Charset) sinkCall.getContext()[ 2 ];

    line = (StringBuilder) delimitedParser.joinFirstLine( fields, line );

    text.set( line.toString().getBytes( charset ) );

    sinkCall.getOutput().collect( null, text );

    line.setLength( 0 );
    }

  @Override
  public void sink( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

    Text text = (Text) sinkCall.getContext()[ 0 ];
    StringBuilder line = (StringBuilder) sinkCall.getContext()[ 1 ];
    Charset charset = (Charset) sinkCall.getContext()[ 2 ];

    Iterable<String> strings = tupleEntry.asIterableOf( String.class );

    line = (StringBuilder) delimitedParser.joinLine( strings, line );

    text.set( line.toString().getBytes( charset ) );

    sinkCall.getOutput().collect( null, text );

    line.setLength( 0 );
    }
  }

