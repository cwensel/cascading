/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.Visibility;
import cascading.scheme.FileFormat;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.CompositeTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleViews;

/**
 * Class TextDelimited provides direct support for delimited text files, like
 * TAB (\t) or COMMA (,) delimited files. It also optionally allows for quoted values.
 * <p>
 * TextDelimited may also be used to skip the "header" in a file, where the header is defined as the very first line
 * in every input file. That is, if the byte offset of the current line from the input is zero (0), that line will
 * be skipped.
 * <p>
 * It is assumed if sink/source {@code fields} is set to either {@link Fields#ALL} or {@link Fields#UNKNOWN} and
 * {@code skipHeader} or {@code hasHeader} is {@code true}, the field names will be retrieved from the header of the
 * file and used during planning. The header will parsed with the same rules as the body of the file.
 * <p>
 * By default headers are not skipped.
 * <p>
 * TextDelimited may also be used to write a "header" in a file. The fields names for the header are taken directly
 * from the declared fields. Or if the declared fields are {@link Fields#ALL} or {@link Fields#UNKNOWN}, the
 * resolved field names will be used, if any.
 * <p>
 * By default headers are not written.
 * <p>
 * If {@code hasHeaders} is set to {@code true} on a constructor, both {@code skipHeader} and {@code writeHeader} will
 * be set to {@code true}.
 * <p>
 * By default this {@link cascading.scheme.Scheme} is both {@code strict} and {@code safe}.
 * <p>
 * Strict meaning if a line of text does not parse into the expected number of fields, this class will throw a
 * {@link TapException}. If strict is {@code false}, then {@link Tuple} will be returned with {@code null} values
 * for the missing fields.
 * <p>
 * Safe meaning if a field cannot be coerced into an expected type, a {@code null} will be used for the value.
 * If safe is {@code false}, a {@link TapException} will be thrown.
 * <p>
 * Also by default, {@code quote} strings are not searched for to improve processing speed. If a file is
 * COMMA delimited but may have COMMA's in a value, the whole value should be surrounded by the quote string, typically
 * double quotes ({@literal "}).
 * <p>
 * Note all empty fields in a line will be returned as {@code null} unless coerced into a new type.
 * <p>
 * This Scheme may source/sink {@link Fields#ALL}, when given on the constructor the new instance will automatically
 * default to strict == false as the number of fields parsed are arbitrary or unknown. A type array may not be given
 * either, so all values will be returned as Strings.
 * <p>
 * By default, all text is encoded/decoded as UTF-8. This can be changed via the {@code charsetName} constructor
 * argument.
 * <p>
 * To override field and line parsing behaviors, sub-class {@link DelimitedParser} or provide a
 * {@link cascading.scheme.util.FieldTypeResolver} implementation.
 * <p>
 * Note that there should be no expectation that TextDelimited, or specifically {@link DelimitedParser}, can handle
 * all delimited and quoted combinations reliably. Attempting to do so would impair its performance and maintainability.
 * <p>
 * Further, it can be safely said any corrupted files will not be supported for obvious reasons. Corrupted files may
 * result in exceptions or could cause edge cases in the underlying java regular expression engine.
 * <p>
 * A large part of Cascading was designed to help users cleans data. Thus the recommendation is to create Flows that
 * are responsible for cleansing large data-sets when faced with the problem.
 * <p>
 * DelimitedParser maybe sub-classed and extended if necessary.
 * <p>
 * In order to read or write a compressed files, pass a {@link cascading.scheme.local.CompressorScheme.Compressor}
 * instance to the appropriate constructors. See {@link Compressors} for provided compression algorithms.
 *
 * @see TextLine
 * @see Compressors
 */
public class TextDelimited extends CompressorScheme<LineNumberReader, PrintWriter> implements FileFormat
  {
  public static final String DEFAULT_CHARSET = "UTF-8";

  private final boolean skipHeader;
  private final boolean writeHeader;
  private final DelimitedParser delimitedParser;
  private String charsetName = DEFAULT_CHARSET;

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   */
  public TextDelimited()
    {
    this( Fields.ALL );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param hasHeader
   * @param delimiter
   */
  @ConstructorProperties({"hasHeader", "delimiter"})
  public TextDelimited( boolean hasHeader, String delimiter )
    {
    this( Fields.ALL, hasHeader, delimiter, null, (Class[]) null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param hasHeader
   * @param delimiter
   * @param quote
   */
  @ConstructorProperties({"hasHeader", "delimiter", "quote"})
  public TextDelimited( boolean hasHeader, String delimiter, String quote )
    {
    this( Fields.ALL, hasHeader, delimiter, quote, (Class[]) null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param hasHeader
   * @param delimitedParser
   */
  @ConstructorProperties({"hasHeader", "delimitedParser"})
  public TextDelimited( boolean hasHeader, DelimitedParser delimitedParser )
    {
    this( Fields.ALL, hasHeader, hasHeader, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   * <p>
   * This constructor will set {@code skipHeader} and {@code writeHeader} values to true.
   *
   * @param delimitedParser
   */
  @ConstructorProperties({"delimitedParser"})
  public TextDelimited( DelimitedParser delimitedParser )
    {
    this( Fields.ALL, true, true, delimitedParser );
    }

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
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter", "types"})
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
    this( fields, hasHeader, hasHeader, delimiter, true, quote, types, safe, charsetName );
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
   * @param hasHeader   of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param charsetName of type String
   */
  @ConstructorProperties({"fields", "hasHeader", "delimiter", "quote", "charsetName"})
  public TextDelimited( Fields fields, boolean hasHeader, String delimiter, String quote, String charsetName )
    {
    this( fields, hasHeader, delimiter, quote, null, true, charsetName );
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
    this( fields, skipHeader, writeHeader, delimiter, strict, quote, types, safe, DEFAULT_CHARSET );
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
   * @param charsetName of type String
   */
  @ConstructorProperties({"fields", "skipHeader", "writeHeader", "delimiter", "strict", "quote", "types", "safe",
                          "charsetName"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe, String charsetName )
    {
    this( fields, skipHeader, writeHeader, charsetName, new DelimitedParser( delimiter, quote, types, strict, safe ) );
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
    this( fields, skipHeader, writeHeader, null, delimitedParser );
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
    this( fields, hasHeader, hasHeader, null, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param writeHeader     of type boolean
   * @param charsetName     of type String
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "charsetName", "delimitedParser"})
  public TextDelimited( Fields fields, boolean skipHeader, boolean writeHeader, String charsetName, DelimitedParser delimitedParser )
    {
    this( fields, null, skipHeader, writeHeader, charsetName, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param compressor of type Compressor, see {@link Compressors}
   */
  @ConstructorProperties("compressor")
  public TextDelimited( Compressor compressor )
    {
    this( Fields.ALL, compressor );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param compressor of type Compressor, see {@link Compressors}
   * @param hasHeader
   * @param delimiter
   */
  @ConstructorProperties({"compressor", "hasHeader", "delimiter"})
  public TextDelimited( Compressor compressor, boolean hasHeader, String delimiter )
    {
    this( Fields.ALL, compressor, hasHeader, delimiter, null, (Class[]) null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using TAB as the default delimiter.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param compressor of type Compressor, see {@link Compressors}
   * @param hasHeader
   * @param delimiter
   * @param quote
   */
  @ConstructorProperties({"compressor", "hasHeader", "delimiter", "quote"})
  public TextDelimited( Compressor compressor, boolean hasHeader, String delimiter, String quote )
    {
    this( Fields.ALL, compressor, hasHeader, delimiter, quote, (Class[]) null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   *
   * @param compressor      of type Compressor, see {@link Compressors}
   * @param hasHeader
   * @param delimitedParser
   */
  @ConstructorProperties({"compressor", "hasHeader", "delimitedParser"})
  public TextDelimited( Compressor compressor, boolean hasHeader, DelimitedParser delimitedParser )
    {
    this( Fields.ALL, compressor, hasHeader, hasHeader, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance sourcing {@link Fields#UNKNOWN}, sinking
   * {@link Fields#ALL} and using the given delimitedParser instance for parsing.
   * <p>
   * Use this constructor if the source and sink fields will be resolved during planning, for example, when using
   * with a {@link cascading.pipe.Checkpoint} Tap.
   * <p>
   * This constructor will set {@code skipHeader} and {@code writeHeader} values to true.
   *
   * @param compressor      of type Compressor, see {@link Compressors}
   * @param delimitedParser
   */
  @ConstructorProperties({"compressor", "delimitedParser"})
  public TextDelimited( Compressor compressor, DelimitedParser delimitedParser )
    {
    this( Fields.ALL, compressor, true, true, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance with TAB as the default delimiter.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   */
  @ConstructorProperties({"fields", "compressor"})
  public TextDelimited( Fields fields, Compressor compressor )
    {
    this( fields, compressor, "\t", null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param delimiter  of type String
   */
  @ConstructorProperties({"fields", "compressor", "delimiter"})
  public TextDelimited( Fields fields, Compressor compressor, String delimiter )
    {
    this( fields, compressor, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param hasHeader  of type boolean
   * @param delimiter  of type String
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimiter"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, String delimiter )
    {
    this( fields, compressor, hasHeader, hasHeader, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param skipHeader of type boolean
   * @param delimiter  of type String
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "delimiter"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, String delimiter )
    {
    this( fields, compressor, skipHeader, writeHeader, delimiter, null, null );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param delimiter  of type String
   * @param types      of type Class[]
   */
  @ConstructorProperties({"fields", "compressor", "delimiter", "types"})
  public TextDelimited( Fields fields, Compressor compressor, String delimiter, Class[] types )
    {
    this( fields, compressor, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param hasHeader  of type boolean
   * @param delimiter  of type String
   * @param types      of type Class[]
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, String delimiter, Class[] types )
    {
    this( fields, compressor, hasHeader, hasHeader, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param types       of type Class[]
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "delimiter", "types"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, String delimiter, Class[] types )
    {
    this( fields, compressor, skipHeader, writeHeader, delimiter, null, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param delimiter  of type String
   * @param quote      of type String
   * @param types      of type Class[]
   */
  @ConstructorProperties({"fields", "compressor", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, Compressor compressor, String delimiter, String quote, Class[] types )
    {
    this( fields, compressor, false, delimiter, quote, types );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param hasHeader  of type boolean
   * @param delimiter  of type String
   * @param quote      of type String
   * @param types      of type Class[]
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, compressor, hasHeader, hasHeader, delimiter, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param types       of type Class[]
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "delimiter", "quote", "types"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types )
    {
    this( fields, compressor, skipHeader, writeHeader, delimiter, quote, types, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param delimiter  of type String
   * @param quote      of type String
   * @param types      of type Class[]
   * @param safe       of type boolean
   */
  @ConstructorProperties({"fields", "compressor", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, Compressor compressor, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, compressor, false, delimiter, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param hasHeader  of type boolean
   * @param delimiter  of type String
   * @param quote      of type String
   * @param types      of type Class[]
   * @param safe       of type boolean
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, compressor, hasHeader, hasHeader, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param hasHeader   of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param types       of type Class[]
   * @param safe        of type boolean
   * @param charsetName of type String
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimiter", "quote", "types", "safe", "charsetName"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, String delimiter, String quote, Class[] types, boolean safe, String charsetName )
    {
    this( fields, compressor, hasHeader, hasHeader, delimiter, true, quote, types, safe, charsetName );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param types       of type Class[]
   * @param safe        of type boolean
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "delimiter", "quote", "types", "safe"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types, boolean safe )
    {
    this( fields, compressor, skipHeader, writeHeader, delimiter, true, quote, types, safe );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param delimiter  of type String
   * @param quote      of type String
   */
  @ConstructorProperties({"fields", "compressor", "delimiter", "quote"})
  public TextDelimited( Fields fields, Compressor compressor, String delimiter, String quote )
    {
    this( fields, compressor, false, delimiter, quote, null, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields     of type Fields
   * @param compressor of type Compressor, see {@link Compressors}
   * @param hasHeader  of type boolean
   * @param delimiter  of type String
   * @param quote      of type String
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimiter", "quote"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, String delimiter, String quote )
    {
    this( fields, compressor, hasHeader, delimiter, quote, null, true );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param hasHeader   of type boolean
   * @param delimiter   of type String
   * @param quote       of type String
   * @param charsetName of type String
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimiter", "quote", "charsetName"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, String delimiter, String quote, String charsetName )
    {
    this( fields, compressor, hasHeader, delimiter, quote, null, true, charsetName );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param strict      of type boolean
   * @param quote       of type String
   * @param types       of type Class[]
   * @param safe        of type boolean
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "delimiter", "strict", "quote", "types",
                          "safe"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe )
    {
    this( fields, compressor, skipHeader, writeHeader, delimiter, strict, quote, types, safe, DEFAULT_CHARSET );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields      of type Fields
   * @param compressor  of type Compressor, see {@link Compressors}
   * @param skipHeader  of type boolean
   * @param writeHeader of type boolean
   * @param delimiter   of type String
   * @param strict      of type boolean
   * @param quote       of type String
   * @param types       of type Class[]
   * @param safe        of type boolean
   * @param charsetName of type String
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "delimiter", "strict", "quote", "types",
                          "safe", "charsetName"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, String delimiter, boolean strict, String quote, Class[] types, boolean safe, String charsetName )
    {
    this( fields, compressor, skipHeader, writeHeader, charsetName, new DelimitedParser( delimiter, quote, types, strict, safe ) );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param compressor      of type Compressor, see {@link Compressors}
   * @param writeHeader     of type boolean
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "delimitedParser"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, DelimitedParser delimitedParser )
    {
    this( fields, compressor, skipHeader, writeHeader, null, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param compressor      of type Compressor, see {@link Compressors}
   * @param hasHeader       of type boolean
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"fields", "compressor", "hasHeader", "delimitedParser"})
  public TextDelimited( Fields fields, Compressor compressor, boolean hasHeader, DelimitedParser delimitedParser )
    {
    this( fields, compressor, hasHeader, hasHeader, null, delimitedParser );
    }

  /**
   * Constructor TextDelimited creates a new TextDelimited instance.
   *
   * @param fields          of type Fields
   * @param compressor      of type Compressor, see {@link Compressors}
   * @param compressor      of type Compressor, see {@link Compressors}
   * @param writeHeader     of type boolean
   * @param charsetName     of type String
   * @param delimitedParser of type DelimitedParser
   */
  @ConstructorProperties({"fields", "compressor", "skipHeader", "writeHeader", "charsetName", "delimitedParser"})
  public TextDelimited( Fields fields, Compressor compressor, boolean skipHeader, boolean writeHeader, String charsetName, DelimitedParser delimitedParser )
    {
    super( fields, fields, compressor );

    this.delimitedParser = delimitedParser;

    // normalizes ALL and UNKNOWN
    // calls reset on delimitedParser
    setSourceFields( fields );
    setSinkFields( fields );

    this.skipHeader = skipHeader;
    this.writeHeader = writeHeader;

    if( charsetName != null )
      this.charsetName = charsetName;

    // throws an exception if not found
    Charset.forName( this.charsetName );
    }

  @Property(name = "charset", visibility = Visibility.PUBLIC)
  @PropertyDescription("character set used.")
  public String getCharsetName()
    {
    return charsetName;
    }

  /**
   * Method getDelimiter returns the delimiter used to parse fields from the current line of text.
   *
   * @return a String
   */
  @Property(name = "delimiter", visibility = Visibility.PUBLIC)
  @PropertyDescription("The delimiter used to separate fields.")
  public String getDelimiter()
    {
    return delimitedParser.getDelimiter();
    }

  /**
   * Method getQuote returns the quote string, if any, used to encapsulate each field in a line to delimited text.
   *
   * @return a String
   */
  @Property(name = "quote", visibility = Visibility.PUBLIC)
  @PropertyDescription("The string used for quoting.")
  public String getQuote()
    {
    return delimitedParser.getQuote();
    }

  public LineNumberReader createInput( InputStream inputStream )
    {
    try
      {
      return new LineNumberReader( new InputStreamReader( inputStream, charsetName ) );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new TapException( exception );
      }
    }

  public PrintWriter createOutput( OutputStream outputStream )
    {
    try
      {
      return new PrintWriter( new OutputStreamWriter( outputStream, charsetName ) );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new TapException( exception );
      }
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
  public boolean isSymmetrical()
    {
    return super.isSymmetrical() && skipHeader == writeHeader;
    }

  @Override
  public Fields retrieveSourceFields( FlowProcess<? extends Properties> process, Tap tap )
    {
    if( !skipHeader || !getSourceFields().isUnknown() )
      return getSourceFields();

    // no need to open them all
    if( tap instanceof CompositeTap )
      tap = (Tap) ( (CompositeTap) tap ).getChildTaps().next();

    tap = new FileTap( new TextLine( new Fields( "line" ), charsetName ), tap.getIdentifier() );

    setSourceFields( delimitedParser.parseFirstLine( process, tap ) );

    return getSourceFields();
    }

  @Override
  public void presentSourceFields( FlowProcess<? extends Properties> process, Tap tap, Fields fields )
    {
    // do nothing
    }

  @Override
  public void presentSinkFields( FlowProcess<? extends Properties> flowProcess, Tap tap, Fields fields )
    {
    if( writeHeader )
      presentSinkFieldsInternal( fields );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
    {
    }

  @Override
  public void sourcePrepare( FlowProcess<? extends Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    sourceCall.setContext( createInput( sourceCall.getInput() ) );

    sourceCall.getIncomingEntry().setTuple( TupleViews.createObjectArray() );
    }

  @Override
  public void sourceRePrepare( FlowProcess<? extends Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    sourceCall.setContext( createInput( sourceCall.getInput() ) );
    }

  @Override
  public boolean source( FlowProcess<? extends Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    String line = sourceCall.getContext().readLine();

    if( line == null )
      return false;

    if( skipHeader && sourceCall.getContext().getLineNumber() == 1 ) // todo: optimize this away
      line = sourceCall.getContext().readLine();

    if( line == null )
      return false;

    Object[] split = delimitedParser.parseLine( line );

    // assumption it is better to re-use than to construct new
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();

    TupleViews.reset( tuple, split );

    return true;
    }

  @Override
  public void sourceCleanup( FlowProcess<? extends Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    sourceCall.setContext( null );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
    {
    }

  @Override
  public void sinkPrepare( FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall )
    {
    OutputStream originalOutput = sinkCall.getOutput();
    sinkCall.setContext( createOutput( originalOutput ) );

    if( writeHeader && !isAppendingFile( sinkCall, originalOutput ) )
      {
      Fields fields = sinkCall.getOutgoingEntry().getFields();
      delimitedParser.joinFirstLine( fields, sinkCall.getContext() );

      sinkCall.getContext().println();
      }
    }

  protected boolean isAppendingFile( SinkCall<PrintWriter, OutputStream> sinkCall, OutputStream originalOutput )
    {
    try
      {
      return sinkCall.getTap().getSinkMode() == SinkMode.UPDATE &&
        originalOutput instanceof FileOutputStream &&
        ( (FileOutputStream) originalOutput ).getChannel().position() != 0;
      }
    catch( IOException exception )
      {
      // the error will be thrown immediately downstream
      return false;
      }
    }

  @Override
  public void sink( FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall ) throws IOException
    {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

    Iterable<String> strings = tupleEntry.asIterableOf( String.class );

    delimitedParser.joinLine( strings, sinkCall.getContext() );

    sinkCall.getContext().println();
    }

  @Override
  public void sinkCleanup( FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall )
    {
    sinkCall.getContext().flush();
    sinkCall.setContext( null );
    }

  @Override
  public String getExtension()
    {
    switch( getDelimiter().trim() )
      {
      case "\t":
        return "tsv";

      case ",":
        return "csv";
      }

    return "txt";
    }
  }
