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

package cascading.scheme.util;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.Tuples;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class DelimitedParser is a base class for parsing text delimited files.
 * <p/>
 * It maybe sub-classed to change its behavior.
 * <p/>
 * The interface {@link FieldTypeResolver} maybe used to clean and prepare field names
 * for data columns, and to infer type information from column names.
 */
public class DelimitedParser implements Serializable
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( DelimitedParser.class );

  /** Field SPECIAL_REGEX_CHARS */
  static final String SPECIAL_REGEX_CHARS = "([\\]\\[|.*<>\\\\$^?()=!+])";
  /** Field QUOTED_REGEX_FORMAT */
  static final String QUOTED_REGEX_FORMAT = "%2$s(?=(?:[^%1$s]*%1$s[^%1$s]*[^%1$s%2$s]*%1$s)*(?![^%1$s]*%1$s))";
  /** Field CLEAN_REGEX_FORMAT */
  static final String CLEAN_REGEX_FORMAT = "^(?:%1$s)(.*)(?:%1$s)$";
  /** Field ESCAPE_REGEX_FORMAT */
  static final String ESCAPE_REGEX_FORMAT = "(%1$s%1$s)";

  /** Field sourceFields */
  protected Fields sourceFields;

  /** Field splitPattern */
  protected Pattern splitPattern;
  /** Field cleanPattern */
  protected Pattern cleanPattern;
  /** Field escapePattern */
  protected Pattern escapePattern;
  /** Field delimiter * */
  protected String delimiter;
  /** Field quote */
  protected String quote;
  /** Field strict */
  protected boolean strict = true; // need to cache value across resets
  /** Field enforceStrict */
  protected boolean enforceStrict = true;
  /** Field numValues */
  protected int numValues;
  /** Field types */
  protected Class[] types;
  /** Field safe */
  protected boolean safe = true;
  /** fieldTypeResolver */
  protected FieldTypeResolver fieldTypeResolver;

  public DelimitedParser( String delimiter, String quote, Class[] types )
    {
    reset( delimiter, quote, types, strict, safe, null, null, null );
    }

  public DelimitedParser( String delimiter, String quote, Class[] types, boolean strict, boolean safe )
    {
    reset( delimiter, quote, types, strict, safe, null, null, null );
    }

  public DelimitedParser( String delimiter, String quote, FieldTypeResolver fieldTypeResolver )
    {
    reset( delimiter, quote, null, strict, safe, null, null, fieldTypeResolver );
    }

  public DelimitedParser( String delimiter, String quote, Class[] types, boolean strict, boolean safe, FieldTypeResolver fieldTypeResolver )
    {
    reset( delimiter, quote, types, strict, safe, null, null, fieldTypeResolver );
    }

  public DelimitedParser( String delimiter, String quote, Class[] types, boolean strict, boolean safe, Fields sourceFields, Fields sinkFields )
    {
    reset( delimiter, quote, types, strict, safe, sourceFields, sinkFields, null );
    }

  public DelimitedParser( String delimiter, String quote, Class[] types, boolean strict, boolean safe, Fields sourceFields, Fields sinkFields, FieldTypeResolver fieldTypeResolver )
    {
    reset( delimiter, quote, types, strict, safe, sourceFields, sinkFields, fieldTypeResolver );
    }

  public void reset( Fields sourceFields, Fields sinkFields )
    {
    reset( delimiter, quote, types, strict, safe, sourceFields, sinkFields, fieldTypeResolver );
    }

  public void reset( String delimiter, String quote, Class[] types, boolean strict, boolean safe, Fields sourceFields, Fields sinkFields, FieldTypeResolver fieldTypeResolver )
    {
    if( delimiter == null || delimiter.isEmpty() )
      throw new IllegalArgumentException( "delimiter may not be null or empty" );

    if( delimiter.equals( quote ) )
      throw new IllegalArgumentException( "delimiter and quote character may not be the same value, got: '" + delimiter + "'" );

    this.delimiter = delimiter;
    this.strict = strict;
    this.safe = safe;
    this.fieldTypeResolver = fieldTypeResolver;

    if( quote != null && !quote.isEmpty() ) // if empty, leave null
      this.quote = quote;

    if( types != null && types.length == 0 )
      this.types = null;

    if( types != null )
      this.types = Arrays.copyOf( types, types.length );

    if( sourceFields == null || sinkFields == null )
      return;

    if( types == null && sourceFields.hasTypes() )
      this.types = sourceFields.getTypesClasses(); // gets a copy

    this.sourceFields = sourceFields;
    this.numValues = Math.max( sourceFields.size(), sinkFields.size() ); // if asymmetrical, one is zero

    this.enforceStrict = this.strict;

    if( sourceFields.isUnknown() )
      this.enforceStrict = false;

    if( !sinkFields.isAll() && numValues == 0 )
      throw new IllegalArgumentException( "may not be zero declared fields, found: " + sinkFields.printVerbose() );

    splitPattern = createSplitPatternFor( this.delimiter, this.quote );
    cleanPattern = createCleanPatternFor( this.quote );
    escapePattern = createEscapePatternFor( this.quote );

    if( this.types != null && sinkFields.isAll() )
      throw new IllegalArgumentException( "when using Fields.ALL, field types may not be used" );

    if( this.types != null && this.types.length != sinkFields.size() )
      throw new IllegalArgumentException( "num of types must equal number of fields: " + sinkFields.printVerbose() + ", found: " + types.length );
    }

  public String getDelimiter()
    {
    return delimiter;
    }

  public String getQuote()
    {
    return quote;
    }

  /**
   * Method createEscapePatternFor creates a regex {@link java.util.regex.Pattern} cleaning quote escapes from a String.
   * <p/>
   * If {@code quote} is null or empty, a null value will be returned;
   *
   * @param quote of type String
   * @return Pattern
   */
  public static Pattern createEscapePatternFor( String quote )
    {
    if( quote == null || quote.isEmpty() )
      return null;

    return Pattern.compile( String.format( ESCAPE_REGEX_FORMAT, quote ) );
    }

  /**
   * Method createCleanPatternFor creates a regex {@link java.util.regex.Pattern} for removing quote characters from a String.
   * <p/>
   * If {@code quote} is null or empty, a null value will be returned;
   *
   * @param quote of type String
   * @return Pattern
   */
  public static Pattern createCleanPatternFor( String quote )
    {
    if( quote == null || quote.isEmpty() )
      return null;

    return Pattern.compile( String.format( CLEAN_REGEX_FORMAT, quote ) );
    }

  /**
   * Method createSplitPatternFor creates a regex {@link java.util.regex.Pattern} for splitting a line of text into its component
   * parts using the given delimiter and quote Strings. {@code quote} may be null.
   *
   * @param delimiter of type String
   * @param quote     of type String
   * @return Pattern
   */
  public static Pattern createSplitPatternFor( String delimiter, String quote )
    {
    String escapedDelimiter = delimiter.replaceAll( SPECIAL_REGEX_CHARS, "\\\\$1" );

    if( quote == null || quote.isEmpty() )
      return Pattern.compile( escapedDelimiter );
    else
      return Pattern.compile( String.format( QUOTED_REGEX_FORMAT, quote, escapedDelimiter ) );
    }

  /**
   * Method createSplit will split the given {@code value} with the given {@code splitPattern}.
   *
   * @param value        of type String
   * @param splitPattern of type Pattern
   * @param numValues    of type int
   * @return String[]
   */
  public String[] createSplit( String value, Pattern splitPattern, int numValues )
    {
    return splitPattern.split( value, numValues );
    }

  /**
   * Method cleanSplit will return a quote free array of String values, the given {@code split} array
   * will be updated in place.
   * <p/>
   * If {@code cleanPattern} is null, quote cleaning will not be performed, but all empty String values
   * will be replaces with a {@code null} value.
   *
   * @param split         of type Object[]
   * @param cleanPattern  of type Pattern
   * @param escapePattern of type Pattern
   * @param quote         of type String
   * @return Object[] as a convenience
   */
  public Object[] cleanSplit( Object[] split, Pattern cleanPattern, Pattern escapePattern, String quote )
    {
    if( cleanPattern != null )
      {
      for( int i = 0; i < split.length; i++ )
        {
        split[ i ] = cleanPattern.matcher( (String) split[ i ] ).replaceAll( "$1" );
        split[ i ] = escapePattern.matcher( (String) split[ i ] ).replaceAll( quote );
        }
      }

    for( int i = 0; i < split.length; i++ )
      {
      if( ( (String) split[ i ] ).isEmpty() )
        split[ i ] = null;
      }

    return split;
    }

  public Fields parseFirstLine( FlowProcess flowProcess, Tap tap )
    {
    Fields sourceFields;
    TupleEntryIterator iterator = null;

    try
      {
      if( !tap.resourceExists( flowProcess.getConfigCopy() ) )
        throw new TapException( "unable to read fields from tap: " + tap + ", does not exist" );

      iterator = tap.openForRead( flowProcess );

      TupleEntry entry = iterator.hasNext() ? iterator.next() : null;

      if( entry == null )
        throw new TapException( "unable to read fields from tap: " + tap + ", is empty" );

      Object[] result = onlyParseLine( entry.getTuple().getString( 0 ) ); // don't coerce if type info is avail

      result = cleanParsedLine( result );

      Class[] inferred = inferTypes( result ); // infer type from field name, after removing quotes/escapes

      result = cleanFields( result ); // clean field names to remove any meta-data or manage case

      sourceFields = new Fields( Arrays.copyOf( result, result.length, Comparable[].class ) );

      if( inferred != null )
        sourceFields = sourceFields.applyTypes( inferred );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to read fields from tap: " + tap, exception );
      }
    finally
      {
      if( iterator != null )
        {
        try
          {
          iterator.close();
          }
        catch( IOException exception )
          {
          // do nothing
          }
        }
      }

    return sourceFields;
    }

  public Object[] parseLine( String line )
    {
    Object[] split = onlyParseLine( line );

    split = cleanParsedLine( split );

    return coerceParsedLine( line, split );
    }

  protected Object[] cleanParsedLine( Object[] split )
    {
    return cleanSplit( split, cleanPattern, escapePattern, quote );
    }

  protected Object[] coerceParsedLine( String line, Object[] split )
    {
    if( types != null ) // forced null in ctor
      {
      Object[] result = new Object[ split.length ];

      for( int i = 0; i < split.length; i++ )
        {
        try
          {
          result[ i ] = Tuples.coerce( split[ i ], types[ i ] );
          }
        catch( Exception exception )
          {
          String message = "field " + sourceFields.get( i ) + " cannot be coerced from : " + split[ i ] + " to: " + types[ i ].getName();

          result[ i ] = null;

          LOG.warn( message, exception );

          if( !safe )
            throw new TapException( message, exception, new Tuple( line ) ); // trap actual line data
          }
        }

      split = result;
      }

    return split;
    }

  protected Object[] onlyParseLine( String line )
    {
    Object[] split = createSplit( line, splitPattern, numValues == 0 ? 0 : -1 );

    if( numValues != 0 && split.length != numValues )
      {
      String message = "did not parse correct number of values from input data, expected: " + numValues + ", got: " + split.length + ":" + Util.join( ",", (String[]) split );

      if( enforceStrict )
        throw new TapException( message, new Tuple( line ) ); // trap actual line data

      LOG.warn( message );

      Object[] array = new Object[ numValues ];
      Arrays.fill( array, "" );
      System.arraycopy( split, 0, array, 0, split.length );

      split = array;
      }

    return split;
    }

  public Appendable joinFirstLine( Iterable iterable, Appendable buffer )
    {
    iterable = prepareFields( iterable );

    return joinLine( iterable, buffer );
    }

  public Appendable joinLine( Iterable iterable, Appendable buffer )
    {
    try
      {
      if( quote != null )
        return joinWithQuote( iterable, buffer );

      return joinNoQuote( iterable, buffer );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to append data", exception );
      }
    }

  protected Appendable joinWithQuote( Iterable tuple, Appendable buffer ) throws IOException
    {
    int count = 0;

    for( Object value : tuple )
      {
      if( count != 0 )
        buffer.append( delimiter );

      if( value != null )
        {
        String valueString = value.toString();

        if( valueString.contains( quote ) )
          valueString = valueString.replaceAll( quote, quote + quote );

        if( valueString.contains( delimiter ) )
          valueString = quote + valueString + quote;

        buffer.append( valueString );
        }

      count++;
      }

    return buffer;
    }

  protected Appendable joinNoQuote( Iterable tuple, Appendable buffer ) throws IOException
    {
    int count = 0;

    for( Object value : tuple )
      {
      if( count != 0 )
        buffer.append( delimiter );

      if( value != null )
        buffer.append( value.toString() );

      count++;
      }

    return buffer;
    }

  protected Class[] inferTypes( Object[] result )
    {
    if( fieldTypeResolver == null )
      return null;

    Class[] inferred = new Class[ result.length ];

    for( int i = 0; i < result.length; i++ )
      {
      String field = (String) result[ i ];

      inferred[ i ] = fieldTypeResolver.inferTypeFrom( i, field );
      }

    return inferred;
    }

  protected Iterable prepareFields( Iterable fields )
    {
    if( fieldTypeResolver == null )
      return fields;

    List result = new ArrayList();

    for( Object field : fields )
      {
      int index = result.size();
      Class type = types != null ? types[ index ] : null;
      String value = fieldTypeResolver.prepareField( index, (String) field, type );

      if( value != null && !value.isEmpty() )
        field = value;

      result.add( field );
      }

    return result;
    }

  protected Object[] cleanFields( Object[] result )
    {
    if( fieldTypeResolver == null )
      return result;

    for( int i = 0; i < result.length; i++ )
      {
      Class type = types != null ? types[ i ] : null;
      String value = fieldTypeResolver.cleanField( i, (String) result[ i ], type );

      if( value != null && !value.isEmpty() )
        result[ i ] = value;
      }

    return result;
    }
  }