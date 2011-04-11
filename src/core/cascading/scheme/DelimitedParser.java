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

package cascading.scheme;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.util.Util;
import org.apache.log4j.Logger;

public class DelimitedParser implements Serializable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( DelimitedParser.class );

  /** Field SPECIAL_REGEX_CHARS */
  static final String SPECIAL_REGEX_CHARS = "([\\]\\[|.*<>\\\\$^?()=!+])";
  /** Field QUOTED_REGEX_FORMAT */
  static final String QUOTED_REGEX_FORMAT = "%2$s(?!(?:[^%1$s%2$s]|[^%1$s%2$s]%2$s[^%1$s])+%1$s)";
  /** Field CLEAN_REGEX_FORMAT */
  static final String CLEAN_REGEX_FORMAT = "^(?:%1$s)(.*)(?:%1$s)$";
  /** Field ESCAPE_REGEX_FORMAT */
  static final String ESCAPE_REGEX_FORMAT = "(%1$s%1$s)";

  /** Field sourceFields */
  private final Fields sourceFields;

  /** Field splitPattern */
  protected Pattern splitPattern;
  /** Field cleanPattern */
  protected Pattern cleanPattern;
  /** Field escapePattern */
  protected Pattern escapePattern;
  /** Field skipHeader */
  boolean skipHeader;
  /** Field delimiter * */
  final String delimiter;
  /** Field quote */
  String quote;
  /** Field strict */
  boolean strict = true;
  /** Field numValues */
  final int numValues;
  /** Field types */
  Class[] types;
  /** Field safe */
  boolean safe = true;

  public DelimitedParser( String delimiter, String quote, Class[] types, boolean strict, boolean safe, Fields sourceFields, Fields sinkFields )
    {
    this.delimiter = delimiter;
    this.strict = strict;
    this.safe = safe;
    this.sourceFields = sourceFields;
    this.numValues = sinkFields.size();

    if( sinkFields.isAll() )
      this.strict = false;

    if( !sinkFields.isAll() && numValues == 0 )
      throw new IllegalArgumentException( "may not be zero declared fields, found: " + sinkFields.printVerbose() );

    if( quote != null && !quote.isEmpty() ) // if empty, leave null
      this.quote = quote;

    splitPattern = createSplitPatternFor( this.delimiter, this.quote );
    cleanPattern = createCleanPatternFor( this.quote );
    escapePattern = createEscapePatternFor( this.quote );

    if( types != null && types.length == 0 )
      this.types = null;

    if( types != null )
      this.types = Arrays.copyOf( types, types.length );

    if( this.types != null && sinkFields.isAll() )
      throw new IllegalArgumentException( "when using Fields.ALL, field types may not be used" );

    if( this.types != null && this.types.length != sinkFields.size() )
      throw new IllegalArgumentException( "num of types must equal number of fields: " + sinkFields.printVerbose() + ", found: " + types.length );
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
  public static String[] createSplit( String value, Pattern splitPattern, int numValues )
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
  public static Object[] cleanSplit( Object[] split, Pattern cleanPattern, Pattern escapePattern, String quote )
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

  public Object[] parseLine( String line )
    {
    Object[] split = createSplit( line, splitPattern, numValues );

    if( numValues != 0 && split.length != numValues )
      {
      String message = "did not parse correct number of values from input data, expected: " + numValues + ", got: " + split.length + ":" + Util.join( ",", (String[]) split );

      if( strict )
        throw new TapException( message );

      LOG.warn( message );

      Object[] array = new Object[ numValues ];
      Arrays.fill( array, "" );
      System.arraycopy( split, 0, array, 0, split.length );

      split = array;
      }

    cleanSplit( split, cleanPattern, escapePattern, quote );

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
          String message = "field " + sourceFields.get( i ) + " cannot be coerced from : " + result[ i ] + " to: " + types[ i ].getName();

          result[ i ] = null;

          LOG.warn( message, exception );

          if( !safe )
            throw new TapException( message, exception );
          }
        }

      split = result;
      }
    return split;
    }

  // todo: use StringBuffer instead
  public String joinLine( Tuple tuple, Object[] array )
    {
    Object[] buffer = Tuples.asArray( tuple, array );

    if( quote != null )
      {
      for( int i = 0; i < buffer.length; i++ )
        {
        Object value = buffer[ i ];

        if( value == null )
          continue;

        String valueString = value.toString();

        if( valueString.contains( quote ) )
          valueString = valueString.replaceAll( quote, quote + quote );

        if( valueString.contains( delimiter ) )
          valueString = quote + valueString + quote;

        buffer[ i ] = valueString;
        }
      }

    return Util.join( buffer, delimiter, false );
    }

  }