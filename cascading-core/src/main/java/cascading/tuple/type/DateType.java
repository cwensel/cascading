/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tuple.type;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

import cascading.CascadingException;
import cascading.tuple.coerce.Coercions;
import cascading.util.Util;

/**
 * Class DateType is an implementation of {@link CoercibleType}.
 * <p>
 * Given a {@code dateFormatString}, using the {@link SimpleDateFormat} format, this CoercibleType
 * will convert a value from the formatted string to a {@code Long} canonical type and back.
 * <p>
 * This class when presented with a Long timestamp value will assume the value is in UTC.
 * <p>
 * See {@link cascading.operation.text.DateParser} and {@link cascading.operation.text.DateFormatter} for similar
 * Operations for use within a pipe assembly.
 */
public class DateType implements CoercibleType<Long>
  {
  /** Field zone */
  protected TimeZone zone;
  /** Field locale */
  protected Locale locale;
  /** Field dateFormatString */
  protected String dateFormatString;
  /** Field dateFormat */
  private transient SimpleDateFormat dateFormat;

  /**
   * Create a new DateType instance.
   *
   * @param dateFormatString
   * @param zone
   * @param locale
   */
  public DateType( String dateFormatString, TimeZone zone, Locale locale )
    {
    this.zone = zone;
    this.locale = locale;
    this.dateFormatString = dateFormatString;
    }

  public DateType( String dateFormatString, TimeZone zone )
    {
    this.zone = zone;
    this.dateFormatString = dateFormatString;
    }

  /**
   * Create a new DateType instance.
   *
   * @param dateFormatString
   */
  public DateType( String dateFormatString )
    {
    this.dateFormatString = dateFormatString;
    }

  @Override
  public Class getCanonicalType()
    {
    return Long.TYPE;
    }

  public SimpleDateFormat getDateFormat()
    {
    if( dateFormat != null )
      return dateFormat;

    dateFormat = new SimpleDateFormat( dateFormatString, getLocale() );

    dateFormat.setTimeZone( getZone() );

    return dateFormat;
    }

  private Locale getLocale()
    {
    if( locale != null )
      return locale;

    return Locale.getDefault();
    }

  private TimeZone getZone()
    {
    if( zone != null )
      return zone;

    return TimeZone.getTimeZone( "UTC" );
    }

  protected Calendar getCalendar()
    {
    return Calendar.getInstance( TimeZone.getTimeZone( "UTC" ), getLocale() );
    }

  @Override
  public <T> ToCanonical<T, Long> from( Type from )
    {
    if( from == Long.class )
      return ( v ) -> (Long) v;

    if( from == String.class )
      return ( v ) -> v == null ? null : parse( (String) v ).getTime();

    if( from == Date.class )
      return ( v ) -> v == null ? null : ( (Date) v ).getTime(); // in UTC

    return this::canonical;
    }

  @Override
  public Long canonical( Object value )
    {
    if( value == null )
      return null;

    Class from = value.getClass();

    if( from == Long.class )
      return (Long) value;

    if( from == String.class )
      return parse( (String) value ).getTime();

    if( from == Date.class )
      return ( (Date) value ).getTime(); // in UTC

    throw new CascadingException( "unknown type coercion requested from: " + Util.getTypeName( from ) );
    }

  @Override
  public <T> CoercionFrom<Long, T> to( Type to )
    {
    boolean returnZero = Coercions.primitives.containsKey( to );

    if( to == Long.class || to == Long.TYPE || to == Object.class || DateType.class == to.getClass() )
      return ( v ) -> v == null ? (T) nullCoercion( returnZero ) : (T) v;

    if( to == String.class )
      return ( v ) -> v == null ? (T) nullCoercion( returnZero ) : (T) toString( v );

    return CoercibleType.super.to( to );
    }

  @Override
  public Object coerce( Object value, Type to )
    {
    // we are expecting a Long, as its our canonical type
    if( value == null )
      return nullCoercion( Coercions.primitives.containsKey( to ) );

    Class<?> from = value.getClass();

    if( from != Long.class )
      throw new IllegalStateException( "was not normalized" );

    // no coercion, or already in canonical form
    if( to == Long.class || to == Long.TYPE || to == Object.class || DateType.class == to.getClass() )
      return value;

    if( to == String.class )
      return toString( (Long) value );

    Coercions.Coerce<?> coerce = Coercions.coercions.get( to );

    if( coerce != null )
      return coerce.coerce( value );

    throw new CascadingException( "unknown type coercion requested, from: " + Util.getTypeName( from ) + " to: " + Util.getTypeName( to ) );
    }

  private Object nullCoercion( boolean returnZero )
    {
    if( returnZero )
      return 0;

    return null;
    }

  protected String toString( Long value )
    {
    Calendar calendar = getCalendar();

    calendar.setTimeInMillis( value );

    return getDateFormat().format( calendar.getTime() );
    }

  protected Date parse( String value )
    {
    try
      {
      return getDateFormat().parse( value );
      }
    catch( ParseException exception )
      {
      throw new CascadingException( "unable to parse value: " + value + " with format: " + dateFormatString );
      }
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof DateType ) )
      return false;
    DateType dateType = (DateType) object;
    return Objects.equals( zone, dateType.zone ) &&
      Objects.equals( locale, dateType.locale ) &&
      Objects.equals( dateFormatString, dateType.dateFormatString );
    }

  @Override
  public int hashCode()
    {
    return Objects.hash( zone, locale, dateFormatString );
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "DateType{" );
    sb.append( "dateFormatString='" ).append( dateFormatString ).append( '\'' );
    sb.append( "," );
    sb.append( "canonicalType='" ).append( getCanonicalType() ).append( '\'' );
    sb.append( '}' );
    return sb.toString();
    }
  }
