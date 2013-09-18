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

package cascading.operation.text;

import java.beans.ConstructorProperties;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/**
 * Class DateParser is used to convert a text date string to a timestamp, the number of milliseconds
 * since January 1, 1970, 00:00:00 GMT, using the {@link SimpleDateFormat} syntax.
 * <p/>
 * If given, individual {@link Calendar} fields can be stored in unique fields for a given {@link TimeZone} and {@link Locale}.
 */
public class DateParser extends DateOperation implements Function<Pair<SimpleDateFormat, Tuple>>
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "ts";

  /** Field calendarFields */
  private int[] calendarFields;

  /**
   * Constructor DateParser creates a new DateParser instance that creates a simple long time stamp of the parsed date.
   *
   * @param dateFormatString of type String
   */
  @ConstructorProperties({"dateFormatString"})
  public DateParser( String dateFormatString )
    {
    super( 1, new Fields( FIELD_NAME ), dateFormatString );
    }

  /**
   * Constructor DateParser creates a new DateParser instance.
   *
   * @param fieldDeclaration of type Fields
   * @param dateFormatString of type String
   */
  @ConstructorProperties({"fieldDeclaration", "dateFormatString"})
  public DateParser( Fields fieldDeclaration, String dateFormatString )
    {
    super( 1, fieldDeclaration, dateFormatString );
    }

  /**
   * Constructor DateParser creates a new DateParser instance, where calendarFields is an int[] of {@link Calendar} field
   * values. See {@link Calendar#get(int)}.
   *
   * @param fieldDeclaration of type Fields
   * @param calendarFields   of type int[]
   * @param dateFormatString of type String
   */
  @ConstructorProperties({"fieldDeclaration", "calendarFields", "dateFormatString"})
  public DateParser( Fields fieldDeclaration, int[] calendarFields, String dateFormatString )
    {
    this( fieldDeclaration, calendarFields, null, null, dateFormatString );
    }

  /**
   * Constructor DateParser creates a new DateParser instance, where zone and locale are passed to the internal
   * {@link SimpleDateFormat} instance.
   *
   * @param fieldDeclaration of type Fields
   * @param zone             of type TimeZone
   * @param locale           of type Locale
   * @param dateFormatString of type String
   */
  @ConstructorProperties({"fieldDeclaration", "zone", "locale", "dateFormatString"})
  public DateParser( Fields fieldDeclaration, TimeZone zone, Locale locale, String dateFormatString )
    {
    this( fieldDeclaration, null, zone, locale, dateFormatString );
    }

  /**
   * Constructor DateParser creates a new DateParser instance, where calendarFields is an int[] of {@link Calendar} field
   * values. See {@link Calendar#get(int)}. The {@link TimeZone} and/or {@link Locale} may also be set.
   *
   * @param fieldDeclaration of type Fields
   * @param calendarFields   of type int[]
   * @param zone             of type TimeZone
   * @param locale           of type Locale
   * @param dateFormatString of type String
   */
  @ConstructorProperties({"fieldDeclaration", "calendarFields", "zone", "locale", "dateFormatString"})
  public DateParser( Fields fieldDeclaration, int[] calendarFields, TimeZone zone, Locale locale, String dateFormatString )
    {
    super( 1, fieldDeclaration, dateFormatString, zone, locale );

    if( calendarFields != null )
      {
      this.calendarFields = Arrays.copyOf( calendarFields, calendarFields.length );

      if( fieldDeclaration.size() != calendarFields.length )
        throw new IllegalArgumentException( "fieldDeclaration must be same size as calendarFields, was " + fieldDeclaration.print() + " with calendar size: " + calendarFields.length );
      }
    else
      {
      if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
        throw new IllegalArgumentException( "fieldDeclaration may only declare one field name, got " + fieldDeclaration.print() );
      }
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<SimpleDateFormat, Tuple>> functionCall )
    {
    Tuple output = functionCall.getContext().getRhs();

    try
      {
      String value = functionCall.getArguments().getString( 0 );

      if( value == null ) // if null, return null for the field
        {
        output.set( 0, null ); // safe to call set, tuple is size of 1

        functionCall.getOutputCollector().add( output );

        return;
        }

      Date date = functionCall.getContext().getLhs().parse( value );

      if( calendarFields == null )
        output.set( 0, date.getTime() ); // safe to call set, tuple is size of 1
      else
        makeCalendarFields( output, date );
      }
    catch( ParseException exception )
      {
      throw new OperationException( "unable to parse input value: " + functionCall.getArguments().getObject( 0 ), exception );
      }

    functionCall.getOutputCollector().add( output );
    }

  private void makeCalendarFields( Tuple output, Date date )
    {
    output.clear();

    Calendar calendar = getCalendar();
    calendar.setTime( date );

    for( int i = 0; i < calendarFields.length; i++ )
    //noinspection MagicConstant
      output.add( calendar.get( calendarFields[ i ] ) );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof DateParser ) )
      return false;
    if( !super.equals( object ) )
      return false;

    DateParser that = (DateParser) object;

    if( !Arrays.equals( calendarFields, that.calendarFields ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( calendarFields != null ? Arrays.hashCode( calendarFields ) : 0 );
    return result;
    }
  }
