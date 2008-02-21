/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

package cascading.operation.text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import cascading.operation.Function;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * Class DateParser is used to convert a text date string to a timestamp, the number of milliseconds since January 1, 1970, 00:00:00 GMT,  using the
 * {@link SimpleDateFormat} syntax.
 * <p/>
 * If given, individual {@link Calendar} fields can be stored in unique fields for a given {@link TimeZone} and {@link Locale}.
 */
public class DateParser extends DateOperation implements Function
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
  public DateParser( Fields fieldDeclaration, int[] calendarFields, String dateFormatString )
    {
    this( fieldDeclaration, calendarFields, null, null, dateFormatString );
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
  public DateParser( Fields fieldDeclaration, int[] calendarFields, TimeZone zone, Locale locale, String dateFormatString )
    {
    super( 1, fieldDeclaration, dateFormatString, zone, locale );
    this.calendarFields = calendarFields;

    if( fieldDeclaration.size() != calendarFields.length )
      throw new IllegalArgumentException(
        "fieldDeclaration must be same size as calendarFields, was " + fieldDeclaration.print() + " with calendar size: " + calendarFields.length );
    }

  /** @see Function#operate(TupleEntry, TupleCollector) */
  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    Tuple output = new Tuple();

    try
      {
      Date date = getDateFormat().parse( (String) input.get( 0 ) );

      if( calendarFields == null )
        output.add( date.getTime() );
      else
        makeCalendarFields( output, date );
      }
    catch( ParseException exception )
      {
      throw new OperationException( "unable to parse input value: " + input.get( 0 ), exception );
      }

    outputCollector.add( output );
    }

  private void makeCalendarFields( Tuple output, Date date )
    {
    Calendar calendar = getCalendar();
    calendar.setTime( date );

    for( int i = 0; i < calendarFields.length; i++ )
      output.add( calendar.get( calendarFields[ i ] ) );
    }
  }
