/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import java.text.SimpleDateFormat;
import java.util.Calendar;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class DateFormatter is used to convert a date timestamp to a formatted string, where a timestamp
 * is the number of milliseconds since January 1, 1970, 00:00:00 GMT,  using the {@link SimpleDateFormat} syntax.
 */
public class DateFormatter extends DateOperation implements Function
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "datetime";

  /**
   * Constructor DateParser creates a new DateParser instance that creates a simple long time stamp of the parsed date.
   *
   * @param dateFormatString of type String
   */
  public DateFormatter( String dateFormatString )
    {
    super( 1, new Fields( FIELD_NAME ), dateFormatString );
    }

  /**
   * Constructor DateParser creates a new DateParser instance.
   *
   * @param fieldDeclaration of type Fields
   * @param dateFormatString of type String
   */
  public DateFormatter( Fields fieldDeclaration, String dateFormatString )
    {
    super( 1, fieldDeclaration, dateFormatString );
    }

  /** @see Function#operate(cascading.flow.FlowProcess,cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    Tuple output = new Tuple();

    long ts = functionCall.getArguments().getLong( 0 );

    Calendar calendar = getCalendar();

    calendar.setTimeInMillis( ts );

    output.add( getDateFormat().format( calendar.getTime() ) );

    functionCall.getOutputCollector().add( output );
    }

  }