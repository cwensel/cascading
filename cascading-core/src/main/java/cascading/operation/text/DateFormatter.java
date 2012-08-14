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

package cascading.operation.text;

import java.beans.ConstructorProperties;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/**
 * Class DateFormatter is used to convert a date timestamp to a formatted string, where a timestamp
 * is the number of milliseconds since January 1, 1970, 00:00:00 GMT,  using the {@link SimpleDateFormat} syntax.
 * <p/>
 * Note the timezone data is given to the SimpleDateFormat, not the internal Calendar instance which interprets
 * the 'timestamp' value as it is assumed the timestamp is already in GMT.
 */
public class DateFormatter extends DateOperation implements Function<Pair<SimpleDateFormat, Tuple>>
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "datetime";

  /**
   * Constructor DateParser creates a new DateParser instance that creates a simple long time stamp of the parsed date.
   *
   * @param dateFormatString of type String
   */
  @ConstructorProperties({"dateFormatString"})
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
  @ConstructorProperties({"fieldDeclaration", "dateFormatString"})
  public DateFormatter( Fields fieldDeclaration, String dateFormatString )
    {
    super( 1, fieldDeclaration, dateFormatString );
    }

  /**
   * Constructor DateFormatter creates a new DateFormatter instance.
   *
   * @param fieldDeclaration of type Fields
   * @param dateFormatString of type String
   * @param zone             of type TimeZone
   */
  @ConstructorProperties({"fieldDeclaration", "dateFormatString", "zone"})
  public DateFormatter( Fields fieldDeclaration, String dateFormatString, TimeZone zone )
    {
    super( 1, fieldDeclaration, dateFormatString, zone, null );
    }

  /**
   * Constructor DateFormatter creates a new DateFormatter instance.
   *
   * @param fieldDeclaration of type Fields
   * @param dateFormatString of type String
   * @param zone             of type TimeZone
   * @param locale           of type Locale
   */
  @ConstructorProperties({"fieldDeclaration", "dateFormatString", "zone", "locale"})
  public DateFormatter( Fields fieldDeclaration, String dateFormatString, TimeZone zone, Locale locale )
    {
    super( 1, fieldDeclaration, dateFormatString, zone, locale );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<SimpleDateFormat, Tuple>> functionCall )
    {
    long ts = functionCall.getArguments().getLong( 0 );

    Calendar calendar = getCalendar();

    calendar.setTimeInMillis( ts );

    functionCall.getContext().getRhs().set( 0, functionCall.getContext().getLhs().format( calendar.getTime() ) );

    functionCall.getOutputCollector().add( functionCall.getContext().getRhs() );
    }
  }