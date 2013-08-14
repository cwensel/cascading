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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/** Class DateOperation is the base class for {@link DateFormatter} and {@link DateParser}. */
public class DateOperation extends BaseOperation<Pair<SimpleDateFormat, Tuple>>
  {
  /** Field zone */
  protected TimeZone zone;
  /** Field locale */
  protected Locale locale;
  /** Field dateFormatString */
  final String dateFormatString;

  /**
   * Constructor DateOperation creates a new DateOperation instance.
   *
   * @param numArgs          of type int
   * @param fieldDeclaration of type Fields
   * @param dateFormatString of type String
   */
  @ConstructorProperties({"numArgs", "fieldDeclaration", "dateFormatString"})
  public DateOperation( int numArgs, Fields fieldDeclaration, String dateFormatString )
    {
    super( numArgs, fieldDeclaration );
    this.dateFormatString = dateFormatString;

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field name, got " + fieldDeclaration.print() );
    }

  /**
   * Constructor DateOperation creates a new DateOperation instance.
   *
   * @param numArgs          of type int
   * @param fieldDeclaration of type Fields
   * @param dateFormatString of type String
   * @param zone             of type TimeZone
   * @param locale           of type Locale
   */
  @ConstructorProperties({"numArgs", "fieldDeclaration", "dateFormatString", "zone", "locale"})
  public DateOperation( int numArgs, Fields fieldDeclaration, String dateFormatString, TimeZone zone, Locale locale )
    {
    super( numArgs, fieldDeclaration );
    this.dateFormatString = dateFormatString;
    this.zone = zone;
    this.locale = locale;
    }

  public String getDateFormatString()
    {
    return dateFormatString;
    }

  /**
   * Method getDateFormat returns the dateFormat of this DateParser object.
   *
   * @return the dateFormat (type SimpleDateFormat) of this DateParser object.
   */
  public SimpleDateFormat getDateFormat()
    {
    SimpleDateFormat dateFormat = new SimpleDateFormat( dateFormatString, getLocale() );

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
  public void prepare( FlowProcess flowProcess, OperationCall<Pair<SimpleDateFormat, Tuple>> operationCall )
    {
    operationCall.setContext( new Pair<SimpleDateFormat, Tuple>( getDateFormat(), Tuple.size( 1 ) ) );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof DateOperation ) )
      return false;
    if( !super.equals( object ) )
      return false;

    DateOperation that = (DateOperation) object;

    if( dateFormatString != null ? !dateFormatString.equals( that.dateFormatString ) : that.dateFormatString != null )
      return false;
    if( locale != null ? !locale.equals( that.locale ) : that.locale != null )
      return false;
    if( zone != null ? !zone.equals( that.zone ) : that.zone != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( zone != null ? zone.hashCode() : 0 );
    result = 31 * result + ( locale != null ? locale.hashCode() : 0 );
    result = 31 * result + ( dateFormatString != null ? dateFormatString.hashCode() : 0 );
    return result;
    }
  }
