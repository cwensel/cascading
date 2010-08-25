/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import cascading.operation.BaseOperation;
import cascading.tuple.Fields;

/** Class DateOperation is the base class for {@link DateFormatter} and {@link DateParser}. */
public class DateOperation extends BaseOperation
  {
  /** Field zone */
  protected TimeZone zone;
  /** Field locale */
  protected Locale locale;
  /** Field dateFormatString */
  final String dateFormatString;
  /** Field dateFormat */
  transient SimpleDateFormat dateFormat;

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

  /**
   * Method getDateFormat returns the dateFormat of this DateParser object.
   *
   * @return the dateFormat (type SimpleDateFormat) of this DateParser object.
   */
  public SimpleDateFormat getDateFormat()
    {
    if( dateFormat == null )
      {
      dateFormat = new SimpleDateFormat( dateFormatString, getLocale() );
      dateFormat.setTimeZone( getZone() );
      }

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
