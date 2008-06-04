/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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
import java.util.Locale;
import java.util.TimeZone;

import cascading.operation.Operation;
import cascading.tuple.Fields;

/**
 *
 */
public class DateOperation extends Operation
  {
  /** Field zone */
  protected TimeZone zone;
  /** Field locale */
  protected Locale locale;
  /** Field dateFormatString */
  final String dateFormatString;
  /** Field dateFormat */
  transient SimpleDateFormat dateFormat;

  public DateOperation( int numArgs, Fields fieldDeclaration, String dateFormatString )
    {
    super( numArgs, fieldDeclaration );
    this.dateFormatString = dateFormatString;

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field name, got " + fieldDeclaration.print() );
    }

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
      dateFormat = new SimpleDateFormat( dateFormatString, getLocale() );

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
    return Calendar.getInstance( getZone(), getLocale() );
    }

  }
