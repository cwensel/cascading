/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

/**
 *
 */
public class Texts
  {
  /**
   * Field APACHE_DATE_FORMAT is the text date format of date in an Apache log file. <br/>
   * <code>dd/MMM/yyyy:HH:mm:ss Z</code>
   */
  public static final String APACHE_DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";

  /** Field APACHE_DATE_PARSER is a convenience {@link DateParser} instance for parsing the date from an Apache log file */
  public static final DateParser APACHE_DATE_PARSER = new DateParser( APACHE_DATE_FORMAT );

  /** Field TAB_JOINER is a predfined {@link FieldJoiner} for joining field values with the TAB character. */
  public static final FieldJoiner TAB_JOINER = new FieldJoiner( "\t" );
  }
