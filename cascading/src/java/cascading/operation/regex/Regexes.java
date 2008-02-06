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

package cascading.operation.regex;

import cascading.tuple.Fields;

/**
 *
 */
public class Regexes
  {
  /** Field TAB_SPLITTER is a predefined {@link RegexSplitter} for splitting on the TAB character */
  public static final RegexSplitter TAB_SPLITTER = new RegexSplitter( "\t" );

  /** Field APACHE_GROUPS defines the significant regex groups used in {@link #APACHE_GROUP_FIELDS} */
  public static final int[] APACHE_GROUPS = new int[]{1, 2, 3, 4, 5};

  /**
   * Field APACHE_GROUP_FIELDS are the field names of the groups returned by the APACHE_REGEX.<br/>
   * These fields are: "time", "method", "event", "status", and "size"
   */
  public static final Fields APACHE_GROUP_FIELDS = new Fields( "time", "method", "event", "status", "size" );
  /**
   * Field APACHE_REGEX is used to parse Apache log files.<br/>
   * <code>^[^ ]* +[^ ]* +[^ ]* +\[([^]]*)\] +\"([^ ]*) ([^ ]*) [^ ]*\" ([^ ]*) ([^ ]*).*$</code>
   */
  public static final String APACHE_REGEX = "^[^ ]* +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

  /** Field APACHE_PARSER is a predefined {@link RegexParser} for parsing Apache log files */
  public static final RegexParser APACHE_PARSER = new RegexParser( APACHE_GROUP_FIELDS, APACHE_REGEX, APACHE_GROUPS );
  }
