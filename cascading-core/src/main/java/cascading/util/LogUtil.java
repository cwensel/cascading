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

package cascading.util;

/**
 *
 */
public class LogUtil
  {
  public static void setLog4jLevel( String[] logger )
    {
    setLog4jLevel( logger[ 0 ], logger[ 1 ] );
    }

  public static void setLog4jLevel( String logger, String level )
    {
    // removing logj4 dependency
    // org.apache.log4j.Logger.getLogger( logger[ 0 ] ).setLevel( org.apache.log4j.Level.toLevel( logger[ 1 ] ) );

    Object loggerObject = Util.invokeStaticMethod( "org.apache.log4j.Logger", "getLogger",
      new Object[]{logger}, new Class[]{String.class} );

    Object levelObject = Util.invokeStaticMethod( "org.apache.log4j.Level", "toLevel",
      new Object[]{level}, new Class[]{String.class} );

    Util.invokeInstanceMethod( loggerObject, "setLevel",
      new Object[]{levelObject}, new Class[]{levelObject.getClass()} );
    }
  }
