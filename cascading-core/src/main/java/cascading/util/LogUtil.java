/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
