/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.flow.FlowRuntimeProps;
import org.slf4j.Logger;

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

  public static void logMemory( Logger logger, String message )
    {
    Runtime runtime = Runtime.getRuntime();
    long freeMem = runtime.freeMemory() / 1024 / 1024;
    long maxMem = runtime.maxMemory() / 1024 / 1024;
    long totalMem = runtime.totalMemory() / 1024 / 1024;

    logger.info( message + " (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );
    }

  public static void logCounters( Logger logger, String message, FlowProcess flowProcess )
    {
    String counters = flowProcess.getStringProperty( FlowRuntimeProps.LOG_COUNTERS );

    if( counters == null )
      return;

    String[] split = counters.split( "," );

    for( String value : split )
      {
      String counter[] = value.split( ":" );

      logger.info( "{} {}.{}={}", message, counter[ 0 ], counter[ 1 ], flowProcess.getCounterValue( counter[ 0 ], counter[ 1 ] ) );
      }
    }
  }
