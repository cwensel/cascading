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

package cascading.flow.hadoop;

import java.util.Enumeration;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 */
public class HadoopUtil
  {
  public static void initLog4j( JobConf jobConf )
    {
    String values = jobConf.get( "log4j.logger", null );

    if( values == null || values.length() == 0 )
      return;

    String[] elements = values.split( "," );

    for( String element : elements )
      {
      String[] logger = element.split( "=" );

      Logger.getLogger( logger[ 0 ] ).setLevel( Level.toLevel( logger[ 1 ] ) );
      }
    }

  public static JobConf createJobConf( Properties properties )
    {
    JobConf jobConf = new JobConf();

    if( properties == null )
      return jobConf;

    Enumeration enumeration = properties.propertyNames();

    while( enumeration.hasMoreElements() )
      {
      String key = (String) enumeration.nextElement();
      String value = properties.getProperty( key );

      if( value == null )
        throw new IllegalStateException( "property value was null for key: " + key );

      jobConf.set( key, value );
      }

    return jobConf;
    }
  }
