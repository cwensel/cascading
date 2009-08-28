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

package cascading.flow.hadoop;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 */
public class HadoopUtil
  {
  public static void initLog4j( Configuration jobConf )
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

  public static Configuration createConfiguration( Map<Object, Object> properties, Configuration defaultConf )
    {
    Configuration conf = defaultConf == null ? new Configuration() : new Configuration( defaultConf );

    if( properties == null )
      return conf;

    Set<Object> keys = new HashSet<Object>( properties.keySet() );

    // keys will only be grabbed if both key/value are String, so keep orig keys
    if( properties instanceof Properties )
      keys.addAll( ( (Properties) properties ).stringPropertyNames() );

    for( Object key : keys )
      {
      Object value = properties.get( key );

      if( value == null ) // don't stuff null values
        continue;

      // don't let these objects pass, even though toString is called below.
      if( value instanceof Class || value instanceof JobConf )
        continue;

      conf.set( key.toString(), value.toString() );
      }

    return conf;
    }

  public static Map<Object, Object> createProperties( JobConf jobConf )
    {
    Map<Object, Object> properties = new HashMap<Object, Object>();

    for( Map.Entry<String, String> entry : jobConf )
      properties.put( entry.getKey(), entry.getValue() );

    return properties;
    }
  }
