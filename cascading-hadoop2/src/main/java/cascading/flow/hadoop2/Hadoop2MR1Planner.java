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

package cascading.flow.hadoop2;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.hadoop.planner.HadoopPlanner;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class Hadoop2MR1Planner extends HadoopPlanner
  {
  /**
   * Method copyJobConf adds the given JobConf values to the given properties object. Use this method to pass
   * custom default Hadoop JobConf properties to Hadoop.
   *
   * @param properties    of type Map
   * @param configuration of type JobConf
   */
  public static void copyConfiguration( Map<Object, Object> properties, Configuration configuration )
    {
    for( Map.Entry<String, String> entry : configuration )
      properties.put( entry.getKey(), entry.getValue() );
    }

//  /**
//   * Method createJobConf returns a new JobConf instance using the values in the given properties argument.
//   *
//   * @param properties of type Map
//   * @return a JobConf instance
//   */
//  public static JobConf createJobConf( Map<Object, Object> properties )
//    {
//    JobConf conf = new JobConf();
//
//    copyProperties( conf, properties );
//
//    return conf;
//    }

  /**
   * Method copyProperties adds the given Map values to the given JobConf object.
   *
   * @param configuration of type JobConf
   * @param properties    of type Map
   */
  public static void copyProperties( Configuration configuration, Map<Object, Object> properties )
    {
    if( properties instanceof Properties )
      {
      Properties props = (Properties) properties;
      Set<String> keys = props.stringPropertyNames();

      for( String key : keys )
        configuration.set( key, props.getProperty( key ) );
      }
    else
      {
      for( Map.Entry<Object, Object> entry : properties.entrySet() )
        {
        if( entry.getValue() != null )
          configuration.set( entry.getKey().toString(), entry.getValue().toString() );
        }
      }
    }

  }
