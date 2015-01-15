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
