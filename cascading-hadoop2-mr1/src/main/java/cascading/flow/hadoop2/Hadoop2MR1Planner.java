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

package cascading.flow.hadoop2;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.hadoop.planner.HadoopPlanner;
import org.apache.hadoop.conf.Configuration;

/**
 * Class Hadoop2MR1Planner is the core Hadoop MapReduce planner used by default through the {@link cascading.flow.hadoop2.Hadoop2MR1FlowConnector}.
 * <p/>
 * Notes:
 * <p/>
 * <strong>Custom JobConf properties</strong><br/>
 * A custom JobConf instance can be passed to this planner by calling {@link #copyJobConf(java.util.Map, org.apache.hadoop.mapred.JobConf)}
 * on a map properties object before constructing a new {@link cascading.flow.hadoop2.Hadoop2MR1FlowConnector}.
 * <p/>
 * A better practice would be to set Hadoop properties directly on the map properties object handed to the FlowConnector.
 * All values in the map will be passed to a new default JobConf instance to be used as defaults for all resulting
 * Flow instances.
 * <p/>
 * For example, {@code properties.set("mapred.child.java.opts","-Xmx512m");} would convince Hadoop
 * to spawn all child jvms with a heap of 512MB.
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
