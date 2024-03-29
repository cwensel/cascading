/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tap.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 */
public class DistCacheTap extends BaseDistCacheTap
  {
  public static final String CASCADING_LOCAL_RESOURCES = "cascading.resources.local.";
  public static final String CASCADING_REMOTE_RESOURCES = "cascading.resources.remote.";

  public DistCacheTap( Tap<Configuration, RecordReader, OutputCollector> original )
    {
    super( original );
    }

  @Override
  protected Path[] getLocalCacheFiles( FlowProcess<? extends Configuration> flowProcess ) throws IOException
    {
    String key = CASCADING_REMOTE_RESOURCES + Tap.id( this );
    String property = flowProcess.getStringProperty( key );

    if( property == null )
      throw new TapException( "unable to find local resources property for: " + key );

    String[] split = property.split( "," );
    Path[] paths = new Path[ split.length ];

    for( int i = 0; i < split.length; i++ )
      paths[ i ] = new Path( split[ i ] );

    return paths;
    }

  @Override
  protected void addLocalCacheFiles( Configuration conf, URI uri )
    {
    String key = CASCADING_LOCAL_RESOURCES + Tap.id( this );
    Collection<String> resources = conf.getStringCollection( key );

    if( resources == null )
      resources = new ArrayList<>();

    resources.add( uri.toString() );

    conf.setStrings( key, resources.toArray( new String[ resources.size() ] ) );
    }
  }
