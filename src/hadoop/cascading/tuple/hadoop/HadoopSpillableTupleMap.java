/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import cascading.tuple.Spillable;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.SpillableTupleMap;
import cascading.tuple.Tuple;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;

/**
 * HadoopSpillableTupleMap is responsible for spilling values to disk if the map threshold is reached.
 *
 * @see SpillableTupleMap
 * @see SpillableTupleList
 */
public class HadoopSpillableTupleMap extends SpillableTupleMap
  {
  private CompressionCodec codec;
  private final JobConf jobConf;

  public HadoopSpillableTupleMap( int initialCapacity, float loadFactor, int mapThreshold, int listThreshold, CompressionCodec codec, JobConf jobConf )
    {
    super( initialCapacity, loadFactor, mapThreshold, listThreshold );
    this.jobConf = jobConf;
    this.codec = codec;
    }

  public HadoopSpillableTupleMap( int mapThreshold, int listThreshold, CompressionCodec codec, JobConf jobConf )
    {
    super( mapThreshold, listThreshold );
    this.codec = codec;
    this.jobConf = jobConf;
    }

  @Override
  protected SpillableTupleList createTupleCollection( Tuple object )
    {
    HadoopSpillableTupleList tupleList = new HadoopSpillableTupleList( getThreshold(), codec, jobConf );

    tupleList.setSpillListener( getSpillableListener( object ) );

    return tupleList;
    }

  protected Spillable.SpillListener getSpillableListener( Tuple tuple )
    {
    return Spillable.SpillListener.NULL;
    }

  private SpillableTupleList.Threshold getThreshold()
    {
    return new SpillableTupleList.Threshold()
    {
    @Override
    public int current()
      {
      return Math.min( getInitListThreshold(), getMapThreshold() / size() );
      }
    };
    }
  }
