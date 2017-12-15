/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple.collect;

import java.util.Collection;
import java.util.HashMap;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;

import static cascading.tuple.collect.SpillableProps.defaultMapInitialCapacity;
import static cascading.tuple.collect.SpillableProps.defaultMapLoadFactor;

/**
 * SpillableTupleMap is a HashMap that will allow for multiple values per key, and if the number of values for a given
 * key reach a specific threshold, they will be spilled to disk using a {@link SpillableTupleList} instance. Only
 * values are spilled, keys are not spilled and too many keys can result in a {@link OutOfMemoryError}.
 * <p>
 * The {@link cascading.tuple.collect.SpillableProps#MAP_THRESHOLD} value sets the number of total values that this map will
 * strive to keep in memory regardless of the number of keys. This is achieved by dynamically calculating the threshold
 * used by each child SpillableTupleList instance using
 * {@code threshold = Min( list_threshold, map_threshold / current_num_keys ) }.
 * <p>
 * To set the list threshold, see {@link cascading.tuple.collect.SpillableProps} fluent helper class.
 * <p>
 * This class is used by the {@link cascading.pipe.HashJoin} pipe, to set properties specific to a given
 * join instance, see the {@link cascading.pipe.HashJoin#getConfigDef()} method.
 *
 * @see cascading.tuple.hadoop.collect.HadoopSpillableTupleMap
 */
public abstract class SpillableTupleMap extends HashMap<Tuple, Collection<Tuple>> implements Spillable
  {
  private int mapThreshold;
  private int initListThreshold;
  private Spillable.SpillListener spillListener = Spillable.SpillListener.NULL;

  public static int getMapThreshold( FlowProcess flowProcess, int defaultValue )
    {
    String value = (String) flowProcess.getProperty( SpillableProps.MAP_THRESHOLD );

    if( value == null || value.length() == 0 )
      return defaultValue;

    return Integer.parseInt( value );
    }

  public static int getMapCapacity( FlowProcess flowProcess, int defaultValue )
    {
    String value = (String) flowProcess.getProperty( SpillableProps.MAP_CAPACITY );

    if( value == null || value.length() == 0 )
      return defaultValue;

    return Integer.parseInt( value );
    }

  public static float getMapLoadFactor( FlowProcess flowProcess, float defaultValue )
    {
    String value = (String) flowProcess.getProperty( SpillableProps.MAP_LOADFACTOR );

    if( value == null || value.length() == 0 )
      return defaultValue;

    return Float.parseFloat( value );
    }

  public SpillableTupleMap( int mapThreshold, int initListThreshold )
    {
    super( defaultMapInitialCapacity, defaultMapLoadFactor );
    this.mapThreshold = mapThreshold;
    this.initListThreshold = initListThreshold;
    }

  public SpillableTupleMap( int initialCapacity, float loadFactor, int mapThreshold, int initListThreshold )
    {
    super( initialCapacity, loadFactor );
    this.mapThreshold = mapThreshold;
    this.initListThreshold = initListThreshold;
    }

  protected int getMapThreshold()
    {
    return mapThreshold;
    }

  public int getInitListThreshold()
    {
    return initListThreshold;
    }

  @Override
  public Collection<Tuple> get( Object object )
    {
    Collection<Tuple> value = super.get( object );

    if( value == null )
      {
      value = createTupleCollection( (Tuple) object );

      super.put( (Tuple) object, value );
      }

    return value;
    }

  protected abstract Collection<Tuple> createTupleCollection( Tuple object );

  @Override
  public void setGrouping( Tuple group )
    {
    }

  @Override
  public Tuple getGrouping()
    {
    return null;
    }

  @Override
  public void setSpillStrategy( SpillStrategy spillStrategy )
    {
    }

  @Override
  public int spillCount()
    {
    return 0;
    }

  public Spillable.SpillListener getSpillListener()
    {
    return spillListener;
    }

  public void setSpillListener( Spillable.SpillListener spillListener )
    {
    this.spillListener = spillListener;
    }
  }
