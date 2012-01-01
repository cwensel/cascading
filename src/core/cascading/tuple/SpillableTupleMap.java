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

package cascading.tuple;

import java.util.Collection;
import java.util.HashMap;

import cascading.flow.FlowProcess;

/**
 *
 */
public abstract class SpillableTupleMap extends HashMap<Tuple, Collection<Tuple>> implements Spillable
  {
  private final int threshold;
  private final FlowProcess flowProcess;
  private Listener listener;

  public SpillableTupleMap( int initialCapacity, int threshold, FlowProcess flowProcess )
    {
    super( initialCapacity, 0.75f );
    this.threshold = threshold;
    this.flowProcess = flowProcess;
    }

  public SpillableTupleMap( int initialCapacity, int threshold, float loadFactor, FlowProcess flowProcess )
    {
    super( initialCapacity, loadFactor );
    this.threshold = threshold;
    this.flowProcess = flowProcess;
    }

  protected int getThreshold()
    {
    return threshold;
    }

  protected FlowProcess getFlowProcess()
    {
    return flowProcess;
    }

  @Override
  public Collection<Tuple> get( Object object )
    {
    Collection<Tuple> value = super.get( object );

    if( value == null )
      {
      value = createSpillableTupleList();

      if( value instanceof Spillable )
        ( (Spillable) value ).setListener( listener );

      super.put( (Tuple) object, value );

      if( listener != null )
        listener.notify( this );
      }

    return value;
    }

  protected abstract SpillableTupleList createSpillableTupleList();

  @Override
  public void setListener( Listener listener )
    {
    this.listener = listener;
    }
  }
