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

package cascading.tuple.collect;

import cascading.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public interface Spillable
  {
  interface SpillStrategy
    {
    boolean doSpill( Spillable spillable, int size );

    String getSpillReason( Spillable spillable );
    }

  interface SpillListener
    {
    SpillListener NULL = new SpillListener()
    {
    private final Logger LOG = LoggerFactory.getLogger( SpillListener.class );

    @Override
    public void notifyWriteSpillBegin( Spillable spillable, int spillSize, String spillReason )
      {
      LOG.info( "spilling {} tuples in list to spill number {}", spillSize, spillable.spillCount() + 1 );
      }

    @Override
    public void notifyReadSpillBegin( Spillable spillable )
      {
      }

    @Override
    public void notifyWriteSpillEnd( SpillableTupleList spillableTupleList, long duration )
      {
      }
    };

    void notifyWriteSpillBegin( Spillable spillable, int spillSize, String spillReason );

    void notifyWriteSpillEnd( SpillableTupleList spillableTupleList, long duration );

    void notifyReadSpillBegin( Spillable spillable );
    }

  void setGrouping( Tuple group );

  Tuple getGrouping();

  void setSpillStrategy( SpillStrategy spillStrategy );

  void setSpillListener( SpillListener spillListener );

  /**
   * The number of times this container has spilled data to disk.
   *
   * @return in int
   */
  int spillCount();
  }