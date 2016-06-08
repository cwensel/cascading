/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.util.CloseableIterator;

/**
 *
 */
public class TimedIterator<V> implements CloseableIterator<V>
  {
  private final FlowProcess flowProcess;
  private final Enum durationCounter;
  private final Enum countCounter;
  private final int ordinal;

  public static <V> TimedIterator<V>[] iterators( TimedIterator<V>... iterators )
    {
    return iterators;
    }

  Iterator<V> iterator;

  public TimedIterator( FlowProcess flowProcess, Enum durationCounter, Enum countCounter )
    {
    this( flowProcess, durationCounter, countCounter, 0 );
    }

  public TimedIterator( FlowProcess flowProcess, Enum durationCounter, Enum countCounter, int ordinal )
    {
    this.flowProcess = flowProcess;
    this.durationCounter = durationCounter;
    this.countCounter = countCounter;
    this.ordinal = ordinal;
    }

  public void reset( Iterable<V> iterable )
    {
    if( iterable == null )
      this.iterator = null;
    else
      this.iterator = iterable.iterator();
    }

  public void reset( Iterator<V> iterator )
    {
    this.iterator = iterator;
    }

  @Override
  public boolean hasNext()
    {
    if( iterator == null )
      return false;

    long start = System.currentTimeMillis();

    try
      {
      return iterator.hasNext();
      }
    finally
      {
      flowProcess.increment( durationCounter, System.currentTimeMillis() - start );
      }
    }

  @Override
  public V next()
    {
    long start = System.currentTimeMillis();

    try
      {
      flowProcess.increment( countCounter, 1 );

      return iterator.next();
      }
    finally
      {
      flowProcess.increment( durationCounter, System.currentTimeMillis() - start );
      }
    }

  @Override
  public void remove()
    {
    iterator.remove();
    }

  @Override
  public void close() throws IOException
    {
    if( iterator instanceof Closeable )
      ( (Closeable) iterator ).close();
    }
  }
