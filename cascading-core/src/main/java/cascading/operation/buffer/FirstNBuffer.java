/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.buffer;

import java.beans.ConstructorProperties;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class FirstNBuffer will return the first N tuples seen in a given grouping. After the tuples
 * are returned the Buffer stops iterating the arguments unlike the {@link cascading.operation.aggregator.First}
 * {@link cascading.operation.Aggregator} which by contract sees all the values in the grouping.
 * <p/>
 * By default it returns one Tuple.
 * <p/>
 * Order can be controlled through the prior {@link cascading.pipe.GroupBy} or {@link cascading.pipe.CoGroup}
 * pipes.
 * <p/>
 * This class is used by {@link cascading.pipe.assembly.Unique}.
 */
public class FirstNBuffer extends BaseOperation implements Buffer
  {
  private final int firstN;

  /** Selects and returns the first argument Tuple encountered. */
  public FirstNBuffer()
    {
    super( Fields.ARGS );

    firstN = 1;
    }

  /**
   * Selects and returns the first N argument Tuples encountered.
   *
   * @param firstN of type int
   */
  @ConstructorProperties({"firstN"})
  public FirstNBuffer( int firstN )
    {
    super( Fields.ARGS );

    this.firstN = firstN;
    }

  /**
   * Selects and returns the first argument Tuple encountered.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public FirstNBuffer( Fields fieldDeclaration )
    {
    super( fieldDeclaration.size(), fieldDeclaration );

    this.firstN = 1;
    }

  /**
   * Selects and returns the first N argument Tuples encountered.
   *
   * @param fieldDeclaration of type Fields
   * @param firstN           of type int
   */
  @ConstructorProperties({"fieldDeclaration", "firstN"})
  public FirstNBuffer( Fields fieldDeclaration, int firstN )
    {
    super( fieldDeclaration.size(), fieldDeclaration );

    this.firstN = firstN;
    }

  public int getFirstN()
    {
    return firstN;
    }

  @Override
  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
    {
    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

    int count = 0;

    while( count < firstN && iterator.hasNext() )
      {
      bufferCall.getOutputCollector().add( iterator.next() );
      count++;
      }
    }
  }
