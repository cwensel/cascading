/*
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

package cascading.pipe.joiner;

import java.util.Arrays;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class JoinerClosure wraps all incoming tuple streams with iterator instances allowing for just join implementations.
 * <p/>
 * This class is provided to a {@link Joiner#getIterator(JoinerClosure)} implementation, or to a {@link cascading.operation.Buffer}
 * via the {@link cascading.operation.BufferCall#getJoinerClosure()} method.
 * <p/>
 * All iterators returned by {@link #getIterator(int)} may be retrieved more than once to restart them except for the left
 * most iterator at position {@code 0} (zero).
 * <p/>
 * This iterator may only be iterated across once. All other iterators are backed by memory and possibly disk.
 */
public abstract class JoinerClosure
  {
  protected final FlowProcess flowProcess;

  protected final Fields[] joinFields;
  protected final Fields[] valueFields;

  public JoinerClosure( FlowProcess flowProcess, Fields[] joinFields, Fields[] valueFields )
    {
    this.flowProcess = flowProcess;
    this.joinFields = Arrays.copyOf( joinFields, joinFields.length );
    this.valueFields = Arrays.copyOf( valueFields, valueFields.length );
    }

  public FlowProcess getFlowProcess()
    {
    return flowProcess;
    }

  /**
   * Returns an array of {@link Fields} denoting the join fields or keys uses for each incoming pipe.
   * <p/>
   * The most left handed pipe will be in array position 0.
   *
   * @return an array of Fields
   */
  public Fields[] getJoinFields()
    {
    return joinFields;
    }

  /**
   * Returns an array of all the incoming fields for each incoming pipe.
   * <p/>
   * The most left handed pipe will be in array position 0;
   *
   * @return an array of Fields
   */
  public Fields[] getValueFields()
    {
    return valueFields;
    }

  public boolean isSelfJoin()
    {
    return valueFields.length == 1 && size() != valueFields.length;
    }

  public abstract int size();

  /**
   * Returns a Tuple Iterator for the given pipe position. Position 0 is the most left handed pipe passed to the prior
   * {@link cascading.pipe.CoGroup}.
   * <p/>
   * To restart an Iterator over a given pipe, this method must be called again.
   *
   * @param pos of type int
   * @return an Iterator of Tuple instances.
   */
  public abstract Iterator<Tuple> getIterator( int pos );

  public abstract boolean isEmpty( int pos );

  public abstract Tuple getGroupTuple( Tuple keysTuple );
  }
