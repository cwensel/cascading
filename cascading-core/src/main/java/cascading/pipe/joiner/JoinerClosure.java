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

package cascading.pipe.joiner;

import java.util.Arrays;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
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

  public Fields[] getJoinFields()
    {
    return joinFields;
    }

  public Fields[] getValueFields()
    {
    return valueFields;
    }

  public abstract int size();

  public abstract Iterator<Tuple> getIterator( int pos );

  public abstract boolean isEmpty( int pos );

  public abstract Tuple getGroupTuple( Tuple keysTuple );
  }
