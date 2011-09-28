/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.cogroup;

import java.util.Arrays;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public abstract class GroupClosure
  {
  protected final FlowProcess flowProcess;

  protected final Fields[] groupingFields;
  protected final Fields[] valueFields;
  protected Tuple grouping;

  public GroupClosure( FlowProcess flowProcess, Fields[] groupingFields, Fields[] valueFields )
    {
    this.flowProcess = flowProcess;
    this.groupingFields = Arrays.copyOf( groupingFields, groupingFields.length );
    this.valueFields = Arrays.copyOf( valueFields, valueFields.length );
    }

  public FlowProcess getFlowProcess()
    {
    return flowProcess;
    }

  public Fields[] getGroupingFields()
    {
    return groupingFields;
    }

  public Fields[] getValueFields()
    {
    return valueFields;
    }

  public abstract int size();

  public Tuple getGrouping()
    {
    return grouping;
    }

  public abstract Iterator getIterator( int pos );

  public abstract boolean isEmpty( int pos );
  }
