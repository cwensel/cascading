/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
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
