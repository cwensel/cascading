/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stack;

import java.util.Iterator;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowSession;
import cascading.flow.Scope;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
class GroupReducerStackElement extends ReducerStackElement
  {
  private final Group group;
  private final Set<Scope> incomingScopes;
  private final Scope thisScope;

  public GroupReducerStackElement( FlowSession flowSession, Set<Scope> incomingScopes, Group group, Scope thisScope, Fields outGroupingFields, Tap trap )
    {
    super( trap, outGroupingFields, flowSession );
    this.group = group;
    this.incomingScopes = incomingScopes;
    this.thisScope = thisScope;
    }

  public FlowElement getFlowElement()
    {
    return group;
    }

  public void collect( Tuple key, Iterator values )
    {
    operateGroup( key, values );
    }

  private void operateGroup( Tuple key, Iterator values )
    {
    key = group.unwrapGrouping( key );

    // if a cogroup group instance...
    // an ungrouping iterator to partition the values back into a tuple so reduce stack can run
    // this can be one big tuple. the values iterator will have one Tuple of the format:
    // [ [key] [group1] [group2] ] where [groupX] == [ [...] [...] ...], a cogroup for each source
    // this can be nasty
    values = group.iterateReduceValues( getFlowSession(), incomingScopes, thisScope, key, values );

    values = new TupleEntryIterator( ( (ReducerStackElement) next ).resolveIncomingOperationFields(), values );

    next.collect( key, values );
    }
  }
