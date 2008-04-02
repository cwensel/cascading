/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import cascading.pipe.Group;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class GroupMapperStackElement extends MapperStackElement
  {
  private final Group group;
  private final Scope outgoingScope;

  public GroupMapperStackElement( MapperStackElement previous, Scope incomingScope, Group group, Scope outgoingScope )
    {
    super( previous, incomingScope );
    this.group = group;
    this.outgoingScope = outgoingScope;
    }

  protected FlowElement getFlowElement()
    {
    return group;
    }

  @Override
  public void collect( Tuple tuple )
    {
    super.collect( tuple );

    operateGroup( getTupleEntry( tuple ) );
    }

  private void operateGroup( TupleEntry tupleEntry )
    {
    try
      {
      group.makeReduceGrouping( incomingScope, outgoingScope, tupleEntry, lastOutput );
      }
    catch( IOException exception )
      {
      throw new FlowException( "failed writing output", exception );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error", throwable );
      }
    }
  }
