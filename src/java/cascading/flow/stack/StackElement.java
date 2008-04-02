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

import cascading.flow.FlowCollector;

/** Class StackElement is the base class for Map and Reduce operation stacks. */
public abstract class StackElement implements FlowCollector
  {
  StackElement previous;
  StackElement next;

  public StackElement resolveStack()
    {
    if( previous != null )
      return previous.setNext( this );

    return this;
    }

  private StackElement setNext( StackElement next )
    {
    this.next = next;

    if( previous != null )
      return previous.setNext( this );

    return this;
    }
  }
