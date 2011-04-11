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

package cascading.scheme;

import cascading.tuple.TupleEntry;

/**
 *
 */
public class SourceCall<Context, Input> extends SchemeCall<Context>
  {
  TupleEntry incomingEntry;
  Input input;

  public SourceCall()
    {
    }

  public SourceCall( TupleEntry incomingEntry, Input input )
    {
    this.incomingEntry = incomingEntry;
    this.input = input;
    }

  public TupleEntry getIncomingEntry()
    {
    return incomingEntry;
    }

  public void setIncomingEntry( TupleEntry incomingEntry )
    {
    this.incomingEntry = incomingEntry;
    }

  public Input getInput()
    {
    return input;
    }

  public void setInput( Input input )
    {
    this.input = input;
    }
  }
