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

package cascading.util;

import java.io.IOException;

import cascading.tuple.CloseableIterator;

/**
 *
 */
public abstract class SingleValueIterator<Input> implements CloseableIterator<Input>
  {
  private boolean hasValue = true;
  private final Input input;

  public SingleValueIterator( Input input )
    {
    this.input = input;
    }

  @Override
  public boolean hasNext()
    {
    return hasValue;
    }

  @Override
  public Input next()
    {
    if( !hasValue )
      throw new IllegalStateException( "no value available" );

    try
      {
      return input;
      }
    finally
      {
      hasValue = false;
      }
    }

  @Override
  public void remove()
    {
    throw new UnsupportedOperationException( "unimplimented" );
    }

  protected Input getCloseableInput()
    {
    return input;
    }

  @Override
  public abstract void close() throws IOException;
  }
