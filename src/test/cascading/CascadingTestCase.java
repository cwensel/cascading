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

package cascading;

import java.io.IOException;

import cascading.flow.Flow;
import cascading.tuple.TupleIterator;
import junit.framework.TestCase;

/**
 *
 */
public class CascadingTestCase extends TestCase
  {
  public CascadingTestCase()
    {
    }

  public CascadingTestCase( String string )
    {
    super( string );
    }

  protected void validateLength( Flow flow, int length ) throws IOException
    {
    validateLength( flow, length, null );
    }

  protected void validateLength( Flow flow, int length, String name ) throws IOException
    {
    TupleIterator iterator = name == null ? flow.openSink() : flow.openSink( name );
    validateLength( iterator, length );
    }

  protected void validateLength( TupleIterator iterator, int length )
    {
    int count = 0;
    while( iterator.hasNext() )
      {
      iterator.next();
      count++;
      }

    iterator.close();

    assertEquals( "wrong number of items", length, count );
    }
  }
