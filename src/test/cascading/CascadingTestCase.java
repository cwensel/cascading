/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import java.util.regex.Pattern;

import cascading.flow.Flow;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
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
    validateLength( flow, length, -1 );
    }

  protected void validateLength( Flow flow, int length, String name ) throws IOException
    {
    validateLength( flow, length, -1, null, name );
    }

  protected void validateLength( Flow flow, int length, int size ) throws IOException
    {
    validateLength( flow, length, size, null, null );
    }

  protected void validateLength( Flow flow, int length, int size, Pattern regex ) throws IOException
    {
    validateLength( flow, length, size, regex, null );
    }

  protected void validateLength( Flow flow, int length, Pattern regex, String name ) throws IOException
    {
    validateLength( flow, length, -1, regex, name );
    }

  protected void validateLength( Flow flow, int length, int size, Pattern regex, String name ) throws IOException
    {
    TupleEntryIterator iterator = name == null ? flow.openSink() : flow.openSink( name );
    validateLength( iterator, length, size, regex );
    }

  protected void validateLength( TupleEntryIterator iterator, int length )
    {
    validateLength( iterator, length, -1, null );
    }

  protected void validateLength( TupleEntryIterator iterator, int length, int size )
    {
    validateLength( iterator, length, size, null );
    }

  protected void validateLength( TupleEntryIterator iterator, int length, Pattern regex )
    {
    validateLength( iterator, length, -1, regex );
    }

  protected void validateLength( TupleEntryIterator iterator, int length, int size, Pattern regex )
    {
    int count = 0;
    while( iterator.hasNext() )
      {
      TupleEntry tuple = iterator.next();

      if( size != -1 )
        assertEquals( "wrong number of elements", size, tuple.size() );

      if( regex != null )
        assertTrue( "regex: " + regex + " does not match: " + tuple.getTuple().toString(), regex.matcher( tuple.getTuple().toString() ).matches() );

      count++;
      }

    iterator.close();

    assertEquals( "wrong number of lines", length, count );
    }
  }
