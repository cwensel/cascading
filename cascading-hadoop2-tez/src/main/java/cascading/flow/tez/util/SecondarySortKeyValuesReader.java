/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.tez.util;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import cascading.CascadingException;
import cascading.tuple.Tuple;
import cascading.tuple.io.TuplePair;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 *
 */
public class SecondarySortKeyValuesReader extends KeyValuesReader
  {
  private KeyValuesReader parent;
  private Comparator<Tuple> groupComparator;
  private Tuple currentKey;
  private Iterable<Object> currentValues;
  private boolean isNewKey = false;
  private TuplePair currentKeyPair;

  public SecondarySortKeyValuesReader( KeyValuesReader parent, Comparator<Tuple> groupComparator )
    {
    this.parent = parent;
    this.groupComparator = groupComparator;
    }

  @Override
  public boolean next() throws IOException
    {
    if( parent != null && isNewKey )
      {
      isNewKey = false; // allow next next() to advance underlying iterator
      return true;
      }

    return advance();
    }

  protected boolean advance() throws IOException
    {
    if( parent == null )
      return false;

    boolean next = parent.next();

    if( !next )
      {
      parent = null;
      return false;
      }

    currentKeyPair = (TuplePair) parent.getCurrentKey();

    isNewKey = currentKey == null || groupComparator.compare( currentKey, currentKeyPair.getLhs() ) != 0;
    currentKey = currentKeyPair.getLhs();
    currentValues = parent.getCurrentValues();

    return true;
    }

  @Override
  public Object getCurrentKey() throws IOException
    {
    return currentKeyPair;
    }

  @Override
  public Iterable<Object> getCurrentValues() throws IOException
    {
    return new Iterable<Object>()
    {
    @Override
    public Iterator<Object> iterator()
      {
      final Iterator<Object>[] iterator = new Iterator[]{currentValues.iterator()};

      return new Iterator<Object>()
      {
      @Override
      public boolean hasNext()
        {
        boolean hasNext = iterator[ 0 ].hasNext();

        if( hasNext )
          return true;

        if( !advanceSafe() )
          return false;

        if( isNewKey )
          return false;

        iterator[ 0 ] = currentValues.iterator();

        return hasNext();
        }

      @Override
      public Object next()
        {
        return iterator[ 0 ].next();
        }

      @Override
      public void remove()
        {
        iterator[ 0 ].remove();
        }

      protected boolean advanceSafe()
        {
        try
          {
          return advance();
          }
        catch( IOException exception )
          {
          throw new CascadingException( "unable to advance values iterator", exception );
          }
        }
      };
      }
    };
    }
  }
