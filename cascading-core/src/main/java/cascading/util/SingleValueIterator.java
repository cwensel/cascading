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

package cascading.util;

import java.util.Iterator;

/**
 * Class SingleValueIterator is a convenience to creating an {@link Iterator} that returns one value for use
 * with interfaces that only accept Iterator instances.
 * <p/>
 * If given a {@code null} value, it will be assumed this Iterator instance has no value, and {@link #hasNext()}
 * will return false.
 */
public class SingleValueIterator<Value> implements Iterator<Value>
  {
  public static final SingleValueIterator EMPTY = new SingleValueIterator();

  private boolean hasValue = true;
  protected Value value;

  public SingleValueIterator()
    {
    this.hasValue = false;
    }

  public SingleValueIterator( Value value )
    {
    this.hasValue = value != null;
    this.value = value;
    }

  @Override
  public boolean hasNext()
    {
    return hasValue;
    }

  @Override
  public Value next()
    {
    if( !hasValue )
      throw new IllegalStateException( "no value available" );

    try
      {
      return value;
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

  public void reset( Value value )
    {
    this.hasValue = value != null;
    this.value = value;
    }
  }
