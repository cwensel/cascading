/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
