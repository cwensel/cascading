/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
 * SingleValueIterator is a utility class used for quickly presenting a single value to a consumer
 * expecting both a {@link java.io.Closeable} and an {@link java.util.Iterator} interface. After returning the Value
 * value via {@link #next}, {@link #hasNext()} will return {@code false}.
 * <p/>
 * This class is especially useful if you do not wish to create a {@link java.util.Collection} to hold a single value
 * in which to create an Iterator.
 *
 * @param <Value>
 */
public abstract class SingleValueCloseableIterator<Value> extends SingleValueIterator<Value> implements CloseableIterator<Value>
  {

  public SingleValueCloseableIterator( Value value )
    {
    super( value );
    }

  protected Value getCloseableInput()
    {
    return value;
    }

  @Override
  public abstract void close() throws IOException;
  }
