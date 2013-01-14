/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Class TupleEntryIterator provides an efficient Iterator for returning {@link TupleEntry} elements in an
 * underlying {@link Tuple} collection.
 */
public abstract class TupleEntryIterator implements Iterator<TupleEntry>, Closeable
  {
  /** Field entry */
  final TupleEntry entry;

  /**
   * Constructor TupleEntryIterator creates a new TupleEntryIterator instance.
   *
   * @param fields of type Fields
   */
  public TupleEntryIterator( Fields fields )
    {
    this.entry = new TupleEntry( fields, Tuple.size( fields.size() ) );
    }

  /**
   * Method getFields returns the fields of this TupleEntryIterator object.
   *
   * @return the fields (type Fields) of this TupleEntryIterator object.
   */
  public Fields getFields()
    {
    return entry.getFields();
    }

  /**
   * Method getTupleEntry returns the entry of this TupleEntryIterator object.
   * <p/>
   * Since {@link TupleEntry} and the underlying {@link Tuple} instances are re-used, you must make a copy if you wish
   * to store the instances in a Collection for later use.
   *
   * @return the entry (type TupleEntry) of this TupleEntryIterator object.
   */
  public TupleEntry getTupleEntry()
    {
    return entry;
    }
  }
