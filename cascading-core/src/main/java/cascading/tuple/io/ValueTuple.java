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

package cascading.tuple.io;

import java.util.List;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.tuple.util.Resettable1;

/**
 *
 */
public class ValueTuple extends Tuple implements Resettable1<Tuple>
  {
  /** A constant empty Tuple instance. This instance is immutable. */
  public static final ValueTuple NULL = Tuples.asUnmodifiable( new ValueTuple() );

  public ValueTuple( List<Object> elements )
    {
    super( elements );
    }

  public ValueTuple( Fields fields, List<Object> elements )
    {
    super( fields, elements );
    }

  public ValueTuple()
    {
    }

  public ValueTuple( Tuple tuple )
    {
    super( tuple );
    }

  public ValueTuple( Object... values )
    {
    super( values );
    }

  @Override
  public void reset( Tuple value )
    {
    elements = Tuple.elements( value );
    }
  }
