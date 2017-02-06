/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple.util;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;

/** This class is experimental and for internal use only. */
public class TupleViews
  {
  public static Tuple createComposite( Tuple... tuples )
    {
    return Tuples.create( new CompositeTupleList( tuples ) );
    }

  public static Tuple createComposite( Fields... fields )
    {
    return Tuples.create( new CompositeTupleList( fields ) );
    }

  public static Tuple createComposite( Fields[] fields, Tuple[] tuples )
    {
    return Tuples.create( new CompositeTupleList( fields, tuples ) );
    }

  public static Tuple createNarrow( int[] basePos )
    {
    return Tuples.create( new NarrowTupleList( basePos ) );
    }

  public static Tuple createNarrow( int[] basePos, Tuple tuple )
    {
    return Tuples.create( new NarrowTupleList( basePos, tuple ) );
    }

  public static Tuple createOverride( Fields base, Fields override )
    {
    return Tuples.create( new OverrideTupleList( base, override ) );
    }

  public static Tuple createOverride( Fields base, Tuple baseTuple, Fields override, Tuple overrideTuple )
    {
    return Tuples.create( new OverrideTupleList( base, baseTuple, override, overrideTuple ) );
    }

  public static Tuple createOverride( int[] basePos, Tuple baseTuple, int[] overridePos, Tuple overrideTuple )
    {
    return Tuples.create( new OverrideTupleList( basePos, baseTuple, overridePos, overrideTuple ) );
    }

  public static Tuple createObjectArray()
    {
    return Tuples.create( new ObjectArrayList() );
    }

  public static Tuple createObjectArray( Object... values )
    {
    return Tuples.create( new ObjectArrayList( values ) );
    }

  public static <V> Tuple reset( Tuple tuple, V... values )
    {
    ( (Resettable<V>) Tuple.elements( tuple ) ).reset( values );

    return tuple;
    }
  }
