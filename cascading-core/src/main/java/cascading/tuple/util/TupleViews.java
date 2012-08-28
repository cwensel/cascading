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
