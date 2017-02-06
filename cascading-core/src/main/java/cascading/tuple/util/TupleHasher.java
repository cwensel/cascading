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

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import cascading.tuple.Fields;
import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import cascading.util.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TupleHasher implements Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( TupleHasher.class );

  private static HashFunction DEFAULT_HASH_FUNCTION = new HashFunction();
  private static Hasher DEFAULT_HASHER = new ObjectHasher();

  protected HashFunction hashFunction = DEFAULT_HASH_FUNCTION;
  private Hasher[] hashers;

  public TupleHasher()
    {
    }

  public TupleHasher( Comparator defaultComparator, Comparator[] comparators )
    {
    initialize( defaultComparator, comparators );
    }

  public static Comparator[] merge( Fields[] keyFields )
    {
    Comparator[] comparators = new Comparator[ keyFields[ 0 ].size() ];

    for( Fields keyField : keyFields )
      {
      if( keyField == null )
        continue;

      for( int i = 0; i < keyField.getComparators().length; i++ )
        {
        Comparator comparator = keyField.getComparators()[ i ];

        if( !( comparator instanceof Hasher ) )
          continue;

        if( comparators[ i ] != null && !comparators[ i ].equals( comparator ) )
          LOG.warn( "two unequal Hasher instances for the same key field position found: {}, and: {}", comparators[ i ], comparator );

        comparators[ i ] = comparator;
        }
      }

    return comparators;
    }

  public static boolean isNull( Comparator[] comparators )
    {
    int count = 0;

    for( Comparator comparator : comparators )
      {
      if( comparator == null )
        count++;
      }

    if( count == comparators.length )
      return true;

    return false;
    }

  protected void initialize( Comparator defaultComparator, Comparator[] comparators )
    {
    Hasher defaultHasher = DEFAULT_HASHER;

    if( defaultComparator instanceof Hasher )
      defaultHasher = (Hasher) defaultComparator;

    hashers = new Hasher[ comparators.length ];

    for( int i = 0; i < comparators.length; i++ )
      {
      Comparator comparator = comparators[ i ];

      if( comparator instanceof Hasher )
        hashers[ i ] = (Hasher) comparator;
      else
        hashers[ i ] = defaultHasher;
      }
    }

  public final int hashCode( Tuple tuple )
    {
    return getHashFunction().hash( tuple, hashers );
    }

  protected HashFunction getHashFunction()
    {
    return hashFunction;
    }

  private static class ObjectHasher implements Hasher<Object>, Serializable
    {
    @Override
    public int hashCode( Object value )
      {
      if( value == null )
        return 0;

      return value.hashCode();
      }
    }

  static class WrappedTuple extends Tuple
    {
    private final TupleHasher tupleHasher;

    public WrappedTuple( TupleHasher tupleHasher, Tuple input )
      {
      super( Tuple.elements( input ) );
      this.tupleHasher = tupleHasher;
      }

    @Override
    public int hashCode()
      {
      return tupleHasher.hashCode( this );
      }
    }

  /**
   * Wraps the given Tuple in a subtype, that uses the provided Hasher for hashCode calculations. If the given Hasher
   * is {@code null} the input Tuple will be returned.
   *
   * @param tupleHasher A TupleHasher instance.
   * @param input       A Tuple instance.
   * @return A tuple using the provided TupleHasher for hashCode calculations if the TupleHasher is not null.
   */
  public static Tuple wrapTuple( TupleHasher tupleHasher, Tuple input )
    {
    if( tupleHasher == null )
      return input;

    return new WrappedTuple( tupleHasher, input );
    }

  public static class HashFunction implements Serializable
    {
    public int hash( Tuple tuple, Hasher[] hashers )
      {
      int hash = Murmur3.SEED;

      List<Object> elements = Tuple.elements( tuple );

      for( int i = 0; i < elements.size(); i++ )
        {
        Object element = elements.get( i );

        int hashCode = hashers[ i % hashers.length ].hashCode( element );

        hash = Murmur3.mixH1( hash, Murmur3.mixK1( hashCode ) );
        }

      return Murmur3.fmix( hash, elements.size() );
      }
    }
  }
