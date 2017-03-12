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

package cascading.tuple.hadoop.util;

import java.util.Comparator;
import java.util.List;

import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.util.TupleHasher;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Super class of all Hadoop partitioners.
 * <p/>
 * As of Cascading 2.7 the hashing used to calculate partitions has been changed to use Murmur3. Users that rely on the
 * old behaviour should set {@link cascading.tuple.hadoop.util.HasherPartitioner#HASHER_PARTITIONER_USE_LEGACY_HASH} to
 * {@code true}.
 */
public class HasherPartitioner extends TupleHasher implements Configurable
  {
  public final static String HASHER_PARTITIONER_USE_LEGACY_HASH = "cascading.tuple.hadoop.util.hasherpartitioner.uselegacyhash";

  private static Comparator defaultComparator;

  private Comparator[] comparators;
  private Configuration conf;

  @Override
  public void setConf( Configuration conf )
    {
    if( this.conf != null )
      return;

    this.conf = conf;

    defaultComparator = TupleSerialization.getDefaultComparator( defaultComparator, conf );

    comparators = DeserializerComparator.getFieldComparatorsFrom( conf, "cascading.group.comparator" );

    if( conf.getBoolean( HASHER_PARTITIONER_USE_LEGACY_HASH, false ) )
      this.hashFunction = new LegacyHashFunction();

    initialize( defaultComparator, comparators );
    }

  @Override
  public Configuration getConf()
    {
    return conf;
    }

  static class LegacyHashFunction extends TupleHasher.HashFunction
    {
    @Override
    public int hash( Tuple tuple, Hasher[] hashers )
      {
      int hash = 1;
      List<Object> elements = Tuple.elements( tuple );
      for( int i = 0; i < elements.size(); i++ )
        {
        Object element = elements.get( i );
        hash = 31 * hash + ( element != null ? hashers[ i % hashers.length ].hashCode( element ) : 0 );
        }
      return hash;
      }
    }
  }
