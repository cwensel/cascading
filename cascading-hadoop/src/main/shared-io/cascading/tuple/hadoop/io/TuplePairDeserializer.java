/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop.io;

import java.io.IOException;

import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TuplePair;

public class TuplePairDeserializer extends BaseDeserializer<TuplePair>
  {
  private final TupleInputStream.TupleElementReader[] keyReaders;
  private final TupleInputStream.TupleElementReader[] sortReaders;

  public TuplePairDeserializer( TupleSerialization.SerializationElementReader elementReader )
    {
    super( elementReader );

    Class[] keyClasses = elementReader.getTupleSerialization().getKeyTypes();
    Class[] sortClasses = elementReader.getTupleSerialization().getSortTypes();

    if( elementReader.getTupleSerialization().areTypesRequired() )
      {
      if( keyClasses == null )
        throw new IllegalStateException( "types are required to perform serialization, grouping declared fields: " + elementReader.getTupleSerialization().getKeyFields() );

      if( sortClasses == null )
        throw new IllegalStateException( "types are required to perform serialization, sorting declared fields: " + elementReader.getTupleSerialization().getSortFields() );
      }

    keyReaders = HadoopTupleInputStream.getReadersFor( elementReader, keyClasses );
    sortReaders = HadoopTupleInputStream.getReadersFor( elementReader, sortClasses );
    }

  public TuplePair deserialize( TuplePair tuple ) throws IOException
    {
    if( tuple == null )
      tuple = createTuple();

    Tuple[] tuples = TuplePair.tuples( tuple );

    if( keyReaders == null )
      tuples[ 0 ] = inputStream.readUnTyped( tuples[ 0 ] );
    else
      tuples[ 0 ] = inputStream.readWith( keyReaders, tuples[ 0 ] );

    if( sortReaders == null )
      tuples[ 1 ] = inputStream.readUnTyped( tuples[ 1 ] );
    else
      tuples[ 1 ] = inputStream.readWith( sortReaders, tuples[ 1 ] );

    return tuple;
    }

  @Override
  protected TuplePair createTuple()
    {
    return new TuplePair();
    }
  }
