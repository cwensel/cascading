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

package cascading.tuple.hadoop.io;

import java.io.IOException;
import java.io.InputStream;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.io.TupleInputStream;
import org.apache.hadoop.io.serializer.Deserializer;

abstract class BaseDeserializer<T extends Tuple> implements Deserializer<T>
  {
  private final TupleSerialization.SerializationElementReader elementReader;
  private TupleInputStream.TupleElementReader[] readers;

  HadoopTupleInputStream inputStream;

  protected BaseDeserializer( TupleSerialization.SerializationElementReader elementReader )
    {
    this.elementReader = elementReader;
    }

  protected void setReaders( Fields fields )
    {
    if( fields == null )
      return;

    Class[] classes = elementReader.getTupleSerialization().getTypesFor( fields );

    if( elementReader.getTupleSerialization().areTypesRequired() )
      {
      if( classes == null )
        throw new IllegalStateException( "types are required to perform serialization, declared fields: " + fields );
      }

    readers = HadoopTupleInputStream.getReadersFor( elementReader, classes );
    }

  public void open( InputStream in )
    {
    if( in instanceof HadoopTupleInputStream )
      inputStream = (HadoopTupleInputStream) in;
    else
      inputStream = new HadoopTupleInputStream( in, elementReader );
    }

  public T deserialize( T tuple ) throws IOException
    {
    if( tuple == null )
      tuple = createTuple();

    if( readers == null )
      return inputStream.readUnTyped( tuple );
    else
      return inputStream.readWith( readers, tuple );
    }

  protected abstract T createTuple();

  public void close() throws IOException
    {
    try
      {
      if( inputStream != null )
        inputStream.close();
      }
    finally
      {
      inputStream = null;
      }
    }
  }
