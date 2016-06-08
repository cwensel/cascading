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
import java.util.Map;

import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.io.IndexTuple;

public class IndexTupleDeserializer<T extends IndexTuple> extends BaseDeserializer<T>
  {
  protected Map<Integer, Class[]> typeMap;

  public IndexTupleDeserializer( TupleSerialization.SerializationElementReader elementReader )
    {
    super( elementReader );
    }

  public T deserialize( IndexTuple tuple ) throws IOException
    {
    if( tuple == null )
      tuple = createTuple();

    int ordinal = inputStream.readVInt();
    tuple.setIndex( ordinal );

    Class[] types = getTypesFor( ordinal );

    // in both cases, we need to fill a new Tuple instance
    if( types == null )
      tuple.setTuple( inputStream.readUnTyped( new Tuple() ) );
    else
      tuple.setTuple( inputStream.readTyped( types, new Tuple() ) );

    return (T) tuple;
    }

  @Override
  protected T createTuple()
    {
    return (T) new IndexTuple();
    }

  protected Class[] getTypesFor( int ordinal )
    {
    return null;
    }
  }