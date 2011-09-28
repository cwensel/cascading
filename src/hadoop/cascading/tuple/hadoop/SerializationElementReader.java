/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import cascading.tuple.TupleInputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializationElementReader implements TupleInputStream.ElementReader
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( SerializationElementReader.class );

  /** Field tupleSerialization */
  private final TupleSerialization tupleSerialization;

  /** Field deserializers */
  final Map<String, Deserializer> deserializers = new HashMap<String, Deserializer>();

  /**
   * Constructor SerializationElementReader creates a new SerializationElementReader instance.
   *
   * @param tupleSerialization of type TupleSerialization
   */
  public SerializationElementReader( TupleSerialization tupleSerialization )
    {
    this.tupleSerialization = tupleSerialization;

    tupleSerialization.initTokenMaps();
    }

  public Object read( int token, DataInputStream inputStream ) throws IOException
    {
    String className = getClassNameFor( token, inputStream );
    Deserializer deserializer = getDeserializerFor( inputStream, className );

    Object foundObject = null;
    Object object = null;

    try
      {
      object = deserializer.deserialize( foundObject );
      }
    catch( IOException exception )
      {
      LOG.error( "failed deserializing token: " + token + " with classname: " + className, exception );

      throw exception;
      }

    return object;
    }

  @Override
  public Comparator getComparatorFor( int token, DataInputStream inputStream ) throws IOException
    {
    Class type = tupleSerialization.getClass( getClassNameFor( token, inputStream ) );

    return tupleSerialization.getComparator( type );
    }

  private Deserializer getDeserializerFor( DataInputStream inputStream, String className ) throws IOException
    {
    Deserializer deserializer = deserializers.get( className );

    if( deserializer == null )
      {
      deserializer = tupleSerialization.getNewDeserializer( className );
      deserializer.open( inputStream );
      deserializers.put( className, deserializer );
      }

    return deserializer;
    }

  public String getClassNameFor( int token, DataInputStream inputStream ) throws IOException
    {
    String className = tupleSerialization.getClassNameFor( token );

    if( className == null )
      className = WritableUtils.readString( inputStream );

    return className;
    }

  public void close()
    {
    if( deserializers.size() == 0 )
      return;

    Collection<Deserializer> clone = new ArrayList<Deserializer>( deserializers.values() );

    deserializers.clear();

    for( Deserializer deserializer : clone )
      {
      try
        {
        deserializer.close();
        }
      catch( IOException exception )
        {
        // do nothing
        }
      }
    }
  }
