/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.tuple.TupleInputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.log4j.Logger;

public class SerializationElementReader implements TupleInputStream.ElementReader
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( SerializationElementReader.class );

  /** Field tupleSerialization */
  private final TupleSerialization tupleSerialization;

  /** Field deserializers */
  Map<String, Deserializer> deserializers = new HashMap<String, Deserializer>();

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
    String className = tupleSerialization.getClassNameFor( token );

    if( className == null )
      className = WritableUtils.readString( inputStream );

    Deserializer deserializer = deserializers.get( className );

    if( deserializer == null )
      {
      deserializer = tupleSerialization.getNewDeserializer( className );
      deserializer.open( inputStream );
      deserializers.put( className, deserializer );
      }

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
