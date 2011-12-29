/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.tuple.TupleOutputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializationElementWriter implements TupleOutputStream.ElementWriter
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( SerializationElementWriter.class );

  /** Field tupleSerialization */
  private final TupleSerialization tupleSerialization;

  /** Field serializers */
  final Map<Class, Serializer> serializers = new HashMap<Class, Serializer>();

  public SerializationElementWriter( TupleSerialization tupleSerialization )
    {
    this.tupleSerialization = tupleSerialization;

    tupleSerialization.initTokenMaps();
    }

  public void write( DataOutputStream outputStream, Object object ) throws IOException
    {
    Class<?> type = object.getClass();
    String className = type.getName();
    Integer token = tupleSerialization.getTokenFor( className );

    if( token == null )
      {
      LOG.debug( "no serialization token found for classname: {}", className );

      WritableUtils.writeVInt( outputStream, HadoopTupleOutputStream.WRITABLE_TOKEN ); // denotes to punt to hadoop serialization
      WritableUtils.writeString( outputStream, className );
      }
    else
      WritableUtils.writeVInt( outputStream, token );

    Serializer serializer = serializers.get( type );

    if( serializer == null )
      {
      serializer = tupleSerialization.getNewSerializer( type );
      serializer.open( outputStream );
      serializers.put( type, serializer );
      }

    try
      {
      serializer.serialize( object );
      }
    catch( IOException exception )
      {
      LOG.error( "failed serializing token: " + token + " with classname: " + className, exception );

      throw exception;
      }
    }

  public void close()
    {
    if( serializers.size() == 0 )
      return;

    Collection<Serializer> clone = new ArrayList<Serializer>( serializers.values() );

    serializers.clear();

    for( Serializer serializer : clone )
      {
      try
        {
        serializer.close();
        }
      catch( IOException exception )
        {
        // do nothing
        }
      }
    }
  }
