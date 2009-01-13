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
import org.apache.log4j.Logger;

public class SerializationElementWriter implements TupleOutputStream.ElementWriter
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( SerializationElementWriter.class );

  /** Field tupleSerialization */
  private final TupleSerialization tupleSerialization;

  /** Field serializers */
  Map<Class, Serializer> serializers = new HashMap<Class, Serializer>();

  public SerializationElementWriter( TupleSerialization tupleSerialization )
    {
    this.tupleSerialization = tupleSerialization;

    tupleSerialization.initTokenMaps();
    }

  public void write( DataOutputStream outputStream, Comparable comparable ) throws IOException
    {
    Class<? extends Comparable> type = comparable.getClass();
    String className = type.getName();
    Integer token = tupleSerialization.getTokenFor( className );

    if( token == null )
      {
      WritableUtils.writeVInt( outputStream, TupleOutputStream.WRITABLE_TOKEN ); // denotes to punt to hadoop serialization
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
      serializer.serialize( comparable );
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
