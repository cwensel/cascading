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

package cascading.flow.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import cascading.flow.FlowException;

/** Class JavaObjectSerializer is the default implementation of {@link ObjectSerializer}. */
public class JavaObjectSerializer implements ObjectSerializer
  {
  @Override
  public <T> byte[] serialize( T object, boolean compress ) throws IOException
    {
    if( object instanceof Map )
      return serializeMap( (Map<String, ?>) object, compress );

    if( object instanceof List )
      return serializeList( (List<?>) object, compress );

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    ObjectOutputStream out = new ObjectOutputStream( compress ? new GZIPOutputStream( bytes ) : bytes );

    try
      {
      out.writeObject( object );
      }
    finally
      {
      out.close();
      }

    return bytes.toByteArray();
    }

  @Override
  public <T> T deserialize( byte[] bytes, Class<T> type, boolean decompress ) throws IOException
    {

    if( Map.class.isAssignableFrom( type ) )
      return (T) deserializeMap( bytes, decompress );

    if( List.class.isAssignableFrom( type ) )
      {
      return (T) deserializeList( bytes, decompress );
      }

    ObjectInputStream in = null;

    try
      {
      ByteArrayInputStream byteStream = new ByteArrayInputStream( bytes );

      in = new ObjectInputStream( decompress ? new GZIPInputStream( byteStream ) : byteStream )
      {
      @Override
      protected Class<?> resolveClass( ObjectStreamClass desc ) throws IOException, ClassNotFoundException
        {
        try
          {
          return Class.forName( desc.getName(), false, Thread.currentThread().getContextClassLoader() );
          }
        catch( ClassNotFoundException exception )
          {
          return super.resolveClass( desc );
          }
        }
      };

      return (T) in.readObject();
      }
    catch( ClassNotFoundException exception )
      {
      throw new FlowException( "unable to deserialize data", exception );
      }
    finally
      {
      if( in != null )
        in.close();
      }
    }

  @Override
  public <T> boolean accepts( Class<T> type )
    {
    return Serializable.class.isAssignableFrom( type )
      || Map.class.isAssignableFrom( type )
      || List.class.isAssignableFrom( type );
    }

  public <T> byte[] serializeMap( Map<String, T> map, boolean compress ) throws IOException
    {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream( compress ? new GZIPOutputStream( bytes ) : bytes );

    Class<T> tClass;

    if( map.size() == 0 )
      tClass = (Class<T>) Object.class;
    else
      tClass = (Class<T>) map.values().iterator().next().getClass();
    try
      {
      out.writeInt( map.size() );
      out.writeUTF( tClass.getName() );

      for( Map.Entry<String, T> entry : map.entrySet() )
        {
        out.writeUTF( entry.getKey() );
        byte[] itemBytes = serialize( entry.getValue(), false );
        out.writeInt( itemBytes.length );
        out.write( itemBytes );
        }
      }
    finally
      {
      out.close();
      }

    return bytes.toByteArray();
    }

  public <T> Map<String, T> deserializeMap( byte[] bytes, boolean decompress ) throws IOException
    {
    ObjectInputStream in = null;

    try
      {
      ByteArrayInputStream byteStream = new ByteArrayInputStream( bytes );

      in = new ObjectInputStream( decompress ? new GZIPInputStream( byteStream ) : byteStream );

      int mapSize = in.readInt();
      Class<T> tClass = (Class<T>) Class.forName( in.readUTF() );

      Map<String, T> map = new HashMap<String, T>( mapSize );

      for( int j = 0; j < mapSize; j++ )
        {
        String key = in.readUTF();
        byte[] valBytes = new byte[ in.readInt() ];
        in.readFully( valBytes );
        map.put( key, deserialize( valBytes, tClass, false ) );
        }

      return map;
      }
    catch( ClassNotFoundException e )
      {
      throw new IOException( e );
      }
    finally
      {
      if( in != null )
        in.close();
      }
    }

  public <T> byte[] serializeList( List<T> list, boolean compress ) throws IOException
    {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    ObjectOutputStream out = new ObjectOutputStream( compress ? new GZIPOutputStream( bytes ) : bytes );

    Class<T> tClass;

    if( list.size() == 0 )
      tClass = (Class<T>) Object.class;
    else
      tClass = (Class<T>) list.get( 0 ).getClass();

    try
      {
      out.writeInt( list.size() );
      out.writeUTF( tClass.getName() );

      for( T item : list )
        {
        byte[] itemBytes = serialize( item, false );
        out.writeInt( itemBytes.length );
        out.write( itemBytes );
        }
      }
    finally
      {
      out.close();
      }

    return bytes.toByteArray();
    }

  public <T> List<T> deserializeList( byte[] bytes, boolean decompress ) throws IOException
    {
    ObjectInputStream in = null;

    try
      {
      ByteArrayInputStream byteStream = new ByteArrayInputStream( bytes );

      in = new ObjectInputStream( decompress ? new GZIPInputStream( byteStream ) : byteStream );

      int listSize = in.readInt();
      Class<T> tClass = (Class<T>) Class.forName( in.readUTF() );

      List<T> list = new ArrayList<T>( listSize );

      for( int i = 0; i < listSize; i++ )
        {
        byte[] itemBytes = new byte[ in.readInt() ];
        in.readFully( itemBytes );
        list.add( deserialize( itemBytes, tClass, false ) );
        }

      return list;
      }
    catch( ClassNotFoundException e )
      {
      throw new IOException( e );
      }
    finally
      {
      if( in != null )
        in.close();
      }
    }
  }
