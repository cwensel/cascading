/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;

import cascading.tuple.Comparison;
import cascading.tuple.hadoop.util.BytesComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Class BytesSerialization is an implementation of Hadoop's {@link Serialization} interface for use
 * by {@code byte} arrays ({@code byte[]}).
 * <p/>
 * To use, call<br/>
 * {@code TupleSerializationProps.addSerialization(properties, BytesSerialization.class.getName() );}
 * <p/>
 * This class also implements {@link Comparison} so it is not required to set a {@link cascading.tuple.hadoop.util.BytesComparator}
 * when attempting to group on a byte array via GroupBy or CoGroup.
 *
 * @see TupleSerialization#addSerialization(java.util.Map, String)
 * @see cascading.tuple.hadoop.util.BytesComparator
 * @see Comparison
 */
@SerializationToken(tokens = {126}, classNames = {"[B"})
public class BytesSerialization extends Configured implements Comparison<byte[]>, Serialization<byte[]>
  {

  public static class RawBytesDeserializer implements Deserializer<byte[]>
    {
    private DataInputStream in;

    @Override
    public void open( InputStream in ) throws IOException
      {
      if( in instanceof DataInputStream )
        this.in = (DataInputStream) in;
      else
        this.in = new DataInputStream( in );
      }

    @Override
    public byte[] deserialize( byte[] existing ) throws IOException
      {
      int len = in.readInt();

      byte[] bytes = existing != null && existing.length == len ? existing : new byte[ len ];

      in.readFully( bytes );

      return bytes;
      }

    @Override
    public void close() throws IOException
      {
      in.close();
      }
    }

  public static class RawBytesSerializer implements Serializer<byte[]>
    {
    private DataOutputStream out;

    @Override
    public void open( OutputStream out ) throws IOException
      {
      if( out instanceof DataOutputStream )
        this.out = (DataOutputStream) out;
      else
        this.out = new DataOutputStream( out );
      }

    @Override
    public void serialize( byte[] bytes ) throws IOException
      {
      out.writeInt( bytes.length );
      out.write( bytes );
      }

    @Override
    public void close() throws IOException
      {
      out.close();
      }
    }


  public BytesSerialization()
    {
    }

  @Override
  public boolean accept( Class<?> c )
    {
    return byte[].class == c;
    }

  @Override
  public Serializer<byte[]> getSerializer( Class<byte[]> c )
    {
    return new RawBytesSerializer();
    }

  @Override
  public Deserializer<byte[]> getDeserializer( Class<byte[]> c )
    {
    return new RawBytesDeserializer();
    }

  @Override
  public Comparator<byte[]> getComparator( Class<byte[]> type )
    {
    return new BytesComparator();
    }
  }