/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;

import cascading.tuple.Comparison;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Class BytesSerialization is an implementation of Hadoop's {@link Serialization} interface for use
 * by {@code byte} arrays ({@code byte[]}).
 * <p/>
 * To use, call<br/>
 * {@code TupleSerialization.addSerialization(properties, BytesSerialization.class.getName() );}
 * <p/>
 * This class also implements {@link Comparison} so it is not required to set a {@link BytesComparator}
 * when attempting to group on a byte array via GroupBy or CoGroup.
 *
 * @see cascading.tuple.hadoop.BytesComparator
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
    public byte[] deserialize( byte[] testText ) throws IOException
      {
      int len = in.readInt();

      byte[] bytes = new byte[ len ];

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