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
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Class BigDecimalSerialization is an implementation of Hadoop's {@link org.apache.hadoop.io.serializer.Serialization} interface for use
 * by {@link BigDecimal} instances.
 * <p/>
 * To use, call<br/>
 * {@code TupleSerializationProps.addSerialization(properties, BigDecimalSerialization.class.getName());}
 * <p/>
 *
 * @see cascading.tuple.hadoop.TupleSerializationProps#addSerialization(java.util.Map, String)
 */
@SerializationToken(tokens = {125}, classNames = {"java.math.BigDecimal"})
public class BigDecimalSerialization extends Configured implements Serialization<BigDecimal>
  {
  public static class BigDecimalDeserializer implements Deserializer<BigDecimal>
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
    public BigDecimal deserialize( BigDecimal existing ) throws IOException
      {
      int len = in.readInt();
      byte[] valueBytes = new byte[ len ];

      in.readFully( valueBytes );

      BigInteger value = new BigInteger( valueBytes );

      return new BigDecimal( value, in.readInt() );
      }

    @Override
    public void close() throws IOException
      {
      in.close();
      }
    }

  public static class BigDecimalSerializer implements Serializer<BigDecimal>
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
    public void serialize( BigDecimal bigDecimal ) throws IOException
      {
      BigInteger value = bigDecimal.unscaledValue();
      byte[] valueBytes = value.toByteArray();

      out.writeInt( valueBytes.length );
      out.write( valueBytes );
      out.writeInt( bigDecimal.scale() );
      }

    @Override
    public void close() throws IOException
      {
      out.close();
      }
    }


  public BigDecimalSerialization()
    {
    }

  @Override
  public boolean accept( Class<?> c )
    {
    return BigDecimal.class == c;
    }

  @Override
  public Serializer<BigDecimal> getSerializer( Class<BigDecimal> c )
    {
    return new BigDecimalSerializer();
    }

  @Override
  public Deserializer<BigDecimal> getDeserializer( Class<BigDecimal> c )
    {
    return new BigDecimalDeserializer();
    }
  }