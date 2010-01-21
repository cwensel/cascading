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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 *
 */
@SerializationToken(tokens = {222}, classNames = {"cascading.tuple.hadoop.TestText"})
public class TestSerialization extends Configured implements Serialization<TestText>
  {

  public static class TestTextDeserializer implements Deserializer<TestText>
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
    public TestText deserialize( TestText testText ) throws IOException
      {
      return new TestText( WritableUtils.readString( in ) );
      }

    @Override
    public void close() throws IOException
      {
      in.close();
      }
    }

  public static class TestTextSerializer implements Serializer<TestText>
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
    public void serialize( TestText testText ) throws IOException
      {
      WritableUtils.writeString( out, testText.value );
      }

    @Override
    public void close() throws IOException
      {
      out.close();
      }
    }


  public TestSerialization()
    {
    }

  @Override
  public boolean accept( Class<?> c )
    {
    return TestText.class.isAssignableFrom( c );
    }

  @Override
  public Serializer<TestText> getSerializer( Class<TestText> c )
    {
    return new TestTextSerializer();
    }

  @Override
  public Deserializer<TestText> getDeserializer( Class<TestText> c )
    {
    return new TestTextDeserializer();
    }
  }
