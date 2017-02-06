/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 *
 */
@SerializationToken(tokens = {222}, classNames = {"cascading.tuple.hadoop.TestText"})
public class TestSerialization extends Configured implements Comparison<TestText>, Serialization<TestText>
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

  @Override
  public Comparator<TestText> getComparator( Class<TestText> type )
    {
    return new TestTextComparator();
    }
  }
