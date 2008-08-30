/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import cascading.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

public class TupleSerialization extends Configured implements Serialization<Tuple>
  {

  static class TupleDeserializer extends Configured implements Deserializer<Tuple>
    {
    private Class<? extends Tuple> tupleClass;
    private DataInputStream intputStream;

    TupleDeserializer( Class<? extends Tuple> tupleClass )
      {
      this.tupleClass = tupleClass;
      }

    public TupleDeserializer( Configuration conf, Class<? extends Tuple> type )
      {
      setConf( conf );
      this.tupleClass = type;
      }

    public void open( InputStream in )
      {
      if( in instanceof DataInputStream )
        intputStream = (DataInputStream) in;
      else
        intputStream = new DataInputStream( in );
      }

    public Tuple deserialize( Tuple tuple ) throws IOException
      {
      if( tuple == null )
        tuple = (Tuple) ReflectionUtils.newInstance( tupleClass, getConf() );

      tuple.readFields( intputStream );

      return tuple;
      }

    public void close() throws IOException
      {
      intputStream.close();
      }
    }

  static class TupleSerializer implements Serializer<Tuple>
    {
    private DataOutputStream outputStream;

    public void open( OutputStream out )
      {
      if( out instanceof DataOutputStream )
        outputStream = (DataOutputStream) out;
      else
        outputStream = new DataOutputStream( out );
      }

    public void serialize( Tuple tuple ) throws IOException
      {
      tuple.write( outputStream );
      }

    public void close() throws IOException
      {
      outputStream.close();
      }
    }

  public boolean accept( Class<?> c )
    {
    return Tuple.class.isAssignableFrom( c );
    }

  public Deserializer<Tuple> getDeserializer( Class<Tuple> c )
    {
    return new TupleDeserializer( getConf(), c );
    }

  public Serializer<Tuple> getSerializer( Class<Tuple> c )
    {
    return new TupleSerializer();
    }

  }