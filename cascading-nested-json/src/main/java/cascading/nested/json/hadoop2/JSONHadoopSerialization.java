/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.nested.json.hadoop2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 *
 */
public class JSONHadoopSerialization extends Configured implements Serialization
  {
  ObjectMapper mapper = new IonObjectMapper();

  @Override
  public boolean accept( Class type )
    {
    return JsonNode.class.isAssignableFrom( type );
    }

  @Override
  public Serializer getSerializer( Class type )
    {
    return new Serializer()
      {
      private OutputStream out;

      @Override
      public void open( OutputStream out )
        {
        this.out = out;
        }

      @Override
      public void serialize( Object object ) throws IOException
        {
        mapper.writeValue( out, object );
        }

      @Override
      public void close() throws IOException
        {
        this.out.close();
        this.out = null;
        }
      };
    }

  @Override
  public Deserializer getDeserializer( Class type )
    {
    return new Deserializer()
      {
      private InputStream in;

      @Override
      public void open( InputStream in )
        {
        this.in = in;
        }

      @Override
      public Object deserialize( Object object ) throws IOException
        {
        return mapper.readTree( in );
        }

      @Override
      public void close() throws IOException
        {
        this.in.close();
        this.in = null;
        }
      };
    }
  }
