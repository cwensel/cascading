/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.property.Props;
import cascading.util.Util;

/**
 * Class TupleSerializationProps is a fluent interface for building properties to be passed to a
 * {@link cascading.flow.FlowConnector} before creating new {@link cascading.flow.Flow} instances.
 * <p/>
 * See {@link TupleSerialization} for details on these properties.
 *
 * @see TupleSerialization
 */
public class TupleSerializationProps extends Props
  {
  public static final String SERIALIZATION_TOKENS = "cascading.serialization.tokens";
  public static final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

  Map<Integer, String> serializationTokens = new LinkedHashMap<Integer, String>();
  List<String> hadoopSerializations = new ArrayList<String>();

  /**
   * Adds the given token and className pair as a serialization token property. During object serialization and deserialization,
   * the given token will be used instead of the className when an instance of the className is encountered.
   *
   * @param properties of type Map
   * @param token      of type int
   * @param className  of type String
   */
  public static void addSerializationToken( Map<Object, Object> properties, int token, String className )
    {
    String tokens = getSerializationTokens( properties );

    properties.put( SERIALIZATION_TOKENS, Util.join( ",", Util.removeNulls( tokens, token + "=" + className ) ) );
    }

  /**
   * Returns the serialization tokens property.
   *
   * @param properties of type Map
   * @return returns a String
   */
  public static String getSerializationTokens( Map<Object, Object> properties )
    {
    return (String) properties.get( SERIALIZATION_TOKENS );
    }

  /**
   * Adds the given className as a Hadoop IO serialization class.
   *
   * @param properties of type Map
   * @param className  of type String
   */
  public static void addSerialization( Map<Object, Object> properties, String className )
    {
    String serializations = (String) properties.get( HADOOP_IO_SERIALIZATIONS );

    properties.put( HADOOP_IO_SERIALIZATIONS, Util.join( ",", Util.removeNulls( serializations, className ) ) );
    }

  public static TupleSerializationProps tupleSerializationProps()
    {
    return new TupleSerializationProps();
    }

  public TupleSerializationProps()
    {
    }

  public Map<Integer, String> getSerializationTokens()
    {
    return serializationTokens;
    }

  public TupleSerializationProps setSerializationTokens( Map<Integer, String> serializationTokens )
    {
    this.serializationTokens = serializationTokens;

    return this;
    }

  public TupleSerializationProps addSerializationTokens( Map<Integer, String> serializationTokens )
    {
    this.serializationTokens.putAll( serializationTokens );

    return this;
    }

  public TupleSerializationProps addSerializationToken( int token, String serialization )
    {
    this.serializationTokens.put( token, serialization );

    return this;
    }

  public List<String> getHadoopSerializations()
    {
    return hadoopSerializations;
    }

  public TupleSerializationProps setHadoopSerializations( List<String> hadoopSerializations )
    {
    this.hadoopSerializations = hadoopSerializations;

    return this;
    }

  public TupleSerializationProps addHadoopSerializations( List<String> hadoopSerializations )
    {
    this.hadoopSerializations.addAll( hadoopSerializations );

    return this;
    }

  public TupleSerializationProps addHadoopSerialization( String hadoopSerialization )
    {
    this.hadoopSerializations.add( hadoopSerialization );

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    for( Map.Entry<Integer, String> entry : serializationTokens.entrySet() )
      addSerializationToken( properties, entry.getKey(), entry.getValue() );

    for( String hadoopSerialization : hadoopSerializations )
      addSerialization( properties, hadoopSerialization );
    }
  }
