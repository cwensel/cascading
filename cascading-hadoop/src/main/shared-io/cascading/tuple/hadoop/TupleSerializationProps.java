/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import cascading.tuple.Tuple;
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
  public static final String SERIALIZATION_COMPARISON_BITWISE_PREVENT = "cascading.serialization.comparison.bitwise.prevent";
  public static final String IGNORE_TYPES = "cascading.serialization.types.ignored";
  public static final String REQUIRE_TYPES = "cascading.serialization.types.required";
  public static final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

  Map<Integer, String> serializationTokens = new LinkedHashMap<Integer, String>();
  List<String> hadoopSerializations = new ArrayList<String>();
  Boolean ignoreTypes;
  Boolean requireTypes;
  Boolean preventBitWiseComparisons;

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

  /**
   * Creates a new TupleSerializationProps instance.
   *
   * @return TupleSerializationProps instance
   */
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

  /**
   * Method setSerializationTokens sets the given integer tokens and classNames Map as a serialization properties.
   * <p/>
   * During object serialization and deserialization, the given tokens will be used instead of the className when an
   * instance of the className is encountered.
   *
   * @param serializationTokens Map of Integer tokens and String classnames
   * @return this
   */
  public TupleSerializationProps setSerializationTokens( Map<Integer, String> serializationTokens )
    {
    this.serializationTokens = serializationTokens;

    return this;
    }

  /**
   * Method addSerializationTokens adds the given integer tokens and classNames Map as a serialization properties.
   * <p/>
   * During object serialization and deserialization, the given tokens will be used instead of the className when an
   * instance of the className is encountered.
   *
   * @param serializationTokens Map of Integer tokens and String classnames
   * @return this
   */
  public TupleSerializationProps addSerializationTokens( Map<Integer, String> serializationTokens )
    {
    this.serializationTokens.putAll( serializationTokens );

    return this;
    }

  /**
   * Method addSerializationToken adds the given integer token and classNames as a serialization properties.
   * <p/>
   * During object serialization and deserialization, the given tokens will be used instead of the className when an
   * instance of the className is encountered.
   *
   * @param token                  type int
   * @param serializationClassName type String
   * @return this
   */
  public TupleSerializationProps addSerializationToken( int token, String serializationClassName )
    {
    this.serializationTokens.put( token, serializationClassName );

    return this;
    }

  public List<String> getHadoopSerializations()
    {
    return hadoopSerializations;
    }

  /**
   * Method setHadoopSerializations sets the Hadoop serialization classNames to be used as properties.
   *
   * @param hadoopSerializationClassNames List of classNames
   * @return this
   */
  public TupleSerializationProps setHadoopSerializations( List<String> hadoopSerializationClassNames )
    {
    this.hadoopSerializations = hadoopSerializationClassNames;

    return this;
    }

  /**
   * Method addHadoopSerializations adds the Hadoop serialization classNames to be used as properties.
   *
   * @param hadoopSerializationClassNames List of classNames
   * @return this
   */
  public TupleSerializationProps addHadoopSerializations( List<String> hadoopSerializationClassNames )
    {
    this.hadoopSerializations.addAll( hadoopSerializationClassNames );

    return this;
    }

  /**
   * Method addHadoopSerialization adds a Hadoop serialization className to be used as properties.
   *
   * @param hadoopSerializationClassName List of classNames
   * @return this
   */
  public TupleSerializationProps addHadoopSerialization( String hadoopSerializationClassName )
    {
    this.hadoopSerializations.add( hadoopSerializationClassName );

    return this;
    }

  public Boolean getIgnoreTypes()
    {
    return ignoreTypes;
    }

  /**
   * Method setIgnoreTypes forces the {@link TupleSerialization} class to ignore any and all
   * declared types causing the serialization to write each type or {@link SerializationToken}
   * per {@link Tuple} element.
   * <p/>
   * This disables the declared type optimizations.
   * <p/>
   * See {@link #setRequireTypes(Boolean)} to force a failure if field type information is missing.
   *
   * @param ignoreTypes
   * @return
   */
  public TupleSerializationProps setIgnoreTypes( Boolean ignoreTypes )
    {
    this.ignoreTypes = ignoreTypes;

    return this;
    }

  public Boolean getRequireTypes()
    {
    return requireTypes;
    }

  /**
   * Method setRequireTypes forces {@link TupleSerialization} to fail if field types are not declared.
   * <p/>
   * This ensures the field type optimizations are leveraged.
   * <p/>
   * See {@link #setIgnoreTypes(Boolean)} to force field type information to be discarded.
   *
   * @param requireTypes
   * @return
   */
  public TupleSerializationProps setRequireTypes( Boolean requireTypes )
    {
    this.requireTypes = requireTypes;

    return this;
    }

  /**
   * Method preventBitWiseComparison will enable/disable bitwise comparisons of grouping keys
   * during ordered partitioning ({@link cascading.pipe.GroupBy} and {@link cascading.pipe.CoGroup}).
   * <p/>
   * If natural ordering of grouping/join keys is required, disable bit wise comparisons. They are enabled
   * by default (subject to the below conditions).
   * <p/>
   * Bit wise comparisons will only apply if the {@link cascading.tuple.Fields} used in the grouping/join are
   * declared and no custom {@link java.util.Comparator} instances are provided on the grouping/key Fields, or
   * no secondary sorting is being performed on a GroupBy.
   *
   * @param preventBitWiseComparisons set to true to disable bit wise comparisons
   * @return this
   */
  public TupleSerializationProps preventBitWiseComparison( boolean preventBitWiseComparisons )
    {
    this.preventBitWiseComparisons = preventBitWiseComparisons;

    return this;
    }

  public boolean getPreventBitWiseComparisons()
    {
    return preventBitWiseComparisons;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    for( Map.Entry<Integer, String> entry : serializationTokens.entrySet() )
      addSerializationToken( properties, entry.getKey(), entry.getValue() );

    for( String hadoopSerialization : hadoopSerializations )
      addSerialization( properties, hadoopSerialization );

    if( ignoreTypes != null )
      properties.put( IGNORE_TYPES, ignoreTypes.toString() );

    if( requireTypes != null )
      properties.put( REQUIRE_TYPES, requireTypes.toString() );

    if( preventBitWiseComparisons != null )
      properties.put( SERIALIZATION_COMPARISON_BITWISE_PREVENT, preventBitWiseComparisons.toString() );
    }
  }
