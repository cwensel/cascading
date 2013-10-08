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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProps;
import cascading.tuple.Comparison;
import cascading.tuple.Tuple;
import cascading.tuple.TupleException;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.hadoop.io.IndexTupleDeserializer;
import cascading.tuple.hadoop.io.IndexTupleSerializer;
import cascading.tuple.hadoop.io.TupleDeserializer;
import cascading.tuple.hadoop.io.TuplePairDeserializer;
import cascading.tuple.hadoop.io.TuplePairSerializer;
import cascading.tuple.hadoop.io.TupleSerializer;
import cascading.tuple.io.IndexTuple;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;
import cascading.tuple.io.TuplePair;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.tuple.hadoop.TupleSerializationProps.HADOOP_IO_SERIALIZATIONS;

/**
 * Class TupleSerialization is an implementation of Hadoop's {@link Serialization} interface.
 * <p/>
 * Typically developers will not use this implementation directly as it is automatically added
 * to any relevant MapReduce jobs via the {@link JobConf}.
 * <p/>
 * By default, all primitive types are natively handled, and {@link org.apache.hadoop.io.BytesWritable}
 * has a pre-configured serialization token since byte arrays are not handled natively by {@link Tuple}.
 * <p/>
 * To add or manipulate Hadoop serializations or Cascading serializations tokens, see
 * {@link TupleSerializationProps} for a fluent property builder class.
 * <p/>
 * By default this Serialization interface registers the class {@link org.apache.hadoop.io.ByteWritable} as
 * token 127.
 */
@SerializationToken(
  tokens = {127},
  classNames = {"org.apache.hadoop.io.BytesWritable"})
public class TupleSerialization extends Configured implements Serialization
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( TupleSerialization.class );

  /** Field defaultComparator * */
  private Comparator defaultComparator;
  /** Field classCache */
  private final Map<String, Class> classCache = new HashMap<String, Class>();
  /** Field serializationFactory */
  private SerializationFactory serializationFactory;

  /** Field tokenClassesMap */
  private HashMap<Integer, String> tokenClassesMap;
  /** Field classesTokensMap */
  private HashMap<String, Integer> classesTokensMap;
  /** Field tokenMapSize */
  private long tokensSize = 0;

  /**
   * Adds the given token and className pair as a serialization token property. During object serialization and deserialization,
   * the given token will be used instead of the className when an instance of the className is encountered.
   * <p/>
   * This method has moved to {@link TupleSerializationProps#addSerializationToken(java.util.Map, int, String)}.
   *
   * @param properties of type Map
   * @param token      of type int
   * @param className  of type String
   */
  @Deprecated
  public static void addSerializationToken( Map<Object, Object> properties, int token, String className )
    {
    TupleSerializationProps.addSerializationToken( properties, token, className );
    }

  /**
   * Returns the serialization tokens property.
   * <p/>
   * This method has moved to {@link TupleSerializationProps#getSerializationTokens(java.util.Map)}.
   *
   * @param properties of type Map
   * @return returns a String
   */
  @Deprecated
  public static String getSerializationTokens( Map<Object, Object> properties )
    {
    return TupleSerializationProps.getSerializationTokens( properties );
    }

  static String getSerializationTokens( Configuration jobConf )
    {
    return jobConf.get( TupleSerializationProps.SERIALIZATION_TOKENS );
    }

  /**
   * Adds the given className as a Hadoop IO serialization class.
   * <p/>
   * This method has moved to {@link TupleSerializationProps#addSerialization(java.util.Map, String)}.
   *
   * @param properties of type Map
   * @param className  of type String
   */
  @Deprecated
  public static void addSerialization( Map<Object, Object> properties, String className )
    {
    TupleSerializationProps.addSerialization( properties, className );
    }

  /**
   * Adds this class as a Hadoop Serialization class. This method is safe to call redundantly.
   * <p/>
   * This method will guarantee {@link TupleSerialization} and {@link WritableSerialization} are
   * first in the list, as both are required.
   *
   * @param jobConf of type JobConf
   */
  public static void setSerializations( JobConf jobConf )
    {
    String serializations = getSerializations( jobConf );

    LinkedList<String> list = new LinkedList<String>();

    if( serializations != null && !serializations.isEmpty() )
      Collections.addAll( list, serializations.split( "," ) );

    // required by MultiInputSplit
    String writable = WritableSerialization.class.getName();
    String tuple = TupleSerialization.class.getName();

    list.remove( writable );
    list.remove( tuple );

    list.addFirst( writable );
    list.addFirst( tuple );

    // make writable last
    jobConf.set( HADOOP_IO_SERIALIZATIONS, Util.join( list, "," ) );
    }

  static String getSerializations( Configuration jobConf )
    {
    return jobConf.get( HADOOP_IO_SERIALIZATIONS, null );
    }

  public static Comparator getDefaultComparator( Comparator comparator, Configuration jobConf )
    {
    String typeName = jobConf.get( FlowProps.DEFAULT_ELEMENT_COMPARATOR );

    if( Util.isEmpty( typeName ) )
      return null;

    if( comparator == null )
      return createComparator( jobConf, typeName );

    if( comparator.getClass().getName().equals( typeName ) && !( comparator instanceof Configured ) )
      return comparator;

    return createComparator( jobConf, typeName );
    }

  public static Comparator getDefaultComparator( Configuration jobConf )
    {
    String typeName = jobConf.get( FlowProps.DEFAULT_ELEMENT_COMPARATOR );

    if( Util.isEmpty( typeName ) )
      return null;

    return createComparator( jobConf, typeName );
    }

  private static Comparator createComparator( Configuration jobConf, String typeName )
    {
    LOG.debug( "using default comparator: {}", typeName );

    try
      {
      Class<Comparator> type = (Class<Comparator>) TupleSerialization.class.getClassLoader().loadClass( typeName );

      return ReflectionUtils.newInstance( type, jobConf );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + typeName, exception );
      }
    }

  /** Constructor TupleSerialization creates a new TupleSerialization instance. */
  public TupleSerialization()
    {
    }

  public TupleSerialization( final FlowProcess<JobConf> flowProcess )
    {
    super( new Configuration()
    {
    @Override
    public String get( String name )
      {
      return get( name, null );
      }

    @Override
    public String get( String name, String defaultValue )
      {
      Object value = flowProcess.getProperty( name );
      return value == null ? defaultValue : String.valueOf( value );
      }
    } );
    }

  /**
   * Constructor TupleSerialization creates a new TupleSerialization instance.
   *
   * @param conf of type Configuration
   */
  public TupleSerialization( Configuration conf )
    {
    super( conf );
    }

  @Override
  public void setConf( Configuration conf )
    {
    super.setConf( conf );

    if( conf != null )
      defaultComparator = getDefaultComparator( conf );
    }

  @Override
  public Configuration getConf()
    {
    if( super.getConf() == null )
      setConf( new JobConf() );

    return super.getConf();
    }

  SerializationFactory getSerializationFactory()
    {
    if( serializationFactory == null )
      serializationFactory = new SerializationFactory( getConf() );

    return serializationFactory;
    }

  /** Must be called before {@link #getClassNameFor(int)} and {@link #getTokenFor(String)} methods. */
  void initTokenMaps()
    {
    if( tokenClassesMap != null )
      return;

    tokenClassesMap = new HashMap<Integer, String>();
    classesTokensMap = new HashMap<String, Integer>();

    String tokenProperty = getSerializationTokens( getConf() );

    if( tokenProperty != null )
      {
      tokenProperty = tokenProperty.replaceAll( "\\s", "" ); // allow for whitespace in token set

      for( String pair : tokenProperty.split( "," ) )
        {
        String[] elements = pair.split( "=" );
        addToken( null, Integer.parseInt( elements[ 0 ] ), elements[ 1 ] );
        }
      }

    String serializationsString = getSerializations( getConf() );

    LOG.debug( "using hadoop serializations from the job conf: {} ", serializationsString );

    if( serializationsString == null )
      return;

    String[] serializations = serializationsString.split( "," );

    for( String serializationName : serializations )
      {
      try
        {
        Class type = getConf().getClassByName( serializationName );

        SerializationToken tokenAnnotation = (SerializationToken) type.getAnnotation( SerializationToken.class );

        if( tokenAnnotation == null )
          continue;

        if( tokenAnnotation.tokens().length != tokenAnnotation.classNames().length )
          throw new CascadingException( "serialization annotation tokens and classNames must be the same length" );

        int[] tokens = tokenAnnotation.tokens();

        for( int i = 0; i < tokens.length; i++ )
          addToken( type, tokens[ i ], tokenAnnotation.classNames()[ i ] );
        }
      catch( ClassNotFoundException exception )
        {
        LOG.warn( "unable to load serialization class: {}", serializationName, exception );
        }
      }

    tokensSize = tokenClassesMap.size();
    }

  private void addToken( Class type, int token, String className )
    {
    if( type != null && !type.getName().startsWith( "cascading." ) && token < 128 )
      throw new CascadingException( "serialization annotation tokens may not be less than 128, was: " + token );

    if( tokenClassesMap.containsKey( token ) )
      {
      if( type == null )
        throw new IllegalStateException( "duplicate serialization token: " + token + " for class: " + className + " found in properties" );

      throw new IllegalStateException( "duplicate serialization token: " + token + " for class: " + className + " on serialization: " + type.getName() );
      }

    if( classesTokensMap.containsKey( className ) )
      {
      if( type == null )
        throw new IllegalStateException( "duplicate serialization classname: " + className + " for token: " + token + " found in properties " );

      throw new IllegalStateException( "duplicate serialization classname: " + className + " for token: " + token + " on serialization: " + type.getName() );
      }

    LOG.debug( "adding serialization token: {}, for classname: {}", token, className );

    tokenClassesMap.put( token, className );
    classesTokensMap.put( className, token );
    }

  /**
   * Returns the className for the given token.
   *
   * @param token of type int
   * @return a String
   */
  final String getClassNameFor( int token )
    {
    if( tokensSize == 0 )
      return null;

    return tokenClassesMap.get( token );
    }

  final long getTokensMapSize()
    {
    return tokensSize;
    }

  /**
   * Returns the token for the given className.
   *
   * @param className of type String
   * @return an Integer
   */
  final Integer getTokenFor( String className )
    {
    if( tokensSize == 0 )
      return null;

    return classesTokensMap.get( className );
    }

  public Comparator getDefaultComparator()
    {
    return defaultComparator;
    }

  public Comparator getComparator( Class type )
    {
    Serialization serialization = getSerialization( type );

    Comparator comparator = null;

    if( serialization instanceof Comparison )
      comparator = ( (Comparison) serialization ).getComparator( type );

    if( comparator != null )
      return comparator;

    return defaultComparator;
    }

  Serialization getSerialization( String className )
    {
    return getSerialization( getClass( className ) );
    }

  Serialization getSerialization( Class type )
    {
    return getSerializationFactory().getSerialization( type );
    }

  Serializer getNewSerializer( Class type )
    {
    try
      {
      Serializer serializer = getSerializationFactory().getSerializer( type );

      if( serializer == null )
        throw new CascadingException( "unable to load serializer for: " + type.getName() + " from: " + getSerializationFactory().getClass().getName() );

      return serializer;
      }
    catch( NullPointerException exception )
      {
      throw new CascadingException( "unable to load serializer for: " + type.getName() + " from: " + getSerializationFactory().getClass().getName() );
      }
    }

  Deserializer getNewDeserializer( String className )
    {
    try
      {
      Deserializer deserializer = getSerializationFactory().getDeserializer( getClass( className ) );

      if( deserializer == null )
        throw new CascadingException( "unable to load deserializer for: " + className + " from: " + getSerializationFactory().getClass().getName() );

      return deserializer;
      }
    catch( NullPointerException exception )
      {
      throw new CascadingException( "unable to load deserializer for: " + className + " from: " + getSerializationFactory().getClass().getName() );
      }
    }

  TuplePairDeserializer getTuplePairDeserializer()
    {
    return new TuplePairDeserializer( getElementReader() );
    }

  /**
   * Method getElementReader returns the elementReader of this TupleSerialization object.
   *
   * @return the elementReader (type SerializationElementReader) of this TupleSerialization object.
   */
  public SerializationElementReader getElementReader()
    {
    return new SerializationElementReader( this );
    }

  TupleDeserializer getTupleDeserializer()
    {
    return new TupleDeserializer( getElementReader() );
    }

  private TuplePairSerializer getTuplePairSerializer()
    {
    return new TuplePairSerializer( getElementWriter() );
    }

  IndexTupleDeserializer getIndexTupleDeserializer()
    {
    return new IndexTupleDeserializer( getElementReader() );
    }

  /**
   * Method getElementWriter returns the elementWriter of this TupleSerialization object.
   *
   * @return the elementWriter (type SerializationElementWriter) of this TupleSerialization object.
   */
  public SerializationElementWriter getElementWriter()
    {
    return new SerializationElementWriter( this );
    }

  private TupleSerializer getTupleSerializer()
    {
    return new TupleSerializer( getElementWriter() );
    }

  private IndexTupleSerializer getIndexTupleSerializer()
    {
    return new IndexTupleSerializer( getElementWriter() );
    }

  /**
   * Method accept implements {@link Serialization#accept(Class)}.
   *
   * @param c of type Class
   * @return boolean
   */
  public boolean accept( Class c )
    {
    return Tuple.class == c || TuplePair.class == c || IndexTuple.class == c;
    }

  /**
   * Method getDeserializer implements {@link Serialization#getDeserializer(Class)}.
   *
   * @param c of type Class
   * @return Deserializer
   */
  public Deserializer getDeserializer( Class c )
    {
    if( c == Tuple.class )
      return getTupleDeserializer();
    else if( c == TuplePair.class )
      return getTuplePairDeserializer();
    else if( c == IndexTuple.class )
      return getIndexTupleDeserializer();

    throw new IllegalArgumentException( "unknown class, cannot deserialize: " + c.getName() );
    }

  /**
   * Method getSerializer implements {@link Serialization#getSerializer(Class)}.
   *
   * @param c of type Class
   * @return Serializer
   */
  public Serializer getSerializer( Class c )
    {
    if( c == Tuple.class )
      return getTupleSerializer();
    else if( c == TuplePair.class )
      return getTuplePairSerializer();
    else if( c == IndexTuple.class )
      return getIndexTupleSerializer();

    throw new IllegalArgumentException( "unknown class, cannot serialize: " + c.getName() );
    }

  public Class getClass( String className )
    {
    Class type = classCache.get( className );

    if( type != null )
      return type;

    try
      {
      if( className.charAt( 0 ) == '[' )
        type = Class.forName( className, true, Thread.currentThread().getContextClassLoader() );
      else
        type = Thread.currentThread().getContextClassLoader().loadClass( className );
      }
    catch( ClassNotFoundException exception )
      {
      throw new TupleException( "unable to load class named: " + className, exception );
      }

    classCache.put( className, type );

    return type;
    }

  public static class SerializationElementReader implements TupleInputStream.ElementReader
    {
    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger( SerializationElementReader.class );

    /** Field tupleSerialization */
    private final TupleSerialization tupleSerialization;

    /** Field deserializers */
    final Map<String, Deserializer> deserializers = new HashMap<String, Deserializer>();

    /**
     * Constructor SerializationElementReader creates a new SerializationElementReader instance.
     *
     * @param tupleSerialization of type TupleSerialization
     */
    public SerializationElementReader( TupleSerialization tupleSerialization )
      {
      this.tupleSerialization = tupleSerialization;

      tupleSerialization.initTokenMaps();
      }

    public Object read( int token, DataInputStream inputStream ) throws IOException
      {
      String className = getClassNameFor( token, inputStream );
      Deserializer deserializer = getDeserializerFor( inputStream, className );

      Object foundObject = null;
      Object object;

      try
        {
        object = deserializer.deserialize( foundObject );
        }
      catch( IOException exception )
        {
        LOG.error( "failed deserializing token: " + token + " with classname: " + className, exception );

        throw exception;
        }

      return object;
      }

    @Override
    public Comparator getComparatorFor( int token, DataInputStream inputStream ) throws IOException
      {
      Class type = tupleSerialization.getClass( getClassNameFor( token, inputStream ) );

      return tupleSerialization.getComparator( type );
      }

    private Deserializer getDeserializerFor( DataInputStream inputStream, String className ) throws IOException
      {
      Deserializer deserializer = deserializers.get( className );

      if( deserializer == null )
        {
        deserializer = tupleSerialization.getNewDeserializer( className );
        deserializer.open( inputStream );
        deserializers.put( className, deserializer );
        }

      return deserializer;
      }

    public String getClassNameFor( int token, DataInputStream inputStream ) throws IOException
      {
      String className = tupleSerialization.getClassNameFor( token );

      try
        {
        if( className == null )
          className = WritableUtils.readString( inputStream );
        }
      catch( IOException exception )
        {
        LOG.error( "unable to resolve token: {}, to a valid classname, with token map of size: {}, rethrowing IOException", token, tupleSerialization.getTokensMapSize() );
        throw exception;
        }

      return className;
      }

    public void close()
      {
      if( deserializers.size() == 0 )
        return;

      Collection<Deserializer> clone = new ArrayList<Deserializer>( deserializers.values() );

      deserializers.clear();

      for( Deserializer deserializer : clone )
        {
        try
          {
          deserializer.close();
          }
        catch( IOException exception )
          {
          // do nothing
          }
        }
      }
    }

  public static class SerializationElementWriter implements TupleOutputStream.ElementWriter
    {
    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger( SerializationElementWriter.class );

    /** Field tupleSerialization */
    private final TupleSerialization tupleSerialization;

    /** Field serializers */
    final Map<Class, Serializer> serializers = new HashMap<Class, Serializer>();

    public SerializationElementWriter( TupleSerialization tupleSerialization )
      {
      this.tupleSerialization = tupleSerialization;

      tupleSerialization.initTokenMaps();
      }

    public void write( DataOutputStream outputStream, Object object ) throws IOException
      {
      Class<?> type = object.getClass();
      String className = type.getName();
      Integer token = tupleSerialization.getTokenFor( className );

      if( token == null )
        {
        LOG.debug( "no serialization token found for classname: {}", className );

        WritableUtils.writeVInt( outputStream, HadoopTupleOutputStream.WRITABLE_TOKEN ); // denotes to punt to hadoop serialization
        WritableUtils.writeString( outputStream, className );
        }
      else
        {
        WritableUtils.writeVInt( outputStream, token );
        }

      Serializer serializer = serializers.get( type );

      if( serializer == null )
        {
        serializer = tupleSerialization.getNewSerializer( type );
        serializer.open( outputStream );
        serializers.put( type, serializer );
        }

      try
        {
        serializer.serialize( object );
        }
      catch( IOException exception )
        {
        LOG.error( "failed serializing token: " + token + " with classname: " + className, exception );

        throw exception;
        }
      }

    public void close()
      {
      if( serializers.size() == 0 )
        return;

      Collection<Serializer> clone = new ArrayList<Serializer>( serializers.values() );

      serializers.clear();

      for( Serializer serializer : clone )
        {
        try
          {
          serializer.close();
          }
        catch( IOException exception )
          {
          // do nothing
          }
        }
      }
    }
  }