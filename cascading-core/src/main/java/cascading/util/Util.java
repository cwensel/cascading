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

package cascading.util;

import java.beans.Expression;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.coerce.Coercions;
import cascading.util.jgrapht.ComponentAttributeProvider;
import cascading.util.jgrapht.DOTExporter;
import cascading.util.jgrapht.EdgeNameProvider;
import cascading.util.jgrapht.IntegerNameProvider;
import cascading.util.jgrapht.VertexNameProvider;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class Util provides reusable operations. */
public class Util
  {
  /**
   * On OS X only, and if the graphviz dot binary is installed, when true, dot will be invoked to convert the dot file
   * to a pdf document.
   */
  public static final String CONVERT_DOT_TO_PDF = "util.dot.to.pdf.enabled";
  public static int ID_LENGTH = 32;

  private static final Logger LOG = LoggerFactory.getLogger( Util.class );
  private static final String HEXES = "0123456789ABCDEF";

  public static final boolean IS_OSX = System.getProperty( "os.name" ).toLowerCase().contains( "Mac OS X".toLowerCase() );
  public static final boolean HAS_DOT_EXEC = IS_OSX && Boolean.getBoolean( CONVERT_DOT_TO_PDF ) && hasDOT();

  public static <K, V> HashMap<K, V> createHashMap()
    {
    return new HashMap<K, V>();
    }

  public static <K, V> boolean reverseMap( Map<V, K> from, Map<K, V> to )
    {
    boolean dupes = false;

    for( Map.Entry<V, K> entry : from.entrySet() )
      dupes |= to.put( entry.getValue(), entry.getKey() ) != null;

    return dupes;
    }

  public static <V> Set<V> createIdentitySet()
    {
    return Collections.<V>newSetFromMap( new IdentityHashMap() );
    }

  public static <V> Set<V> createIdentitySet( Collection<V> collection )
    {
    Set<V> identitySet = createIdentitySet();

    if( collection != null )
      identitySet.addAll( collection );

    return identitySet;
    }

  public static <V> V getFirst( Collection<V> collection )
    {
    if( collection == null || collection.isEmpty() )
      return null;

    return collection.iterator().next();
    }

  public static <V> V getFirst( Iterator<V> iterator )
    {
    if( iterator == null || !iterator.hasNext() )
      return null;

    return iterator.next();
    }

  public static <V> V getLast( Iterator<V> iterator )
    {
    if( iterator == null || !iterator.hasNext() )
      return null;

    V v = iterator.next();

    while( iterator.hasNext() )
      v = iterator.next();

    return v;
    }

  public static <N extends Number> N max( Collection<N> collection )
    {
    return new TreeSet<>( collection ).first();
    }

  public static <N extends Number> N min( Collection<N> collection )
    {
    return new TreeSet<>( collection ).last();
    }

  public static <T> Set<T> narrowSet( Class<T> type, Collection collection )
    {
    return narrowSet( type, collection.iterator() );
    }

  public static <T> Set<T> narrowIdentitySet( Class<T> type, Collection collection )
    {
    return narrowIdentitySet( type, collection.iterator() );
    }

  public static <T> Set<T> narrowSet( Class<T> type, Collection collection, boolean include )
    {
    return narrowSet( type, collection.iterator(), include );
    }

  public static <T> Set<T> narrowIdentitySet( Class<T> type, Collection collection, boolean include )
    {
    return narrowIdentitySet( type, collection.iterator(), include );
    }

  public static <T> Set<T> narrowSet( Class<T> type, Iterator iterator )
    {
    return narrowSet( type, iterator, true );
    }

  public static <T> Set<T> narrowIdentitySet( Class<T> type, Iterator iterator )
    {
    return narrowIdentitySet( type, iterator, true );
    }

  public static <T> Set<T> narrowSet( Class<T> type, Iterator iterator, boolean include )
    {
    return narrowSetInternal( type, iterator, include, new HashSet<T>() );
    }

  public static <T> Set<T> narrowIdentitySet( Class<T> type, Iterator iterator, boolean include )
    {
    return narrowSetInternal( type, iterator, include, Util.<T>createIdentitySet() );
    }

  private static <T> Set<T> narrowSetInternal( Class<T> type, Iterator iterator, boolean include, Set<T> set )
    {
    while( iterator.hasNext() )
      {
      Object o = iterator.next();

      if( type.isInstance( o ) == include )
        set.add( (T) o );
      }

    return set;
    }

  public static <T> boolean contains( Class<T> type, Collection collection )
    {
    return contains( type, collection.iterator() );
    }

  public static <T> boolean contains( Class<T> type, Iterator iterator )
    {
    while( iterator.hasNext() )
      {
      Object o = iterator.next();

      if( type.isInstance( o ) )
        return true;
      }

    return false;
    }

  public static <T> Set<T> differenceIdentity( Set<T> lhs, Set<T> rhs )
    {
    Set<T> diff = createIdentitySet( lhs );

    diff.removeAll( rhs );

    return diff;
    }

  public static synchronized String createUniqueIDWhichStartsWithAChar()
    {
    String value;

    do
      {
      value = createUniqueID();
      }
    while( Character.isDigit( value.charAt( 0 ) ) );

    return value;
    }

  public static synchronized String createUniqueID()
    {
    // creates a cryptographically secure random value
    String value = UUID.randomUUID().toString();
    return value.toUpperCase().replaceAll( "-", "" );
    }

  public static String createID( String rawID )
    {
    return createID( rawID.getBytes() );
    }

  /**
   * Method CreateID returns a HEX hash of the given bytes with length 32 characters long.
   *
   * @param bytes the bytes
   * @return string
   */
  public static String createID( byte[] bytes )
    {
    try
      {
      return getHex( MessageDigest.getInstance( "MD5" ).digest( bytes ) );
      }
    catch( NoSuchAlgorithmException exception )
      {
      throw new RuntimeException( "unable to digest string" );
      }
    }

  public static String getHex( byte[] bytes )
    {
    if( bytes == null )
      return null;

    final StringBuilder hex = new StringBuilder( 2 * bytes.length );

    for( final byte b : bytes )
      hex.append( HEXES.charAt( ( b & 0xF0 ) >> 4 ) ).append( HEXES.charAt( b & 0x0F ) );

    return hex.toString();
    }

  public static byte[] longToByteArray( long value )
    {
    return new byte[]{
      (byte) ( value >> 56 ),
      (byte) ( value >> 48 ),
      (byte) ( value >> 40 ),
      (byte) ( value >> 32 ),
      (byte) ( value >> 24 ),
      (byte) ( value >> 16 ),
      (byte) ( value >> 8 ),
      (byte) value
    };
    }

  public static byte[] intToByteArray( int value )
    {
    return new byte[]{
      (byte) ( value >> 24 ),
      (byte) ( value >> 16 ),
      (byte) ( value >> 8 ),
      (byte) value
    };
    }

  public static <T> T[] copy( T[] source )
    {
    if( source == null )
      return null;

    return Arrays.copyOf( source, source.length );
    }

  public static String unique( String value, String delim )
    {
    String[] split = value.split( delim );

    Set<String> values = new LinkedHashSet<String>();

    Collections.addAll( values, split );

    return join( values, delim );
    }

  /**
   * This method joins the values in the given list with the delim String value.
   *
   * @param list
   * @param delim
   * @return String
   */
  public static String join( int[] list, String delim )
    {
    return join( list, delim, false );
    }

  public static String join( int[] list, String delim, boolean printNull )
    {
    StringBuffer buffer = new StringBuffer();
    int count = 0;

    for( Object s : list )
      {
      if( count != 0 )
        buffer.append( delim );

      if( printNull || s != null )
        buffer.append( s );

      count++;
      }

    return buffer.toString();
    }

  public static String join( String delim, String... strings )
    {
    return join( delim, false, strings );
    }

  public static String join( String delim, boolean printNull, String... strings )
    {
    return join( strings, delim, printNull );
    }

  /**
   * This method joins the values in the given list with the delim String value.
   *
   * @param list
   * @param delim
   * @return a String
   */
  public static String join( Object[] list, String delim )
    {
    return join( list, delim, false );
    }

  public static String join( Object[] list, String delim, boolean printNull )
    {
    return join( list, delim, printNull, 0 );
    }

  public static String join( Object[] list, String delim, boolean printNull, int beginAt )
    {
    return join( list, delim, printNull, beginAt, list.length - beginAt );
    }

  public static String join( Object[] list, String delim, boolean printNull, int beginAt, int length )
    {
    StringBuffer buffer = new StringBuffer();
    int count = 0;

    for( int i = beginAt; i < beginAt + length; i++ )
      {
      Object s = list[ i ];
      if( count != 0 )
        buffer.append( delim );

      if( printNull || s != null )
        buffer.append( s );

      count++;
      }

    return buffer.toString();
    }

  public static String join( Iterable iterable, String delim, boolean printNull )
    {
    int count = 0;

    StringBuilder buffer = new StringBuilder();

    for( Object s : iterable )
      {
      if( count != 0 )
        buffer.append( delim );

      if( printNull || s != null )
        buffer.append( s );

      count++;
      }

    return buffer.toString();
    }

  /**
   * This method joins each value in the collection with a tab character as the delimiter.
   *
   * @param collection
   * @return a String
   */
  public static String join( Collection collection )
    {
    return join( collection, "\t" );
    }

  /**
   * This method joins each valuein the collection with the given delimiter.
   *
   * @param collection
   * @param delim
   * @return a String
   */
  public static String join( Collection collection, String delim )
    {
    return join( collection, delim, false );
    }

  public static String join( Collection collection, String delim, boolean printNull )
    {
    StringBuffer buffer = new StringBuffer();

    join( buffer, collection, delim, printNull );

    return buffer.toString();
    }

  /**
   * This method joins each value in the collection with the given delimiter. All results are appended to the
   * given {@link StringBuffer} instance.
   *
   * @param buffer
   * @param collection
   * @param delim
   */
  public static void join( StringBuffer buffer, Collection collection, String delim )
    {
    join( buffer, collection, delim, false );
    }

  public static void join( StringBuffer buffer, Collection collection, String delim, boolean printNull )
    {
    int count = 0;

    for( Object s : collection )
      {
      if( count != 0 )
        buffer.append( delim );

      if( printNull || s != null )
        buffer.append( s );

      count++;
      }
    }

  public static <T> List<T> split( Class<T> type, String values )
    {
    return split( type, ",", values );
    }

  public static <T> List<T> split( Class<T> type, String delim, String values )
    {
    List<T> results = new ArrayList<>();

    if( values == null )
      return results;

    String[] split = values.split( delim );

    for( String value : split )
      results.add( Coercions.<T>coerce( value, type ) );

    return results;
    }

  public static String[] removeNulls( String... strings )
    {
    List<String> list = new ArrayList<String>();

    for( String string : strings )
      {
      if( string != null )
        list.add( string );
      }

    return list.toArray( new String[ list.size() ] );
    }

  public static Collection<String> quote( Collection<String> collection, String quote )
    {
    List<String> list = new ArrayList<String>();

    for( String string : collection )
      list.add( quote + string + quote );

    return list;
    }

  public static String print( Collection collection, String delim )
    {
    StringBuffer buffer = new StringBuffer();

    print( buffer, collection, delim );

    return buffer.toString();
    }

  public static void print( StringBuffer buffer, Collection collection, String delim )
    {
    int count = 0;

    for( Object s : collection )
      {
      if( count != 0 )
        buffer.append( delim );

      buffer.append( "[" );
      buffer.append( s );
      buffer.append( "]" );

      count++;
      }
    }

  /**
   * This method attempts to remove any username and password from the given url String.
   *
   * @param url
   * @return a String
   */
  public static String sanitizeUrl( String url )
    {
    if( url == null )
      return null;

    return url.replaceAll( "(?<=//).*:.*@", "" );
    }

  /**
   * This method attempts to remove duplicate consecutive forward slashes from the given url.
   *
   * @param url
   * @return a String
   */
  public static String normalizeUrl( String url )
    {
    if( url == null )
      return null;

    return url.replaceAll( "([^:]/)/{2,}", "$1/" );
    }

  /**
   * This method returns the {@link Object#toString()} of the given object, or an empty String if the object
   * is null.
   *
   * @param object
   * @return a String
   */
  public static String toNull( Object object )
    {
    if( object == null )
      return "";

    return object.toString();
    }

  /**
   * This method truncates the given String value to the given size, but appends an ellipse ("...") if the
   * String is larger than maxSize.
   *
   * @param string
   * @param maxSize
   * @return a String
   */
  public static String truncate( String string, int maxSize )
    {
    string = toNull( string );

    if( string.length() <= maxSize )
      return string;

    return String.format( "%s...", string.subSequence( 0, maxSize - 3 ) );
    }

  public static void printGraph( String filename, SimpleDirectedGraph graph )
    {
    try
      {
      new File( filename ).getParentFile().mkdirs();
      Writer writer = new FileWriter( filename );

      try
        {
        printGraph( writer, graph );
        }
      finally
        {
        writer.close();
        }
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to {}, with exception: {}", filename, exception );
      }
    }

  @SuppressWarnings({"unchecked"})
  private static void printGraph( Writer writer, SimpleDirectedGraph graph )
    {
    DOTExporter dot = new DOTExporter( new IntegerNameProvider(), new VertexNameProvider()
      {
      public String getVertexName( Object object )
        {
        if( object == null )
          return "none";

        return object.toString().replaceAll( "\"", "\'" );
        }
      }, new EdgeNameProvider<Object>()
      {
      public String getEdgeName( Object object )
        {
        if( object == null )
          return "none";

        return object.toString().replaceAll( "\"", "\'" );
        }
      }
    );

    dot.export( writer, graph );
    }

  /**
   * This method removes all nulls from the given List.
   *
   * @param list
   */
  @SuppressWarnings({"StatementWithEmptyBody"})
  public static void removeAllNulls( List list )
    {
    while( list.remove( null ) )
      ;
    }

  public static void writeDOT( Writer writer, DirectedGraph graph, IntegerNameProvider vertexIdProvider, VertexNameProvider vertexNameProvider, EdgeNameProvider edgeNameProvider )
    {
    new DOTExporter( vertexIdProvider, vertexNameProvider, edgeNameProvider ).export( writer, graph );
    }

  public static void writeDOT( Writer writer, DirectedGraph graph, IntegerNameProvider vertexIdProvider, VertexNameProvider vertexNameProvider, EdgeNameProvider edgeNameProvider,
                               ComponentAttributeProvider vertexAttributeProvider, ComponentAttributeProvider edgeAttributeProvider )
    {
    new DOTExporter( vertexIdProvider, vertexNameProvider, edgeNameProvider, vertexAttributeProvider, edgeAttributeProvider ).export( writer, graph );
    }

  public static boolean isEmpty( String string )
    {
    return string == null || string.isEmpty();
    }

  private static String[] findSplitName( String path )
    {
    String separator = "/";

    if( path.lastIndexOf( "/" ) < path.lastIndexOf( "\\" ) )
      separator = "\\\\";

    String[] split = path.split( separator );

    path = split[ split.length - 1 ];

    path = path.substring( 0, path.lastIndexOf( '.' ) ); // remove .jar

    return path.split( "-(?=\\d)", 2 );
    }

  public static String findVersion( String path )
    {
    if( path == null || path.isEmpty() )
      return null;

    String[] split = findSplitName( path );

    if( split.length == 2 )
      return split[ 1 ];

    return null;
    }

  public static String findName( String path )
    {
    if( path == null || path.isEmpty() )
      return null;

    String[] split = findSplitName( path );

    if( split.length == 0 )
      return null;

    return split[ 0 ];
    }

  public static long getSourceModified( Object confCopy, Iterator<Tap> values, long sinkModified ) throws IOException
    {
    long sourceModified = 0;

    while( values.hasNext() )
      {
      Tap source = values.next();

      if( source instanceof MultiSourceTap )
        return getSourceModified( confCopy, ( (MultiSourceTap) source ).getChildTaps(), sinkModified );

      sourceModified = source.getModifiedTime( confCopy );

      // source modified returns zero if does not exist
      // this should minimize number of times we touch any file meta-data server
      if( sourceModified == 0 && !source.resourceExists( confCopy ) )
        throw new FlowException( "source does not exist: " + source );

      if( sinkModified < sourceModified )
        return sourceModified;
      }

    return sourceModified;
    }

  public static long getSinkModified( Object config, Collection<Tap> sinks ) throws IOException
    {
    long sinkModified = Long.MAX_VALUE;

    for( Tap sink : sinks )
      {
      if( sink.isReplace() || sink.isUpdate() )
        sinkModified = -1L;
      else
        {
        if( !sink.resourceExists( config ) )
          sinkModified = 0L;
        else
          sinkModified = Math.min( sinkModified, sink.getModifiedTime( config ) ); // return youngest mod date
        }
      }
    return sinkModified;
    }

  public static String getTypeName( Type type )
    {
    if( type == null )
      return null;

    return type instanceof Class ? ( (Class) type ).getCanonicalName() : type.toString();
    }

  public static String getSimpleTypeName( Type type )
    {
    if( type == null )
      return null;

    return type instanceof Class ? ( (Class) type ).getSimpleName() : type.toString();
    }

  public static String[] typeNames( Type[] types )
    {
    String[] names = new String[ types.length ];

    for( int i = 0; i < types.length; i++ )
      names[ i ] = getTypeName( types[ i ] );

    return names;
    }

  public static String[] simpleTypeNames( Type[] types )
    {
    String[] names = new String[ types.length ];

    for( int i = 0; i < types.length; i++ )
      names[ i ] = getSimpleTypeName( types[ i ] );

    return names;
    }

  public static boolean containsNull( Object[] values )
    {
    for( Object value : values )
      {
      if( value == null )
        return true;
      }

    return false;
    }

  public static void safeSleep( long durationMillis )
    {
    try
      {
      Thread.sleep( durationMillis );
      }
    catch( InterruptedException exception )
      {
      // do nothing
      }
    }

  public static void writePDF( String path )
    {
    if( !HAS_DOT_EXEC )
      return;

    // dot *.dot -Tpdf -O -Nshape=box
    File file = new File( path );
    execProcess( file.getParentFile(), "dot", file.getName(), "-Tpdf", "-O" );
    }

  static boolean hasDOT()
    {
    return execProcess( null, "which", "dot" ) == 0;
    }

  public static int execProcess( File parentFile, String... command )
    {
    try
      {
      String commandLine = join( command, " " );

      LOG.debug( "command: {}", commandLine );

      Process process = Runtime.getRuntime().exec( commandLine, null, parentFile );

      int result = process.waitFor();

      BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) );

      String line = reader.readLine();

      while( line != null )
        {
        LOG.warn( "{} stdout returned: {}", command[ 0 ], line );
        line = reader.readLine();
        }

      reader = new BufferedReader( new InputStreamReader( process.getErrorStream() ) );

      line = reader.readLine();

      while( line != null )
        {
        LOG.warn( "{} stderr returned: {}", command[ 0 ], line );
        line = reader.readLine();
        }

      return result;
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to exec " + command[ 0 ], exception );
      }
    catch( InterruptedException exception )
      {
      LOG.warn( "interrupted exec " + command[ 0 ], exception );
      }

    return Integer.MIN_VALUE;
    }

  public static String formatDurationFromMillis( long duration )
    {
    if( duration / 1000 / 60 / 60 / 24 > 0.0 )
      return formatDurationDHMSms( duration );
    if( duration / 1000 / 60 / 60 > 0.0 )
      return formatDurationHMSms( duration );
    else
      return formatDurationMSms( duration );
    }

  public static String formatDurationMSms( long duration )
    {
    long ms = duration % 1000;
    long durationSeconds = duration / 1000;
    long seconds = durationSeconds % 60;
    long minutes = durationSeconds / 60;

    return String.format( "%02d:%02d.%03d", minutes, seconds, ms );
    }

  public static String formatDurationHMSms( long duration )
    {
    long ms = duration % 1000;
    long durationSeconds = duration / 1000;
    long seconds = durationSeconds % 60;
    long minutes = ( durationSeconds / 60 ) % 60;
    long hours = durationSeconds / 60 / 60;

    return String.format( "%02d:%02d:%02d.%03d", hours, minutes, seconds, ms );
    }

  public static String formatDurationDHMSms( long duration )
    {
    long ms = duration % 1000;
    long durationSeconds = duration / 1000;
    long seconds = durationSeconds % 60;
    long minutes = ( durationSeconds / 60 ) % 60;
    long hours = ( durationSeconds / 60 / 60 ) % 24;
    long days = durationSeconds / 60 / 60 / 24;

    return String.format( "%02d:%02d:%02d:%02d.%03d", days, hours, minutes, seconds, ms );
    }

  /**
   * Converts a given comma separated String of Exception names into a List of classes.
   * ClassNotFound exceptions are ignored if no warningMessage is given, otherwise logged as a warning.
   *
   * @param classNames A comma separated String of Exception names.
   * @return List of Exception classes.
   */
  public static Set<Class<? extends Exception>> asClasses( String classNames, String warningMessage )
    {
    Set<Class<? extends Exception>> exceptionClasses = new HashSet<Class<? extends Exception>>();
    String[] split = classNames.split( "," );

    // possibly user provided type, load from context
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    for( String className : split )
      {
      if( className != null )
        className = className.trim();

      if( isEmpty( className ) )
        continue;

      try
        {
        Class<? extends Exception> exceptionClass = contextClassLoader.loadClass( className ).asSubclass( Exception.class );

        exceptionClasses.add( exceptionClass );
        }
      catch( ClassNotFoundException exception )
        {
        if( !Util.isEmpty( warningMessage ) )
          LOG.warn( "{}: {}", warningMessage, className );
        }
      }

    return exceptionClasses;
    }

  public static Boolean submitWithTimeout( Callable<Boolean> task, int timeout, TimeUnit timeUnit ) throws Exception
    {
    ExecutorService executor = Executors.newFixedThreadPool( 1 );

    Future<Boolean> future = executor.submit( task );

    executor.shutdown();

    try
      {
      return future.get( timeout, timeUnit );
      }
    catch( TimeoutException exception )
      {
      future.cancel( true );
      }
    catch( ExecutionException exception )
      {
      Throwable cause = exception.getCause();

      if( cause instanceof RuntimeException )
        throw (RuntimeException) cause;

      throw (Exception) cause;
      }

    return null;
    }

  public interface RetryOperator<T>
    {
    T operate() throws Exception;

    boolean rethrow( Exception exception );
    }

  public static <T> T retry( Logger logger, int retries, int secondsDelay, String message, RetryOperator<T> operator ) throws Exception
    {
    Exception saved = null;

    for( int i = 0; i < retries; i++ )
      {
      try
        {
        return operator.operate();
        }
      catch( Exception exception )
        {
        if( operator.rethrow( exception ) )
          {
          logger.warn( message + ", but not retrying", exception );

          throw exception;
          }

        saved = exception;

        logger.warn( message + ", attempt: " + ( i + 1 ), exception );

        try
          {
          Thread.sleep( secondsDelay * 1000 );
          }
        catch( InterruptedException exception1 )
          {
          // do nothing
          }
        }
      }

    logger.warn( message + ", done retrying after attempts: " + retries, saved );

    throw saved;
    }

  public static Object createProtectedObject( Class type, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Constructor constructor = type.getDeclaredConstructor( parameterTypes );

      constructor.setAccessible( true );

      return constructor.newInstance( parameters );
      }
    catch( Exception exception )
      {
      LOG.error( "unable to instantiate type: {}, with exception: {}", type.getName(), exception );

      throw new FlowException( "unable to instantiate type: " + type.getName(), exception );
      }
    }

  public static boolean hasClass( String typeString )
    {
    try
      {
      Util.class.getClassLoader().loadClass( typeString );

      return true;
      }
    catch( ClassNotFoundException exception )
      {
      return false;
      }
    }

  public static <T> T newInstance( String className, Object... parameters )
    {
    try
      {
      Class<T> type = (Class<T>) Util.class.getClassLoader().loadClass( className );

      return newInstance( type, parameters );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + className, exception );
      }
    }

  public static <T> T newInstance( Class<T> target, Object... parameters )
    {
    // using Expression makes sure that constructors using sub-types properly work, otherwise we get a
    // NoSuchMethodException.
    Expression expr = new Expression( target, "new", parameters );

    try
      {
      return (T) expr.getValue();
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to create new instance: " + target.getName() + "(" + Arrays.toString( parameters ) + ")", exception );
      }
    }

  public static Object invokeStaticMethod( String typeString, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    Class type = loadClass( typeString );

    return invokeStaticMethod( type, methodName, parameters, parameterTypes );
    }

  public static Class<?> loadClass( String typeString )
    {
    try
      {
      return Thread.currentThread().getContextClassLoader().loadClass( typeString );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + typeString, exception );
      }
    }

  public static Class<?> loadClassSafe( String typeString )
    {
    try
      {
      return Thread.currentThread().getContextClassLoader().loadClass( typeString );
      }
    catch( ClassNotFoundException exception )
      {
      return null;
      }
    }

  public static Object invokeStaticMethod( Class type, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Method method = type.getDeclaredMethod( methodName, parameterTypes );

      method.setAccessible( true );

      return method.invoke( null, parameters );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to invoke static method: " + type.getName() + "." + methodName, exception );
      }
    }

  public static boolean hasInstanceMethod( Object target, String methodName, Class[] parameterTypes )
    {
    try
      {
      return target.getClass().getMethod( methodName, parameterTypes ) != null;
      }
    catch( NoSuchMethodException exception )
      {
      return false;
      }
    }

  public static Object invokeInstanceMethodSafe( Object target, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      return invokeInstanceMethod( target, methodName, parameters, parameterTypes );
      }
    catch( Exception exception )
      {
      return null;
      }
    }

  public static Object invokeInstanceMethod( Object target, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Method method = target.getClass().getMethod( methodName, parameterTypes );

      method.setAccessible( true );

      return method.invoke( target, parameters );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to invoke instance method: " + target.getClass().getName() + "." + methodName, exception );
      }
    }

  public static <R> R returnInstanceFieldIfExistsSafe( Object target, String fieldName )
    {
    try
      {
      return returnInstanceFieldIfExists( target, fieldName );
      }
    catch( Exception exception )
      {
      // do nothing
      return null;
      }
    }

  public static Object invokeConstructor( String className, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Class type = Util.class.getClassLoader().loadClass( className );

      return invokeConstructor( type, parameters, parameterTypes );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + className, exception );
      }
    }

  public static <T> T invokeConstructor( Class<T> target, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Constructor<T> constructor = target.getConstructor( parameterTypes );

      constructor.setAccessible( true );

      return constructor.newInstance( parameters );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to create new instance: " + target.getName() + "(" + Arrays.toString( parameters ) + ")", exception );
      }
    }

  public static <R> R returnInstanceFieldIfExists( Object target, String fieldName )
    {
    try
      {
      Class<?> type = target.getClass();
      Field field = getDeclaredField( fieldName, type );

      field.setAccessible( true );

      return (R) field.get( target );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to get instance field: " + target.getClass().getName() + "." + fieldName, exception );
      }
    }

  public static <R> boolean setInstanceFieldIfExistsSafe( Object target, String fieldName, R value )
    {
    try
      {
      setInstanceFieldIfExists( target, fieldName, value );
      }
    catch( Exception exception )
      {
      return false;
      }

    return true;
    }

  public static <R> void setInstanceFieldIfExists( Object target, String fieldName, R value )
    {
    try
      {
      Class<?> type = target.getClass();
      Field field = getDeclaredField( fieldName, type );

      field.setAccessible( true );

      field.set( target, value );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to set instance field: " + target.getClass().getName() + "." + fieldName, exception );
      }
    }

  private static Field getDeclaredField( String fieldName, Class<?> type )
    {
    if( type == Object.class )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "did not find {} field on {}", fieldName, type.getName() );

      return null;
      }

    try
      {
      return type.getDeclaredField( fieldName );
      }
    catch( NoSuchFieldException exception )
      {
      return getDeclaredField( fieldName, type.getSuperclass() );
      }
    }

  public static String makePath( String prefix, String name )
    {
    if( name == null || name.isEmpty() )
      throw new IllegalArgumentException( "name may not be null or empty " );

    if( prefix == null || prefix.isEmpty() )
      prefix = Long.toString( (long) ( Math.random() * 10000000000L ) );

    name = cleansePathName( name.substring( 0, name.length() < 25 ? name.length() : 25 ) );

    return prefix + "/" + name + "/";
    }

  public static String cleansePathName( String name )
    {
    return name.replaceAll( "\\s+|\\*|\\+|/+", "_" );
    }

  public static Class findMainClass( Class defaultType, String packageExclude )
    {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

    for( StackTraceElement stackTraceElement : stackTrace )
      {
      if( stackTraceElement.getMethodName().equals( "main" ) && !stackTraceElement.getClassName().startsWith( packageExclude ) )
        {
        try
          {
          LOG.info( "resolving application jar from found main method on: {}", stackTraceElement.getClassName() );

          return Thread.currentThread().getContextClassLoader().loadClass( stackTraceElement.getClassName() );
          }
        catch( ClassNotFoundException exception )
          {
          LOG.warn( "unable to load class while discovering application jar: {}", stackTraceElement.getClassName(), exception );
          }
        }
      }

    LOG.info( "using default application jar, may cause class not found exceptions on the cluster" );

    return defaultType;
    }

  public static String findContainingJar( Class<?> type )
    {
    ClassLoader classLoader = type.getClassLoader();

    String classFile = type.getName().replaceAll( "\\.", "/" ) + ".class";

    try
      {
      for( Enumeration<URL> iterator = classLoader.getResources( classFile ); iterator.hasMoreElements(); )
        {
        URL url = iterator.nextElement();

        if( !"jar".equals( url.getProtocol() ) )
          continue;

        String path = url.getPath();

        if( path.startsWith( "file:" ) )
          path = path.substring( "file:".length() );

        path = URLDecoder.decode( path, "UTF-8" );

        return path.replaceAll( "!.*$", "" );
        }
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }

    return null;
    }

  public static boolean containsWhitespace( String string )
    {
    return Pattern.compile( "\\s" ).matcher( string ).find();
    }

  public static String parseHostname( String uri )
    {
    if( isEmpty( uri ) )
      return null;

    String[] parts = uri.split( "://", 2 );
    String result;

    // missing protocol
    result = parts[ parts.length - 1 ];

    // user:pass@hostname:port/stuff
    parts = result.split( "/", 2 );
    result = parts[ 0 ];

    // user:pass@hostname:port
    parts = result.split( "@", 2 );
    result = parts[ parts.length - 1 ];

    // hostname:port
    parts = result.split( ":", 2 );
    result = parts[ 0 ];

    return result;
    }
  }
