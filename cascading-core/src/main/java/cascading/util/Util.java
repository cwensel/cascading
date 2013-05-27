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

package cascading.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.planner.Scope;
import cascading.operation.BaseOperation;
import cascading.operation.Operation;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.MatrixExporter;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class Util provides reusable operations. */
public class Util
  {
  public static int ID_LENGTH = 32;

  private static final Logger LOG = LoggerFactory.getLogger( Util.class );
  private static final String HEXES = "0123456789ABCDEF";

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

  private static String getHex( byte[] bytes )
    {
    if( bytes == null )
      return null;

    final StringBuilder hex = new StringBuilder( 2 * bytes.length );

    for( final byte b : bytes )
      hex.append( HEXES.charAt( ( b & 0xF0 ) >> 4 ) ).append( HEXES.charAt( b & 0x0F ) );

    return hex.toString();
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

  public static String printGraph( SimpleDirectedGraph graph )
    {
    StringWriter writer = new StringWriter();

    printGraph( writer, graph );

    return writer.toString();
    }

  public static void printGraph( PrintStream out, SimpleDirectedGraph graph )
    {
    PrintWriter printWriter = new PrintWriter( out );

    printGraph( printWriter, graph );
    }

  public static void printGraph( String filename, SimpleDirectedGraph graph )
    {
    try
      {
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

  public static void printMatrix( PrintStream out, SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    new MatrixExporter().exportAdjacencyMatrix( new PrintWriter( out ), graph );
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

  public static String formatTrace( Scheme scheme, String message )
    {
    if( scheme == null )
      return message;

    String trace = scheme.getTrace();

    if( trace == null )
      return message;

    return "[" + truncate( scheme.toString(), 25 ) + "][" + trace + "] " + message;
    }


  /**
   * Method formatRawTrace does not include the pipe name
   *
   * @param pipe    of type Pipe
   * @param message of type String
   * @return String
   */
  public static String formatRawTrace( Pipe pipe, String message )
    {
    if( pipe == null )
      return message;

    String trace = pipe.getTrace();

    if( trace == null )
      return message;

    return "[" + trace + "] " + message;
    }

  public static String formatTrace( Pipe pipe, String message )
    {
    if( pipe == null )
      return message;

    String trace = pipe.getTrace();

    if( trace == null )
      return message;

    return "[" + truncate( pipe.getName(), 25 ) + "][" + trace + "] " + message;
    }

  public static String formatTrace( Tap tap, String message )
    {
    if( tap == null )
      return message;

    String trace = tap.getTrace();

    if( trace == null )
      return message;

    return "[" + truncate( tap.toString(), 25 ) + "][" + trace + "] " + message;
    }

  public static String formatTrace( Operation operation, String message )
    {
    if( !( operation instanceof BaseOperation ) )
      return message;

    String trace = ( (BaseOperation) operation ).getTrace();

    if( trace == null )
      return message;

    return "[" + trace + "] " + message;
    }

  public static String captureDebugTrace( Class type )
    {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    Package packageName = type.getPackage();
    String typeName = type.getName();

    boolean skip = true;

    for( StackTraceElement stackTraceElement : stackTrace )
      {
      String className = stackTraceElement.getClassName();

      if( skip )
        {
        skip = !className.equals( typeName );
        continue;
        }
      else
        {
        if( packageName != null && stackTraceElement.getClassName().equals( typeName ) )
          {
          continue;
          }
        }

      return stackTraceElement.toString();
      }

    return null;
    }

  public static void writeDOT( Writer writer, SimpleDirectedGraph graph, IntegerNameProvider vertexIdProvider, VertexNameProvider vertexNameProvider, EdgeNameProvider edgeNameProvider )
    {
    new DOTExporter( vertexIdProvider, vertexNameProvider, edgeNameProvider ).export( writer, graph );
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

  public static String[] typeNames( Type[] types )
    {
    String[] names = new String[ types.length ];

    for( int i = 0; i < types.length; i++ )
      names[ i ] = getTypeName( types[ i ] );

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

  public static Object invokeStaticMethod( String typeString, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Class type = Util.class.getClassLoader().loadClass( typeString );

      return invokeStaticMethod( type, methodName, parameters, parameterTypes );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + typeString, exception );
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

  @Deprecated
  public static String makeTempPath( String name )
    {
    if( name == null || name.isEmpty() )
      throw new IllegalArgumentException( "name may not be null or empty " );

    name = cleansePathName( name.substring( 0, name.length() < 25 ? name.length() : 25 ) );

    return name + "/" + (int) ( Math.random() * 100000 ) + "/";
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
  }
