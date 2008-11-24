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

package cascading.flow;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.CascadingException;
import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Use the FlowConnector to link sink and source {@link Tap} instances with an assembly of {@link Pipe} instances into
 * an executable {@link Flow}.
 * <p/>
 * FlowConnector invokes a planner for the target execution environment. Currently only {@link cascading.flow.MultiMapReducePlanner}
 * is supported. If you have just one custom Hadoop job to execute, see {@link cascading.flow.MapReduceFlow}.
 * <p/>
 * The FlowConnector and resulting Flow can be configured via a Map of properties given on the constructor. This properties
 * map can be populated through static methods on FlowConnector and MultiMapReducePlanner.
 * <p/>
 * Most applications will need to call {@link #setApplicationJarClass(java.util.Map, Class)} or {@link #setApplicationJarPath(java.util.Map, String)}
 * so that the correct application jar file is passed through to all child processes.
 * <p/>
 * Note that Map<Object,Object> is compatible with the {@link Properties} class, so properties can be loaded at
 * runtime from a configuation file.
 * <p/>
 * By default, all {@link Assertion} are planned into the resulting Flow instance. This can be
 * changed by calling {@link #setAssertionLevel(java.util.Map, cascading.operation.AssertionLevel)}.
 * <p/>
 * <strong>Properties</strong><br/>
 * <ul>
 * <li>cascading.flowconnector.appjar.class</li>
 * <li>cascading.flowconnector.appjar.path</li>
 * <li>cascading.flowconnector.assertionlevel</li>
 * <li>cascading.flowconnector.intermediateschemeclass</li>
 * </ul>
 *
 * @see cascading.flow.MapReduceFlow
 */
public class FlowConnector
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FlowConnector.class );

  /** Field properties */
  private Map<Object, Object> properties;

  public static void setAssertionLevel( Map<Object, Object> properties, AssertionLevel assertionLevel )
    {
    if( assertionLevel != null )
      properties.put( "cascading.flowconnector.assertionlevel", assertionLevel.toString() );
    }

  public static AssertionLevel getAssertionLevel( Map<Object, Object> properties )
    {
    String assertionLevel = Util.getProperty( properties, "cascading.flowconnector.assertionlevel", AssertionLevel.STRICT.name() );

    return AssertionLevel.valueOf( assertionLevel );
    }

  public static void setIntermediateSchemeClass( Map<Object, Object> properties, Class intermediateSchemeClass )
    {
    properties.put( "cascading.flowconnector.intermediateschemeclass", intermediateSchemeClass );
    }

  public static void setIntermediateSchemeClass( Map<Object, Object> properties, String intermediateSchemeClass )
    {
    properties.put( "cascading.flowconnector.intermediateschemeclass", intermediateSchemeClass );
    }

  public static Class getIntermediateSchemeClass( Map<Object, Object> properties )
    {
    // supporting stuffed classes to overcome classloading issue
    Object type = Util.getProperty( properties, "cascading.flowconnector.intermediateschemeclass", (Object) null );

    if( type == null )
      return SequenceFile.class;

    if( type instanceof Class )
      return (Class) type;

    try
      {
      return FlowConnector.class.getClassLoader().loadClass( type.toString() );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + type.toString(), exception );
      }
    }

  /**
   * Method setJarClass is used to set the application jar file.
   *
   * @param properties of type Map
   * @param type       of type Class
   */
  public static void setApplicationJarClass( Map<Object, Object> properties, Class type )
    {
    if( type != null )
      properties.put( "cascading.flowconnector.appjar.class", type );
    }

  public static Class getApplicationJarClass( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.flowconnector.appjar.class", (Class) null );
    }

  /**
   * Method setJarClass is used to set the application jar file.
   *
   * @param properties of type Map
   * @param path       of type String
   */
  public static void setApplicationJarPath( Map<Object, Object> properties, String path )
    {
    if( path != null )
      properties.put( "cascading.flowconnector.appjar.path", path );
    }

  public static String getApplicationJarPath( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.flowconnector.appjar.path", (String) null );
    }

  /** Constructor FlowConnector creates a new FlowConnector instance. */
  public FlowConnector()
    {
    }

  /**
   * Constructor FlowConnector creates a new FlowConnector instance using the given {@link Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new {@link JobConf} instance.
   *
   * @param properties of type Properties
   */
  public FlowConnector( Map<Object, Object> properties )
    {
    this.properties = properties;
    }

  /**
   * Method getProperties returns the properties of this FlowConnector object. The returned Map instance
   * is immutable to prevent changes to the underlying property values in this FlowConnector instance.
   *
   * @return the properties (type Map<Object, Object>) of this FlowConnector object.
   */
  public Map<Object, Object> getProperties()
    {
    return Collections.unmodifiableMap( properties );
    }

  /**
   * Method connect links the given source and sink Taps to the given pipe assembly.
   *
   * @param source of type Tap
   * @param sink   of type Tap
   * @param pipe   of type Pipe
   * @return Flow
   */
  public Flow connect( Tap source, Tap sink, Pipe pipe )
    {
    return connect( null, source, sink, pipe );
    }

  /**
   * Method connect links the given source and sink Taps to the given pipe assembly.
   *
   * @param name   of type String
   * @param source of type Tap
   * @param sink   of type Tap
   * @param pipe   of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Pipe pipe )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( pipe.getHeads()[ 0 ].getName(), source );

    return connect( name, sources, sink, pipe );
    }

  /**
   * Method connect links the given source, sink, and trap Taps to the given pipe assembly. The given trap will
   * be linked to the assembly head along with the source.
   *
   * @param name   of type String
   * @param source of type Tap
   * @param sink   of type Tap
   * @param trap   of type Tap
   * @param pipe   of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Tap trap, Pipe pipe )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( pipe.getHeads()[ 0 ].getName(), source );

    Map<String, Tap> traps = new HashMap<String, Tap>();

    traps.put( pipe.getHeads()[ 0 ].getName(), trap );

    return connect( name, sources, sink, traps, pipe );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   *
   * @param sources of type Map<String, Tap>
   * @param sink    of type Tap
   * @param pipe    of type Pipe
   * @return Flow
   */
  public Flow connect( Map<String, Tap> sources, Tap sink, Pipe pipe )
    {
    return connect( null, sources, sink, pipe );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   *
   * @param name    of type String
   * @param sources of type Map<String, Tap>
   * @param sink    of type Tap
   * @param pipe    of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Tap sink, Pipe pipe )
    {
    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( pipe.getName(), sink );

    return connect( name, sources, sinks, pipe );
    }

  /**
   * Method connect links the named source and trap Taps and sink Tap to the given pipe assembly.
   *
   * @param name    of type String
   * @param sources of type Map<String, Tap>
   * @param sink    of type Tap
   * @param traps   of type Map<String, Tap>
   * @param pipe    of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Tap sink, Map<String, Tap> traps, Pipe pipe )
    {
    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( pipe.getName(), sink );

    return connect( name, sources, sinks, traps, pipe );
    }

  /**
   * Method connect links the named trap Taps, source and sink Tap to the given pipe assembly.
   *
   * @param name    of type String
   * @param source  of type Tap
   * @param sink    of type Tap
   * @param traps   of type Map<String, Tap>
   * @param pipe    of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Map<String, Tap> traps, Pipe pipe )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( pipe.getHeads()[ 0 ].getName(), source );

    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( pipe.getName(), sink );

    return connect( name, sources, sinks, traps, pipe );
    }

  /**
   * Method connect links the named trap Taps, source and sink Tap to the given pipe assembly.
   *
   * @param name    of type String
   * @param source  of type Tap
   * @param sink    of type Tap
   * @param traps   of type Map<String, Tap>
   * @param pipe    of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Map<String, Tap> traps, Pipe pipe )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( pipe.getHeads()[ 0 ].getName(), source );

    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( pipe.getName(), sink );

    return connect( name, sources, sinks, traps, pipe );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param source of type Tap
   * @param sinks  of type Map<String, Tap>
   * @param pipes  of type Pipe...
   * @return Flow
   */
  public Flow connect( Tap source, Map<String, Tap> sinks, Pipe... pipes )
    {
    return connect( null, source, sinks, pipes );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param name   of type String
   * @param source of type Tap
   * @param sinks  of type Map<String, Tap>
   * @param pipes  of type Pipe...
   * @return Flow
   */
  public Flow connect( String name, Tap source, Map<String, Tap> sinks, Pipe... pipes )
    {
    Set<Pipe> heads = new HashSet<Pipe>();

    for( Pipe pipe : pipes )
      Collections.addAll( heads, pipe.getHeads() );

    if( heads.isEmpty() )
      throw new IllegalArgumentException( "no pipe instance found" );

    if( heads.size() != 1 )
      throw new IllegalArgumentException( "there may be only 1 head pipe instance, found " + heads.size() );

    Map<String, Tap> sources = new HashMap<String, Tap>();

    for( Pipe pipe : heads )
      sources.put( pipe.getName(), source );

    return connect( name, sources, sinks, pipes );
    }


  /**
   * Method connect links the named sources and sinks to the given pipe assembly.
   *
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @param pipes   of type Pipe...
   * @return Flow
   */
  public Flow connect( Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... pipes )
    {
    return connect( null, sources, sinks, pipes );
    }

  /**
   * Method connect links the named sources and sinks to the given pipe assembly.
   *
   * @param name    of type String
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @param pipes   of type Pipe...
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... pipes )
    {
    return connect( name, sources, sinks, new HashMap<String, Tap>(), pipes );
    }

  /**
   * Method connect links the named sources, sinks and traps to the given pipe assembly.
   *
   * @param name    of type String
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @param traps   of type Map<String, Tap>
   * @param pipes   of type Pipe...
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, Pipe... pipes )
    {
    name = name == null ? makeName( pipes ) : name;

    // choose appropriate planner (when there is more than one)
    return new MultiMapReducePlanner( properties ).buildFlow( name, pipes, sources, sinks, traps );
    }

  /////////
  // UTIL
  /////////

  private String makeName( Pipe[] pipes )
    {
    String[] names = new String[pipes.length];

    for( int i = 0; i < pipes.length; i++ )
      names[ i ] = pipes[ i ].getName();

    String name = Util.join( names, "+" );

    if( name.length() > 32 )
      name = name.substring( 0, 32 );

    return name;
    }
  }
