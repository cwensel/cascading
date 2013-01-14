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

package cascading.flow;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlatformInfo;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.property.PropertyUtil;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.util.Util;

import static cascading.flow.FlowDef.flowDef;

/**
 * Class FlowConnector is the base class for all platform planners.
 * <p/>
 * See the {@link FlowDef} class for a fluent way to define a new Flow.
 * <p/>
 * Use the FlowConnector to link source and sink {@link Tap} instances with an assembly of {@link Pipe} instances into
 * an executable {@link cascading.flow.Flow}.
 * <p/>
 * FlowConnector invokes a planner for the target execution environment.
 * <p/>
 * For executing Flows in local memory against local files, see {@link cascading.flow.local.LocalFlowConnector}.
 * <p/>
 * For Apache Hadoop, see the {@link cascading.flow.hadoop.HadoopFlowConnector}.
 * Or if you have a pre-existing custom Hadoop job to execute, see {@link cascading.flow.hadoop.MapReduceFlow}, which
 * doesn't require a planner.
 * <p/>
 * Note that all {@code connect} methods take a single {@code tail} or an array of {@code tail} Pipe instances. "tail"
 * refers to the last connected Pipe instances in a pipe-assembly. Pipe-assemblies are graphs of object with "heads"
 * and "tails". From a given "tail", all connected heads can be found, but not the reverse. So "tails" must be
 * supplied by the user.
 * <p/>
 * The FlowConnector and the underlying execution framework (Hadoop or local mode) can be configured via a
 * {@link Map} or {@link Properties} instance given to the constructor.
 * <p/>
 * This properties map must be populated before constructing a FlowConnector instance. Many planner specific
 * properties can be set through the {@link FlowConnectorProps} fluent interface.
 * <p/>
 * Some planners have required properties. Hadoop expects {@link AppProps#setApplicationJarPath(java.util.Map, String)} or
 * {@link AppProps#setApplicationJarClass(java.util.Map, Class)} to be set.
 * <p/>
 * Any properties set and passed through the FlowConnector constructor will be global to all Flow instances created through
 * the that FlowConnector instance. Some properties are on the {@link FlowDef} and would only be applicable to the
 * resulting Flow instance.
 * <p/>
 * These properties are used to influence the current planner and are also passed down to the
 * execution framework to override any default values. For example when using the Hadoop planner, the number of reducers
 * or mappers can be set by using platform specific properties.
 * <p/>
 * Custom operations (Functions, Filter, etc) may also retrieve these property values at runtime through calls to
 * {@link cascading.flow.FlowProcess#getProperty(String)} or {@link FlowProcess#getStringProperty(String)}.
 * <p/>
 * Most applications will need to call {@link cascading.property.AppProps#setApplicationJarClass(java.util.Map, Class)} or
 * {@link cascading.property.AppProps#setApplicationJarPath(java.util.Map, String)} so that
 * the correct application jar file is passed through to all child processes. The Class or path must reference
 * the custom application jar, not a Cascading library class or jar. The easiest thing to do is give setApplicationJarClass
 * the Class with your static main function and let Cascading figure out which jar to use.
 * <p/>
 * Note that Map<Object,Object> is compatible with the {@link Properties} class, so properties can be loaded at
 * runtime from a configuration file.
 * <p/>
 * By default, all {@link cascading.operation.Assertion}s are planned into the resulting Flow instance. This can be
 * changed for a given Flow by calling {@link FlowDef#setAssertionLevel(cascading.operation.AssertionLevel)} or globally
 * via {@link FlowConnectorProps#setAssertionLevel(cascading.operation.AssertionLevel)}.
 * <p/>
 * Also by default, all {@link cascading.operation.Debug}s are planned into the resulting Flow instance. This can be
 * changed for a given flow by calling {@link FlowDef#setDebugLevel(cascading.operation.DebugLevel)} or globally via
 * {@link FlowConnectorProps#setDebugLevel(cascading.operation.DebugLevel)}.
 *
 * @see cascading.flow.local.LocalFlowConnector
 * @see cascading.flow.hadoop.HadoopFlowConnector
 */
public abstract class FlowConnector
  {
  /** Field properties */
  protected Map<Object, Object> properties;

  /**
   * Method getIntermediateSchemeClass is used for debugging.
   *
   * @param properties of type Map<Object, Object>
   * @return Class
   */
  public Class getIntermediateSchemeClass( Map<Object, Object> properties )
    {
    // supporting stuffed classes to overcome classloading issue
    Object type = PropertyUtil.getProperty( properties, FlowConnectorProps.INTERMEDIATE_SCHEME_CLASS, null );

    if( type == null )
      return getDefaultIntermediateSchemeClass();

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
   * This has moved to {@link cascading.property.AppProps#setApplicationJarClass(java.util.Map, Class)}.
   *
   * @param properties
   * @param type
   */
  @Deprecated
  public static void setApplicationJarClass( Map<Object, Object> properties, Class type )
    {
    AppProps.setApplicationJarClass( properties, type );
    }

  /**
   * This has moved to {@link cascading.property.AppProps#setApplicationJarPath(java.util.Map, String)}.
   *
   * @param properties
   * @param path
   */
  @Deprecated
  public static void setApplicationJarPath( Map<Object, Object> properties, String path )
    {
    AppProps.setApplicationJarPath( properties, path );
    }

  protected abstract Class<? extends Scheme> getDefaultIntermediateSchemeClass();

  protected FlowConnector()
    {
    this.properties = new HashMap<Object, Object>();
    }

  protected FlowConnector( Map<Object, Object> properties )
    {
    if( properties == null )
      this.properties = new HashMap<Object, Object>();
    else if( properties instanceof Properties )
      this.properties = new Properties( (Properties) properties );
    else
      this.properties = new HashMap<Object, Object>( properties );
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
   * @param source source Tap to bind to the head of the given tail Pipe
   * @param sink   sink Tap to bind to the given tail Pipe
   * @param tail   tail end of a pipe assembly
   * @return Flow
   */
  public Flow connect( Tap source, Tap sink, Pipe tail )
    {
    return connect( null, source, sink, tail );
    }

  /**
   * Method connect links the given source and sink Taps to the given pipe assembly.
   *
   * @param name   name to give the resulting Flow
   * @param source source Tap to bind to the head of the given tail Pipe
   * @param sink   sink Tap to bind to the given tail Pipe
   * @param tail   tail end of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Pipe tail )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( tail.getHeads()[ 0 ].getName(), source );

    return connect( name, sources, sink, tail );
    }

  /**
   * Method connect links the given source, sink, and trap Taps to the given pipe assembly. The given trap will
   * be linked to the assembly head along with the source.
   *
   * @param name   name to give the resulting Flow
   * @param source source Tap to bind to the head of the given tail Pipe
   * @param sink   sink Tap to bind to the given tail Pipe
   * @param trap   trap Tap to sink all failed Tuples into
   * @param tail   tail end of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Tap trap, Pipe tail )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( tail.getHeads()[ 0 ].getName(), source );

    Map<String, Tap> traps = new HashMap<String, Tap>();

    traps.put( tail.getHeads()[ 0 ].getName(), trap );

    return connect( name, sources, sink, traps, tail );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   *
   * @param sources all head names and source Taps to bind to the heads of the given tail Pipe
   * @param sink    sink Tap to bind to the given tail Pipe
   * @param tail    tail end of a pipe assembly
   * @return Flow
   */
  public Flow connect( Map<String, Tap> sources, Tap sink, Pipe tail )
    {
    return connect( null, sources, sink, tail );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   *
   * @param name    name to give the resulting Flow
   * @param sources all head names and source Taps to bind to the heads of the given tail Pipe
   * @param sink    sink Tap to bind to the given tail Pipe
   * @param tail    tail end of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Tap sink, Pipe tail )
    {
    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( tail.getName(), sink );

    return connect( name, sources, sinks, tail );
    }

  /**
   * Method connect links the named source and trap Taps and sink Tap to the given pipe assembly.
   *
   * @param name    name to give the resulting Flow
   * @param sources all head names and source Taps to bind to the heads of the given tail Pipe
   * @param sink    sink Tap to bind to the given tail Pipe
   * @param traps   all pipe names and trap Taps to sink all failed Tuples into
   * @param tail    tail end of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Tap sink, Map<String, Tap> traps, Pipe tail )
    {
    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( tail.getName(), sink );

    return connect( name, sources, sinks, traps, tail );
    }

  /**
   * Method connect links the named trap Taps, source and sink Tap to the given pipe assembly.
   *
   * @param name   name to give the resulting Flow
   * @param source source Tap to bind to the head of the given tail Pipe
   * @param sink   sink Tap to bind to the given tail Pipe
   * @param traps  all pipe names and trap Taps to sink all failed Tuples into
   * @param tail   tail end of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Map<String, Tap> traps, Pipe tail )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( tail.getHeads()[ 0 ].getName(), source );

    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( tail.getName(), sink );

    return connect( name, sources, sinks, traps, tail );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param source source Tap to bind to the head of the given tail Pipes
   * @param sinks  all tail names and sink Taps to bind to the given tail Pipes
   * @param tails  all tail ends of a pipe assembly
   * @return Flow
   */
  public Flow connect( Tap source, Map<String, Tap> sinks, Collection<Pipe> tails )
    {
    return connect( null, source, sinks, tails.toArray( new Pipe[ tails.size() ] ) );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param name   name to give the resulting Flow
   * @param source source Tap to bind to the head of the given tail Pipes
   * @param sinks  all tail names and sink Taps to bind to the given tail Pipes
   * @param tails  all tail ends of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Tap source, Map<String, Tap> sinks, Collection<Pipe> tails )
    {
    return connect( name, source, sinks, tails.toArray( new Pipe[ tails.size() ] ) );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param source source Tap to bind to the head of the given tail Pipes
   * @param sinks  all tail names and sink Taps to bind to the given tail Pipes
   * @param tails  all tail ends of a pipe assembly
   * @return Flow
   */
  public Flow connect( Tap source, Map<String, Tap> sinks, Pipe... tails )
    {
    return connect( null, source, sinks, tails );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param name   name to give the resulting Flow
   * @param source source Tap to bind to the head of the given tail Pipes
   * @param sinks  all tail names and sink Taps to bind to the given tail Pipes
   * @param tails  all tail ends of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Tap source, Map<String, Tap> sinks, Pipe... tails )
    {
    Set<Pipe> heads = new HashSet<Pipe>();

    for( Pipe pipe : tails )
      Collections.addAll( heads, pipe.getHeads() );

    if( heads.isEmpty() )
      throw new IllegalArgumentException( "no pipe instance found" );

    if( heads.size() != 1 )
      throw new IllegalArgumentException( "there may be only 1 head pipe instance, found " + heads.size() );

    Map<String, Tap> sources = new HashMap<String, Tap>();

    for( Pipe pipe : heads )
      sources.put( pipe.getName(), source );

    return connect( name, sources, sinks, tails );
    }

  /**
   * Method connect links the named sources and sinks to the given pipe assembly.
   *
   * @param sources all head names and source Taps to bind to the heads of the given tail Pipes
   * @param sinks   all tail names and sink Taps to bind to the given tail Pipes
   * @param tails   all tail ends of a pipe assembly
   * @return Flow
   */
  public Flow connect( Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... tails )
    {
    return connect( null, sources, sinks, tails );
    }

  /**
   * Method connect links the named sources and sinks to the given pipe assembly.
   *
   * @param name    name to give the resulting Flow
   * @param sources all head names and source Taps to bind to the heads of the given tail Pipes
   * @param sinks   all tail names and sink Taps to bind to the given tail Pipes
   * @param tails   all tail ends of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... tails )
    {
    return connect( name, sources, sinks, new HashMap<String, Tap>(), tails );
    }

  /**
   * Method connect links the named sources, sinks and traps to the given pipe assembly.
   *
   * @param name    name to give the resulting Flow
   * @param sources all head names and source Taps to bind to the heads of the given tail Pipes
   * @param sinks   all tail names and sink Taps to bind to the given tail Pipes
   * @param traps   all pipe names and trap Taps to sink all failed Tuples into
   * @param tails   all tail ends of a pipe assembly
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, Pipe... tails )
    {
    name = name == null ? makeName( tails ) : name;

    FlowDef flowDef = flowDef()
      .setName( name )
      .addSources( sources )
      .addSinks( sinks )
      .addTraps( traps )
      .addTails( tails );

    return connect( flowDef );
    }

  public Flow connect( FlowDef flowDef )
    {
    FlowPlanner flowPlanner = createFlowPlanner();

    flowPlanner.initialize( this, properties );

    return flowPlanner.buildFlow( flowDef );
    }

  protected abstract FlowPlanner createFlowPlanner();

  /**
   * Method getPlatformInfo returns an instance of {@link PlatformInfo} for the underlying platform.
   *
   * @return of type PlatformInfo
   */
  public PlatformInfo getPlatformInfo()
    {
    return createFlowPlanner().getPlatformInfo();
    }

  /////////
  // UTIL
  /////////

  private String makeName( Pipe[] pipes )
    {
    String[] names = new String[ pipes.length ];

    for( int i = 0; i < pipes.length; i++ )
      names[ i ] = pipes[ i ].getName();

    String name = Util.join( names, "+" );

    if( name.length() > 32 )
      name = name.substring( 0, 32 );

    return name;
    }
  }
