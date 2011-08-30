/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.planner.FlowPlanner;
import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.util.Util;

import static cascading.flow.FlowDef.flowDef;

/**
 *
 */
public abstract class FlowConnector
  {
  // need a global unique value here
  private static String appID;

  /** Field properties */
  protected Map<Object, Object> properties;

  /**
   * Method setAssertionLevel sets the target planner {@link cascading.operation.AssertionLevel}.
   *
   * @param properties     of type Map<Object, Object>
   * @param assertionLevel of type AssertionLevel
   */
  public static void setAssertionLevel( Map<Object, Object> properties, AssertionLevel assertionLevel )
    {
    if( assertionLevel != null )
      properties.put( "cascading.flowconnector.assertionlevel", assertionLevel.toString() );
    }

  /**
   * Method getAssertionLevel returns the configured target planner {@link cascading.operation.AssertionLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @return AssertionLevel the configured AssertionLevel
   */
  public static AssertionLevel getAssertionLevel( Map<Object, Object> properties )
    {
    String assertionLevel = Util.getProperty( properties, "cascading.flowconnector.assertionlevel", AssertionLevel.STRICT.name() );

    return AssertionLevel.valueOf( assertionLevel );
    }

  /**
   * Method setDebugLevel sets the target planner {@link cascading.operation.DebugLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @param debugLevel of type DebugLevel
   */
  public static void setDebugLevel( Map<Object, Object> properties, DebugLevel debugLevel )
    {
    if( debugLevel != null )
      properties.put( "cascading.flowconnector.debuglevel", debugLevel.toString() );
    }

  /**
   * Method getDebugLevel returns the configured target planner {@link cascading.operation.DebugLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @return DebugLevel the configured DebugLevel
   */
  public static DebugLevel getDebugLevel( Map<Object, Object> properties )
    {
    String debugLevel = Util.getProperty( properties, "cascading.flowconnector.debuglevel", DebugLevel.DEFAULT.name() );

    return DebugLevel.valueOf( debugLevel );
    }

  /**
   * Method setIntermediateSchemeClass is used for debugging.
   *
   * @param properties              of type Map<Object, Object>
   * @param intermediateSchemeClass of type Class
   */
  public static void setIntermediateSchemeClass( Map<Object, Object> properties, Class intermediateSchemeClass )
    {
    properties.put( "cascading.flowconnector.intermediateschemeclass", intermediateSchemeClass );
    }

  /**
   * Method setIntermediateSchemeClass is used for debugging.
   *
   * @param properties              of type Map<Object, Object>
   * @param intermediateSchemeClass of type String
   */
  public static void setIntermediateSchemeClass( Map<Object, Object> properties, String intermediateSchemeClass )
    {
    properties.put( "cascading.flowconnector.intermediateschemeclass", intermediateSchemeClass );
    }

  /**
   * Method getIntermediateSchemeClass is used for debugging.
   *
   * @param properties of type Map<Object, Object>
   * @return Class
   */
  public Class getIntermediateSchemeClass( Map<Object, Object> properties )
    {
    // supporting stuffed classes to overcome classloading issue
    Object type = Util.getProperty( properties, "cascading.flowconnector.intermediateschemeclass", (Object) null );

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

  protected abstract Class<? extends Scheme> getDefaultIntermediateSchemeClass();

  /**
   * Method setApplicationJarClass is used to set the application jar file.
   * </p>
   * All cluster executed Cascading applications
   * need to call setApplicationJarClass(java.util.Map, Class) or
   * {@link #setApplicationJarPath(java.util.Map, String)}, otherwise ClassNotFound exceptions are likely.
   *
   * @param properties of type Map
   * @param type       of type Class
   */
  public static void setApplicationJarClass( Map<Object, Object> properties, Class type )
    {
    if( type != null )
      properties.put( "cascading.flowconnector.appjar.class", type );
    }

  /**
   * Method getApplicationJarClass returns the Class set by the setApplicationJarClass method.
   *
   * @param properties of type Map<Object, Object>
   * @return Class
   */
  public static Class getApplicationJarClass( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.flowconnector.appjar.class", (Class) null );
    }

  /**
   * Method setApplicationJarPath is used to set the application jar file.
   * </p>
   * All cluster executed Cascading applications
   * need to call {@link #setApplicationJarClass(java.util.Map, Class)} or
   * setApplicationJarPath(java.util.Map, String), otherwise ClassNotFound exceptions are likely.
   *
   * @param properties of type Map
   * @param path       of type String
   */
  public static void setApplicationJarPath( Map<Object, Object> properties, String path )
    {
    if( path != null )
      properties.put( "cascading.flowconnector.appjar.path", path );
    }

  /**
   * Method getApplicationJarPath return the path set by the setApplicationJarPath method.
   *
   * @param properties of type Map<Object, Object>
   * @return String
   */
  public static String getApplicationJarPath( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.flowconnector.appjar.path", (String) null );
    }

  public static void setApplicationID( Map<Object, Object> properties )
    {
    properties.put( "cascading.app.id", getAppID( properties ) );
    }

  public static String getApplicationID( Map<Object, Object> properties )
    {
    if( properties == null )
      return getAppID( null );

    return Util.getProperty( properties, "cascading.app.id", getAppID( properties ) );
    }

  private static String getAppID( Map<Object, Object> properties )
    {
    if( appID == null )
      {
      String appName = properties == null ? "appnameseed" : getApplicationName( properties );
      appID = Util.createUniqueID( appName );
      }

    return appID;
    }

  public static void setApplicationName( Map<Object, Object> properties, String name )
    {
    if( name != null )
      properties.put( "cascading.app.name", name );
    }

  public static String getApplicationName( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.app.name", (String) null );
    }

  public static void setApplicationVersion( Map<Object, Object> properties, String version )
    {
    if( version != null )
      properties.put( "cascading.app.version", version );
    }

  public static String getApplicationVersion( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.app.version", (String) null );
    }

  public static void addApplicationTag( Map<Object, Object> properties, String tag )
    {
    if( tag == null )
      return;

    String tags = Util.getProperty( properties, "cascading.app.tags", (String) null );

    if( tags != null )
      tags = Util.join( ",", tag.trim(), tags );
    else
      tags = tag;

    properties.put( "cascading.app.tags", tags );
    }

  public static String getApplicationTags( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.app.tags", (String) null );
    }

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
