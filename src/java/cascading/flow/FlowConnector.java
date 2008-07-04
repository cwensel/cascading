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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.operation.Assertion;
import cascading.operation.AssertionLevel;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Use the FlowConnector to link sink and source {@link Tap} instances with an assembly of {@link Pipe} instances into
 * a {@link Flow}.
 * </p>
 * By default, all {@link Assertion} are planned into the resulting Flow instance. This can be changed by calling {@link #setAssertionLevel(AssertionLevel)}.
 */
public class FlowConnector
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FlowConnector.class );

  /** Field jobConf */
  private JobConf jobConf;
  /** Field assertionLevel */
  private AssertionLevel assertionLevel = AssertionLevel.STRICT;
  /** Field intermediateSchemeClass */
  private Class intermediateSchemeClass;

  /** Constructor FlowConnector creates a new FlowConnector instance. */
  public FlowConnector()
    {
    }

  /**
   * Constructor FlowConnector creates a new FlowConnector instance using the given {@link JobConf} instance as
   * default values for the underlying jobs.
   *
   * @param jobConf of type JobConf
   */
  public FlowConnector( JobConf jobConf )
    {
    this.jobConf = jobConf;
    }

  /**
   * Constructor FlowConnector creates a new FlowConnector instance using the given {@link Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new {@link JobConf} instance.
   *
   * @param properties of type Properties
   */
  public FlowConnector( Properties properties )
    {
    copyProperties( properties );
    }

  private void copyProperties( Properties properties )
    {
    if( properties == null )
      return;

    jobConf = new JobConf();

    Enumeration enumeration = properties.propertyNames();

    while( enumeration.hasMoreElements() )
      {
      String key = (String) enumeration.nextElement();
      String value = properties.getProperty( key );

      if( value == null )
        throw new IllegalStateException( "property value was null for key: " + key );

      jobConf.set( key, value );
      }
    }

  /**
   * Method getAssertionLevel returns the assertionLevel of this FlowConnector object.
   *
   * @return the assertionLevel (type Level) of this FlowConnector object.
   */
  public AssertionLevel getAssertionLevel()
    {
    return assertionLevel;
    }

  /**
   * Method setAssertionLevel sets the assertionLevel of this FlowConnector object.
   *
   * @param assertionLevel the assertionLevel of this FlowConnector object.
   */
  public void setAssertionLevel( AssertionLevel assertionLevel )
    {
    this.assertionLevel = assertionLevel;
    }

  /**
   * Method getIntermediateSchemeClass returns the intermediateSchemeClass of this FlowConnector object.
   *
   * @return the intermediateSchemeClass (type Class) of this FlowConnector object.
   */
  public Class getIntermediateSchemeClass()
    {
    return intermediateSchemeClass;
    }

  /**
   * Method setIntermediateSchemeClass sets the intermediateSchemeClass of this FlowConnector object.
   *
   * @param intermediateSchemeClass the intermediateSchemeClass of this FlowConnector object.
   */
  public void setIntermediateSchemeClass( Class intermediateSchemeClass )
    {
    this.intermediateSchemeClass = intermediateSchemeClass;
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
    return new MultiMapReducePlanner( jobConf, assertionLevel, intermediateSchemeClass ).buildFlow( name, pipes, sources, sinks, traps );
    }

  /////////
  // UTIL
  /////////

  private String makeName( Pipe[] pipes )
    {
    String[] names = new String[pipes.length];

    for( int i = 0; i < pipes.length; i++ )
      names[ i ] = pipes[ i ].getName();

    return Util.join( names, "+" );
    }
  }
