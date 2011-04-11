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

package cascading.flow.hadoop;

import java.beans.ConstructorProperties;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.planner.FlowPlanner;
import cascading.operation.Assertion;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;

/**
 * Use the FlowConnector to link source and sink {@link Tap} instances with an assembly of {@link Pipe} instances into
 * an executable {@link cascading.flow.Flow}.
 * <p/>
 * FlowConnector invokes a planner for the target execution environment. Currently only {@link HadoopPlanner}
 * is supported (for Hadoop). If you have just one pre-existing custom Hadoop job to execute, see {@link cascading.flow.hadoop.MapReduceFlow}.
 * <p/>
 * Note that all {@code connect} methods take a single {@code tail} or an array of {@code tail} Pipe instances. "tail"
 * refers to the last connected Pipe instances in a pipe-assembly. Pipe-assemblies are graphs of object with "heads"
 * and "tails". From a given "tail", all connected heads can be found, but not the reverse. So "tails" must be
 * supplied by the user.
 * <p/>
 * The FlowConnector, resulting Flow, and the underlying execution framework (Hadoop) can be configured via a
 * {@link Map} or {@link Properties} instance given to the constructor. This properties map can be
 * populated before constructing a FlowConnector instance through static methods on FlowConnector and
 * MultiMapReducePlanner. These properties are used to influence the current planner and are also passed down to the
 * execution framework (Hadoop) to override any default values (the number of reducers or mappers, etc. by using
 * application specific properties).
 * <p/>
 * Custom operations (Functions, Filter, etc) may also retrieve these property values at runtime through calls to
 * {@link cascading.flow.FlowProcess#getProperty(String)}.
 * <p/>
 * Most applications will need to call {@link #setApplicationJarClass(java.util.Map, Class)} or
 * {@link #setApplicationJarPath(java.util.Map, String)} so that the correct application jar file is passed through
 * to all child processes. The Class or path must reference
 * the custom application jar, not a Cascading library class or jar. The easiest thing to do is give setApplicationJarClass
 * the Class with your static main function and let Cascading figure out which jar to use.
 * <p/>
 * Note that Map<Object,Object> is compatible with the {@link Properties} class, so properties can be loaded at
 * runtime from a configuration file.
 * <p/>
 * By default, all {@link Assertion}s are planned into the resulting Flow instance. This can be
 * changed by calling {@link #setAssertionLevel(java.util.Map, cascading.operation.AssertionLevel)}.
 * <p/>
 * Also by default, all {@link cascading.operation.Debug}s are planned into the resulting Flow instance. This can be
 * changed by calling {@link #setDebugLevel(java.util.Map, cascading.operation.DebugLevel)}.
 * <p/>
 * <strong>Properties</strong><br/>
 * <ul>
 * <li>cascading.flowconnector.appjar.class</li>
 * <li>cascading.flowconnector.appjar.path</li>
 * <li>cascading.flowconnector.assertionlevel</li>
 * <li>cascading.flowconnector.debuglevel</li>
 * <li>cascading.flowconnector.intermediateschemeclass</li>
 * </ul>
 *
 * @see cascading.flow.hadoop.MapReduceFlow
 */
public class HadoopFlowConnector extends FlowConnector
  {


  /** Constructor FlowConnector creates a new FlowConnector instance. */
  public HadoopFlowConnector()
    {
    }

  /**
   * Constructor FlowConnector creates a new FlowConnector instance using the given {@link Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new native configuration instance.
   *
   * @param properties of type Properties
   */
  @ConstructorProperties({"properties"})
  public HadoopFlowConnector( Map<Object, Object> properties )
    {
    this.properties = properties;
    }

  protected Class<? extends Scheme> getDefaultIntermediateSchemeClass()
    {
    return SequenceFile.class;
    }

  protected FlowPlanner createFlowPlanner()
    {
    return new HadoopPlanner();
    }
  }
