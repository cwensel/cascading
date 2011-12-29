/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
    super( properties );
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
