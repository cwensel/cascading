/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.flow.hadoop.planner.MapReduceHadoopRuleRegistry;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;

/**
 * Use the HadoopFlowConnector to link source and sink {@link Tap} instances with an assembly of {@link Pipe} instances into
 * an executable {@link HadoopFlow} for execution on an Apache Hadoop cluster.
 *
 * @see cascading.property.AppProps
 * @see cascading.flow.FlowConnectorProps
 * @see cascading.flow.FlowDef
 * @see cascading.flow.hadoop.MapReduceFlow
 */
public class HadoopFlowConnector extends FlowConnector
  {
  /**
   * Constructor HadoopFlowConnector creates a new HadoopFlowConnector instance.
   * <p/>
   * All properties passed to Hadoop are retrieved from a default instantiation of the Hadoop
   * {@link org.apache.hadoop.mapred.JobConf} which pulls all properties from the local CLASSPATH.
   */
  public HadoopFlowConnector()
    {
    }

  /**
   * Constructor HadoopFlowConnector creates a new HadoopFlowConnector instance using the given {@link Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new native configuration instance.
   *
   * @param properties of type Map
   */
  @ConstructorProperties({"properties"})
  public HadoopFlowConnector( Map<Object, Object> properties )
    {
    super( properties );
    }

  /**
   * Constructor HadoopFlowConnector creates a new HadoopFlowConnector instance.
   * <p/>
   * All properties passed to Hadoop are retrieved from a default instantiation of the Hadoop
   * {@link org.apache.hadoop.mapred.JobConf} which pulls all properties from the local CLASSPATH.
   *
   * @param ruleRegistry of type RuleRegistry
   */
  @ConstructorProperties({"ruleRegistry"})
  public HadoopFlowConnector( RuleRegistry ruleRegistry )
    {
    super( ruleRegistry );
    }

  /**
   * Constructor HadoopFlowConnector creates a new HadoopFlowConnector instance using the given {@link Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new native configuration instance.
   *
   * @param properties   of type Map
   * @param ruleRegistry of type RuleRegistry
   */
  @ConstructorProperties({"properties", "ruleRegistry"})
  public HadoopFlowConnector( Map<Object, Object> properties, RuleRegistry ruleRegistry )
    {
    super( properties, ruleRegistry );
    }

  @Override
  protected Class<? extends Scheme> getDefaultIntermediateSchemeClass()
    {
    return SequenceFile.class;
    }

  @Override
  protected FlowPlanner createFlowPlanner()
    {
    return new HadoopPlanner();
    }

  @Override
  protected RuleRegistry createDefaultRuleRegistry()
    {
    return new MapReduceHadoopRuleRegistry();
    }
  }
