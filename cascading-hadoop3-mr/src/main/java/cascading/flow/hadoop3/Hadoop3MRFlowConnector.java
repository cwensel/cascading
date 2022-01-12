/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.flow.hadoop3;

import java.beans.ConstructorProperties;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.planner.MapReduceHadoopRuleRegistry;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;

/**
 * Use the Hadoop3MRFlowConnector to link source and sink {@link cascading.tap.Tap} instances with an assembly of {@link cascading.pipe.Pipe} instances into
 * an executable {@link cascading.flow.hadoop.HadoopFlow} for execution on an Apache Hadoop cluster.
 *
 * @see cascading.property.AppProps
 * @see cascading.flow.FlowConnectorProps
 * @see cascading.flow.FlowDef
 * @see cascading.flow.hadoop.MapReduceFlow
 */
public class Hadoop3MRFlowConnector extends FlowConnector
  {
  /**
   * Constructor FlowConnector creates a new FlowConnector instance.
   * <p>
   * All properties passed to Hadoop are retrieved from a default instantiation of the Hadoop
   * {@link org.apache.hadoop.mapred.JobConf} which pulls all properties from the local CLASSPATH.
   */
  public Hadoop3MRFlowConnector()
    {
    }

  /**
   * Constructor FlowConnector creates a new FlowConnector instance using the given {@link java.util.Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new native configuration instance.
   *
   * @param properties of type Properties
   */
  @ConstructorProperties({"properties"})
  public Hadoop3MRFlowConnector( Map<Object, Object> properties )
    {
    super( properties );
    }

  /**
   * Constructor HadoopFlowConnector creates a new HadoopFlowConnector instance.
   * <p>
   * All properties passed to Hadoop are retrieved from a default instantiation of the Hadoop
   * {@link org.apache.hadoop.mapred.JobConf} which pulls all properties from the local CLASSPATH.
   *
   * @param ruleRegistrySet of type RuleRegistry
   */
  @ConstructorProperties({"ruleRegistrySet"})
  public Hadoop3MRFlowConnector( RuleRegistrySet ruleRegistrySet )
    {
    super( ruleRegistrySet );
    }

  /**
   * Constructor HadoopFlowConnector creates a new HadoopFlowConnector instance using the given {@link java.util.Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new native configuration instance.
   *
   * @param properties      of type Map
   * @param ruleRegistrySet of type RuleRegistry
   */
  @ConstructorProperties({"properties", "ruleRegistrySet"})
  public Hadoop3MRFlowConnector( Map<Object, Object> properties, RuleRegistrySet ruleRegistrySet )
    {
    super( properties, ruleRegistrySet );
    }

  @Override
  protected Class<? extends Scheme> getDefaultIntermediateSchemeClass()
    {
    return SequenceFile.class;
    }

  @Override
  protected FlowPlanner createFlowPlanner()
    {
    return new Hadoop3MRPlanner();
    }

  @Override
  protected RuleRegistrySet createDefaultRuleRegistrySet()
    {
    return new RuleRegistrySet( new MapReduceHadoopRuleRegistry() );
    }
  }
