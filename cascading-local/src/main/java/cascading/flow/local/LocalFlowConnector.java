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

package cascading.flow.local;

import java.beans.ConstructorProperties;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.local.planner.LocalPlanner;
import cascading.flow.local.planner.LocalRuleRegistry;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.scheme.Scheme;

/**
 * Use the LocalFlowConnector to link source and sink {@link cascading.tap.Tap} instances with an assembly of {@link cascading.pipe.Pipe} instances into
 * an executable {@link LocalFlow} for execution in local memory.
 *
 * @see cascading.property.AppProps
 * @see cascading.flow.FlowConnectorProps
 * @see cascading.flow.FlowDef
 */
public class LocalFlowConnector extends FlowConnector
  {
  /** Constructor LocalFlowConnector creates a default instance. */
  public LocalFlowConnector()
    {
    }

  /**
   * Constructor LocalFlowConnector creates a new instance using any of the given properties.
   *
   * @param properties of type Map
   */
  @ConstructorProperties({"properties"})
  public LocalFlowConnector( Map<Object, Object> properties )
    {
    super( properties );
    }

  /** Constructor LocalFlowConnector creates a default instance. */
  @ConstructorProperties({"ruleRegistrySet"})
  public LocalFlowConnector( RuleRegistrySet ruleRegistrySet )
    {
    super( ruleRegistrySet );
    }

  /**
   * Constructor LocalFlowConnector creates an instance using any of the given properties.
   *
   * @param properties of type Map
   */
  @ConstructorProperties({"properties", "ruleRegistrySet"})
  public LocalFlowConnector( Map<Object, Object> properties, RuleRegistrySet ruleRegistrySet )
    {
    super( properties, ruleRegistrySet );
    }

  @Override
  protected Class<? extends Scheme> getDefaultIntermediateSchemeClass()
    {
    return null;
    }

  @Override
  protected FlowPlanner createFlowPlanner()
    {
    return new LocalPlanner();
    }

  @Override
  protected RuleRegistrySet createDefaultRuleRegistrySet()
    {
    return new RuleRegistrySet( new LocalRuleRegistry() );
    }
  }
