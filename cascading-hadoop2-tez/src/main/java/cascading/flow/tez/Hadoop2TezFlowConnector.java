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

package cascading.flow.tez;

import java.beans.ConstructorProperties;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.tez.planner.Hadoop2TezPlanner;
import cascading.flow.tez.planner.HashJoinHadoop2TezRuleRegistry;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;

/**
 */
public class Hadoop2TezFlowConnector extends FlowConnector
  {
  public Hadoop2TezFlowConnector()
    {
    }

  @ConstructorProperties({"properties"})
  public Hadoop2TezFlowConnector( Map<Object, Object> properties )
    {
    super( properties );
    }

  @ConstructorProperties({"ruleRegistry"})
  public Hadoop2TezFlowConnector( RuleRegistry ruleRegistry )
    {
    super( ruleRegistry );
    }

  @ConstructorProperties({"properties", "ruleRegistry"})
  public Hadoop2TezFlowConnector( Map<Object, Object> properties, RuleRegistry ruleRegistry )
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
    return new Hadoop2TezPlanner();
    }

  @Override
  protected RuleRegistry createDefaultRuleRegistry()
    {
//    return new NoHashJoinHadoop2TezRuleRegistry();
    return new HashJoinHadoop2TezRuleRegistry();
    }
  }
