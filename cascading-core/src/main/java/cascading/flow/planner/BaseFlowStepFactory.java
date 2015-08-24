/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner;

import cascading.flow.FlowStep;
import cascading.flow.planner.process.FlowNodeFactory;
import cascading.flow.planner.process.FlowStepFactory;
import cascading.tap.Tap;
import cascading.util.Util;

/**
 *
 */
public abstract class BaseFlowStepFactory<Config> implements FlowStepFactory<Config>
  {
  FlowNodeFactory flowNodeFactory;

  protected BaseFlowStepFactory()
    {
    }

  public BaseFlowStepFactory( FlowNodeFactory flowNodeFactory )
    {
    this.flowNodeFactory = flowNodeFactory;
    }

  public String makeFlowStepName( FlowStep flowStep, int numSteps, int stepNum )
    {
    Tap sink = Util.getFirst( flowStep.getSinkTaps() );

    stepNum++; // number more sensical (5/5)

    if( sink == null || sink.isTemporary() )
      return String.format( "(%d/%d)", stepNum, numSteps );

    String identifier = sink.getIdentifier();

    if( identifier.length() > 25 )
      identifier = String.format( "...%25s", identifier.substring( identifier.length() - 25 ) );

    return String.format( "(%d/%d) %s", stepNum, numSteps, identifier );
    }

  @Override
  public FlowNodeFactory getFlowNodeFactory()
    {
    return flowNodeFactory;
    }
  }
