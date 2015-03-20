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

package cascading.flow;

import java.util.Set;

import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.tap.Tap;
import junit.framework.TestCase;

/**
 *
 */
public class FlowStepListenersTest extends TestCase
  {
  public FlowStepListenersTest()
    {
    }

  public void testListeners()
    {
    BaseFlowStep step = new BaseFlowStep( "step1", 0 )
    {

    @Override
    public Object createInitializedConfig( FlowProcess fp, Object config )
      {
      return null;
      }

    @Override
    public void clean( Object config )
      {
      }

    @Override
    protected FlowStepJob createFlowStepJob( ClientState clientState, FlowProcess fp, Object initializedStepConfig )
      {
      return null;
      }

    @Override
    public Set getTraps()
      {
      return null;
      }

    @Override
    public Tap getTrap( String string )
      {
      return null;
      }
    };

    FlowStepListener listener = new FlowStepListener()
    {
    public void onStepStarting( FlowStep flowStep )
      {
      }

    public void onStepStopping( FlowStep flowStep )
      {
      }

    public void onStepCompleted( FlowStep flowStep )
      {
      }

    public boolean onStepThrowable( FlowStep flowStep, Throwable throwable )
      {
      return false;
      }

    public void onStepRunning( FlowStep flowStep )
      {
      }
    };
    step.addListener( listener );

    assertTrue( "no listener found", step.hasListeners() );

    step.removeListener( listener );

    assertFalse( "listener found", step.hasListeners() );
    }
  }