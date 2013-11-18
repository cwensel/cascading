/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.util.Map;

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
    Flow flow = new BaseFlow<Object>()
    {

    @Override
    protected void initConfig( Map<Object, Object> properties, Object parentConfig )
      {
      }

    @Override
    protected void setConfigProperty( Object object, Object key, Object value )
      {
      }

    @Override
    protected Object newConfig( Object defaultConfig )
      {
      return null;
      }

    @Override
    public Object getConfig()
      {
      return null;
      }

    @Override
    public Object getConfigCopy()
      {
      return null;
      }

    @Override
    public Map<Object, Object> getConfigAsProperties()
      {
      return null;
      }

    @Override
    public String getProperty( String key )
      {
      return null;
      }

    @Override
    public FlowProcess getFlowProcess()
      {
      return null;
      }

    @Override
    protected void internalStart()
      {
      }

    @Override
    protected void internalClean( boolean stop )
      {
      }

    @Override
    public boolean stepsAreLocal()
      {
      return false;
      }

    @Override
    protected int getMaxNumParallelSteps()
      {
      return 0;
      }

    @Override
    protected void internalShutdown()
      {
      }
    };

    FlowStepListener listener = new FlowStepListener()
    {

      public void onStepStarting(FlowStep flowStep)
        {
        }

      public void onStepStopping(FlowStep flowStep) 
        {
        }

      public void onStepCompleted(FlowStep flowStep)
        {
        }

      public boolean onStepThrowable(FlowStep flowStep, Throwable throwable)
        {
        }

      public void onStepRunning(FlowStep flowStep)
        {
         return false;
        }
    };

    flow.addStepListener(listener);

    assertTrue( "no listener found", flow.hasStepListeners() );

    flow.removeStepListener( listener );

    assertFalse( "listener found", flow.hasStepListeners() );
    }
  }
