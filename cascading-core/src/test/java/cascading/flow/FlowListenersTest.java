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

package cascading.flow;

import java.util.Map;

import junit.framework.TestCase;

/**
 *
 */
public class FlowListenersTest extends TestCase
  {
  public FlowListenersTest()
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
    protected void internalClean( boolean force )
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

    FlowListener listener = new FlowListener()
    {

    public void onStarting( Flow flow )
      {
      }

    public void onStopping( Flow flow )
      {
      }

    public void onCompleted( Flow flow )
      {
      }

    public boolean onThrowable( Flow flow, Throwable throwable )
      {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
      }
    };

    flow.addListener( listener );

    assertTrue( "no listener found", flow.hasListeners() );

    flow.removeListener( listener );

    assertFalse( "listener found", flow.hasListeners() );
    }
  }
