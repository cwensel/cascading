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

package cascading.flow;

import java.util.Map;

import cascading.CascadingTestCase;
import cascading.test.PlatformTest;

/**
 *
 */
@PlatformTest(platforms = {"none"})
public class FlowListenersTest extends CascadingTestCase
  {
  public FlowListenersTest()
    {
    }

  public void testListeners()
    {
    Flow flow = new Flow<Object>()
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
    public void setProperty( String key, String value )
      {
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
    protected boolean allowParallelExecution()
      {
      return false;
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
