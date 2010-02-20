/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingTestCase;

/**
 *
 */
public class FlowListenersTest extends CascadingTestCase
  {
  public FlowListenersTest()
    {
    super( "flow test" );
    }

  public void testListners()
    {
    Flow flow = new Flow();

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
