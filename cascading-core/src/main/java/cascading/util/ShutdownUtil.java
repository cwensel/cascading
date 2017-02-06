/*
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

package cascading.util;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ShutdownUtil is a private helper class for registering dependent shutdown hooks to maintain internal state
 * information reliably when a jvm is shutting down.
 * <p/>
 * This api is not intended for public use.
 */
public class ShutdownUtil
  {
  /**
   * System property set to true when we have entered shutdown
   */
  public static final String SHUTDOWN_EXECUTING = "cascading.jvm.shutdown.executing";
  public static final String SHUTDOWN_FORCE_NON_DAEMON = "cascading.jvm.shutdown.non-daemon";

  private static final Logger LOG = LoggerFactory.getLogger( ShutdownUtil.class );

  public static abstract class Hook
    {
    public enum Priority
      {
        FIRST, WORK_PARENT, WORK_CHILD, SERVICE_CONSUMER, SERVICE_PROVIDER, LAST
      }

    public abstract Priority priority();

    public abstract void execute();
    }

  private static Queue<Hook> queue = new PriorityBlockingQueue<>( 20, new Comparator<Hook>()
  {
  @Override
  public int compare( Hook lhs, Hook rhs )
    {
    if( lhs == rhs )
      return 0;

    if( lhs == null )
      return -1;
    else if( rhs == null )
      return 1;

    return lhs.priority().compareTo( rhs.priority() );
    }
  }
  );

  private static Thread shutdownHook;

  public static void addHook( Hook hook )
    {
    if( hook == null )
      throw new IllegalArgumentException( "hook may not be null" );

    registerShutdownHook();

    queue.add( hook );
    }

  public static boolean removeHook( Hook hook )
    {
    return queue.remove( hook );
    }

  public static synchronized void registerShutdownHook()
    {
    if( shutdownHook != null )
      return;

    final boolean isForceNonDaemon = Boolean.getBoolean( SHUTDOWN_FORCE_NON_DAEMON );

    shutdownHook = new Thread( "cascading shutdown hooks" )
    {

    {
    if( isForceNonDaemon )
      this.setDaemon( false );
    }

    @Override
    public void run()
      {
      System.setProperty( SHUTDOWN_EXECUTING, "true" );

      try
        {
        // These are not threads, so each one will be run in priority order
        // blocking until the previous is complete
        while( !queue.isEmpty() )
          {
          Hook hook = null;

          try
            {
            hook = queue.poll();

            // may get removed while shutdown is executing
            if( hook == null )
              continue;

            hook.execute();
            }
          catch( Exception exception )
            {
            LOG.error( "failed executing hook: {}, with exception: {}", hook, exception.getMessage() );
            LOG.debug( "with exception trace", exception );
            }
          }
        }
      finally
        {
        System.setProperty( SHUTDOWN_EXECUTING, "false" );
        }
      }
    };

    Runtime.getRuntime().addShutdownHook( shutdownHook );
    }

  public static void deregisterShutdownHook()
    {
    Runtime.getRuntime().removeShutdownHook( shutdownHook );
    }
  }
