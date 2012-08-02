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

package cascading.flow.stream;

import java.util.LinkedList;

/**
 *
 */
public class TestGate<Incoming, Outgoing> extends Gate<Incoming, Outgoing>
  {
  private final LinkedList<Incoming> list = new LinkedList<Incoming>();

  int count = 0;

  public TestGate()
    {
    }

  @Override
  public void start( Duct previous )
    {
    }

  @Override
  public void receive( Duct previous, Incoming incoming )
    {
    list.add( incoming );
    }

  @Override
  public synchronized void complete( Duct previous )
    {
    count++;

    if( count < allPrevious.length )
      return;

    try
      {
      Grouping grouping = new Grouping();

      grouping.iterator = list.listIterator();

      next.start( this );

      next.receive( this, (Outgoing) grouping );

      next.complete( this );
      }
    finally
      {
      list.clear();
      count = 0;
      }
    }

  @Override
  public void cleanup()
    {
    }
  }
