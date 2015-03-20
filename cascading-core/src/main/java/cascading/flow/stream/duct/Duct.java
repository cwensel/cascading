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

package cascading.flow.stream.duct;

import cascading.flow.stream.graph.StreamGraph;

/**
 *
 */
public abstract class Duct<Incoming, Outgoing>
  {
  protected Duct<Outgoing, ?> next;

  Duct()
    {
    }

  Duct( Duct<Outgoing, ?> next )
    {
    this.next = next;
    }

  public Duct getNext()
    {
    return next;
    }

  public void bind( StreamGraph streamGraph )
    {
    next = getNextFor( streamGraph );
    }

  protected Duct getNextFor( StreamGraph streamGraph )
    {
    return streamGraph.createNextFor( this );
    }

  /** Called immediately after bind */
  public void initialize()
    {
    }

  /**
   *
   */
  public void prepare()
    {
    // never chain prepare calls
    }

  public final void receiveFirst( Incoming incoming )
    {
    receive( null, incoming );
    }

  public void start( Duct previous )
    {
    next.start( this );
    }

  public abstract void receive( Duct previous, Incoming incoming );

  public void complete( Duct previous )
    {
    next.complete( this );
    }

  public void cleanup()
    {
    // never chain cleanup calls
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName();
    }
  }
