/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream.element;

import java.util.concurrent.atomic.AtomicInteger;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Collapsing;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.graph.StreamGraph;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class MergeStage extends ElementStage<TupleEntry, TupleEntry> implements Collapsing
  {
  private boolean started = false;
  protected final AtomicInteger completeCount = new AtomicInteger( 0 );
  private int numIncomingPaths;

  public MergeStage( FlowProcess flowProcess, FlowElement flowElement )
    {
    super( flowProcess, flowElement );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph );

    numIncomingPaths = streamGraph.findAllPreviousFor( this ).length;
    }

  @Override
  public void initialize()
    {
    super.initialize();

    completeCount.set( numIncomingPaths );
    }

  @Override
  public synchronized void start( Duct previous )
    {
    if( started )
      return;

    super.start( previous );
    started = true;
    }

  @Override
  public void complete( Duct previous )
    {
    if( completeCount.decrementAndGet() != 0 )
      return;

    super.complete( previous );
    completeCount.set( numIncomingPaths );
    }
  }
