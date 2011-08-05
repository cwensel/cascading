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

package cascading.flow.local;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import cascading.flow.planner.FlowStepJob;
import cascading.management.ClientState;
import cascading.stats.StepStats;

/**
 *
 */
public class LocalFlowStepJob extends FlowStepJob
  {
  private final LocalStepRunner stackRunner;
  private Future<Throwable> future;

  public LocalFlowStepJob( ClientState clientState, LocalFlowProcess flowProcess, LocalFlowStep flowStep )
    {
    super( clientState, flowStep, 1000 );
    flowProcess.setStepStats( (LocalStepStats) this.stepStats );
    this.stackRunner = new LocalStepRunner( flowProcess, flowStep );
    }

  @Override
  protected StepStats createStepStats( ClientState clientState )
    {
    return new LocalStepStats( flowStep, clientState );
    }

  @Override
  protected String internalJobId()
    {
    return "flow";
    }

  @Override
  protected void internalNonBlockingStart() throws IOException
    {
    ExecutorService executors = Executors.newFixedThreadPool( 1 );

    future = executors.submit( stackRunner );

    executors.shutdown();
    }

  @Override
  protected boolean internalIsStarted()
    {
    return future != null;
    }

  @Override
  protected boolean internalNonBlockingIsComplete() throws IOException
    {
    return stackRunner.isComplete();
    }

  @Override
  protected boolean internalNonBlockingIsSuccessful() throws IOException
    {
    return stackRunner.isSuccessful();
    }

  @Override
  protected void internalStop() throws IOException
    {
    }

  @Override
  protected void dumpDebugInfo()
    {
    }
  }
