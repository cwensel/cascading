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

package cascading.flow.local.planner;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import cascading.flow.local.LocalFlowProcess;
import cascading.flow.local.LocalFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;
import cascading.stats.local.LocalStepStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LocalFlowStepJob extends FlowStepJob<Properties>
  {
  private static final Logger LOG = LoggerFactory.getLogger( LocalFlowStepJob.class );

  private final LocalStepRunner stackRunner;
  private Future<Throwable> future;

  public LocalFlowStepJob( ClientState clientState, LocalFlowProcess flowProcess, LocalFlowStep flowStep )
    {
    super( clientState, flowStep, 200, 1000 );
    flowProcess.setStepStats( (LocalStepStats) this.flowStepStats );
    this.stackRunner = new LocalStepRunner( flowProcess, flowStep );
    }

  @Override
  public Properties getConfig()
    {
    return flowStep.getConfig();
    }

  @Override
  protected FlowStepStats createStepStats( ClientState clientState )
    {
    return new LocalStepStats( flowStep, clientState );
    }

  @Override
  protected boolean isRemoteExecution()
    {
    return false;
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
  protected Throwable getThrowable()
    {
    return stackRunner.getThrowable();
    }

  @Override
  protected boolean internalNonBlockingIsSuccessful() throws IOException
    {
    return stackRunner.isSuccessful();
    }

  @Override
  protected void internalBlockOnStop() throws IOException
    {
    }

  @Override
  protected void dumpDebugInfo()
    {
    }
  }
