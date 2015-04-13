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

package cascading.flow.tez.stream.element;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.flow.stream.element.SinkStage;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.tap.Tap;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TezSinkStage extends SinkStage
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezSinkStage.class );

  private final MROutput logicalOutput;
  private OldOutputCollector collector;

  public TezSinkStage( FlowProcess flowProcess, Tap sink, LogicalOutput logicalOutput )
    {
    super( flowProcess, sink );

    if( logicalOutput == null )
      throw new IllegalArgumentException( "output must not be null" );

    this.logicalOutput = (MROutput) logicalOutput;
    }

  @Override
  public void prepare()
    {
    LOG.info( "calling {}#start() on: {}", logicalOutput.getClass().getSimpleName(), getSink() );

    logicalOutput.start();

    collector = new OldOutputCollector( logicalOutput );

    super.prepare();
    }

  @Override
  public void cleanup()
    {
    try
      {
      super.cleanup();
      }
    finally
      {
      try
        {
        if( logicalOutput.isCommitRequired() )
          commit( logicalOutput );
        }
      catch( Exception exception )
        {
        LOG.warn( "exception on output close", exception );
        }
      }
    }

  @Override
  protected Object getOutput()
    {
    return collector;
    }

  private void commit( MROutput output ) throws IOException
    {
    int retries = 3;
    while( true )
      {
      // This will loop till the AM asks for the task to be killed. As
      // against, the AM sending a signal to the task to kill itself
      // gracefully.
      try
        {
        if( ( (Hadoop2TezFlowProcess) flowProcess ).getContext().canCommit() )
          break;

        Thread.sleep( 100 );
        }
      catch( InterruptedException exception )
        {
        //ignore
        }
      catch( IOException exception )
        {
        LOG.warn( "failure sending canCommit", exception );

        if( --retries == 0 )
          throw exception;
        }
      }

    // task can Commit now
    try
      {
      output.commit();
      }
    catch( IOException exception )
      {
      LOG.warn( "failure committing", exception );

      //if it couldn't commit a successfully then delete the output
      discardOutput( output );

      throw exception;
      }
    }

  private void discardOutput( MROutput output )
    {
    try
      {
      output.abort();
      }
    catch( IOException exception )
      {
      LOG.warn( "failure cleaning up", exception );
      }
    }
  }
