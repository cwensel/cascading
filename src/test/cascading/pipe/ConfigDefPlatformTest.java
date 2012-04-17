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

package cascading.pipe;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.property.ConfigDef;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.inputFileNums20;

/**
 *
 */
@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class ConfigDefPlatformTest extends PlatformTestCase
  {
  public ConfigDefPlatformTest()
    {
    super( true );
    }

  public static class IterateInsert extends BaseOperation implements Function
    {

    public IterateInsert( Fields fieldDeclaration )
      {
      super( fieldDeclaration );
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall operationCall )
      {
      if( !( flowProcess instanceof FlowProcessWrapper ) )
        throw new RuntimeException( "not a flow process wrapper" );

      if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      if( !"pipe-replace".equals( flowProcess.getProperty( "replace" ) ) )
        throw new RuntimeException( "not replaced value" );

      flowProcess = ( (FlowProcessWrapper) flowProcess ).getDelegate();

      if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      if( !"process-replace".equals( flowProcess.getProperty( "replace" ) ) )
        throw new RuntimeException( "not replaced value" );
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      functionCall.getOutputCollector().add( new Tuple( "value" ) );
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
      {
      }
    }

  @Test
  public void testConfigDef() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileNums20 );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new IterateInsert( new Fields( "value" ) ), Fields.ALL );

    pipe.getConfigDef().setProperty( ConfigDef.Mode.DEFAULT, "default", "pipe-default" );

    // steps on above value
    pipe.getProcessConfigDef().setProperty( ConfigDef.Mode.DEFAULT, "default", "process-default" );

    pipe.getConfigDef().setProperty( ConfigDef.Mode.DEFAULT, "replace", "pipe-default" );
    pipe.getConfigDef().setProperty( ConfigDef.Mode.REPLACE, "replace", "pipe-replace" );

    pipe.getProcessConfigDef().setProperty( ConfigDef.Mode.DEFAULT, "replace", "process-default" );
    pipe.getProcessConfigDef().setProperty( ConfigDef.Mode.REPLACE, "replace", "process-replace" );

    Tap sink = getPlatform().getTextFile( getOutputPath( "configdef" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 20 );
    }
  }
