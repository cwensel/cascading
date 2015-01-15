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

package cascading.platform.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class LocalConfigDefScheme extends cascading.scheme.local.TextLine
  {

  public LocalConfigDefScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  @Override
  public void sourceConfInit( FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
    {
    // we should not see any config def values here
    if( flowProcess.getProperty( "default" ) != null )
      throw new RuntimeException( "default should be null" );

    super.sourceConfInit( flowProcess, tap, conf );
    }

  @Override
  public void sinkConfInit( FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
    {
    // we should not see any config def values here
    if( flowProcess.getProperty( "default" ) != null )
      throw new RuntimeException( "default should be null" );

    super.sinkConfInit( flowProcess, tap, conf );
    }

  @Override
  public void sourcePrepare( FlowProcess<Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    if( !( flowProcess instanceof FlowProcessWrapper ) )
      throw new RuntimeException( "not a flow process wrapper" );

    if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
      throw new RuntimeException( "not default value" );

    if( !"source-replace".equals( flowProcess.getProperty( "replace" ) ) )
      throw new RuntimeException( "not replaced value" );

    flowProcess = ( (FlowProcessWrapper) flowProcess ).getDelegate();

    if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
      throw new RuntimeException( "not default value" );

    if( !"process-replace".equals( flowProcess.getProperty( "replace" ) ) )
      throw new RuntimeException( "not replaced value" );

    super.sourcePrepare( flowProcess, sourceCall );
    }

  @Override
  public void sinkPrepare( FlowProcess<Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall ) throws IOException
    {
    if( !( flowProcess instanceof FlowProcessWrapper ) )
      throw new RuntimeException( "not a flow process wrapper" );

    if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
      throw new RuntimeException( "not default value" );

    if( !"sink-replace".equals( flowProcess.getProperty( "replace" ) ) )
      throw new RuntimeException( "not replaced value" );

    flowProcess = ( (FlowProcessWrapper) flowProcess ).getDelegate();

    if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
      throw new RuntimeException( "not default value" );

    if( !"process-replace".equals( flowProcess.getProperty( "replace" ) ) )
      throw new RuntimeException( "not replaced value" );

    super.sinkPrepare( flowProcess, sinkCall );
    }
  }
