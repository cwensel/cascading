/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.platform.hadoop;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 */
public class HadoopConfigDefScheme extends cascading.scheme.hadoop.TextLine
  {
  public HadoopConfigDefScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  @Override
  public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    // we should not see any config def values here
    if( flowProcess.getProperty( "default" ) != null )
      throw new RuntimeException( "default should be null" );

    super.sourceConfInit( flowProcess, tap, conf );
    }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    // we should not see any config def values here
    if( flowProcess.getProperty( "default" ) != null )
      throw new RuntimeException( "default should be null" );

    super.sinkConfInit( flowProcess, tap, conf );
    }

  @Override
  public void sourcePrepare( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
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
  public void sinkPrepare( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
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
