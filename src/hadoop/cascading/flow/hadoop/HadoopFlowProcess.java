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

package cascading.flow.hadoop;

import java.io.IOException;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class HadoopFlowProcess is an implementation of {@link FlowProcess} for Hadoop. Use this interface to get direct
 * access to the Hadoop JobConf and Reporter interfaces.
 * <p/>
 * Be warned that coupling to this implementation will cause custom {@link cascading.operation.Operation}s to
 * fail if they are executed on a system other than Hadoop.
 *
 * @see cascading.flow.FlowSession
 * @see JobConf
 * @see Reporter
 */
public class HadoopFlowProcess extends FlowProcess<JobConf>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopFlowProcess.class );

  public static final String SPILL_THRESHOLD = "cascading.cogroup.spill.threshold";
  public static final int defaultThreshold = 10 * 1000;
  public static final String SPILL_COMPRESS = "cascading.cogroup.spill.compress";
  public static final String SPILL_CODECS = "cascading.cogroup.spill.codecs";
  public static final String defaultCodecs = "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec";

  /** Field jobConf */
  final JobConf jobConf;
  /** Field isMapper */
  private final boolean isMapper;
  /** Field reporter */
  Reporter reporter = Reporter.NULL;
  private OutputCollector outputCollector;
  private final CompressionCodec codec;

  public HadoopFlowProcess()
    {
    this.jobConf = new JobConf();
    this.isMapper = true;
    this.codec = getCompressionCodec();
    }

  public HadoopFlowProcess( FlowSession flowSession, JobConf jobConf )
    {
    super( flowSession );
    this.jobConf = jobConf;
    this.isMapper = true;
    this.codec = getCompressionCodec();
    }

  /**
   * Constructor HadoopFlowProcess creates a new HadoopFlowProcess instance.
   *
   * @param flowSession of type FlowSession
   * @param jobConf     of type JobConf
   */
  public HadoopFlowProcess( FlowSession flowSession, JobConf jobConf, boolean isMapper )
    {
    super( flowSession );
    this.jobConf = jobConf;
    this.isMapper = isMapper;
    this.codec = getCompressionCodec();
    }

  public HadoopFlowProcess( HadoopFlowProcess flowProcess, JobConf jobConf )
    {
    super( flowProcess.getCurrentSession() );
    this.jobConf = jobConf;
    this.isMapper = flowProcess.isMapper();
    this.codec = flowProcess.getCompressionCodec();
    this.reporter = flowProcess.getReporter();
    }

  @Override
  public FlowProcess copyWith( JobConf jobConf )
    {
    return new HadoopFlowProcess( this, jobConf );
    }

  /**
   * Method getJobConf returns the jobConf of this HadoopFlowProcess object.
   *
   * @return the jobConf (type JobConf) of this HadoopFlowProcess object.
   */
  public JobConf getJobConf()
    {
    return jobConf;
    }

  @Override
  public JobConf getConfigCopy()
    {
    return new JobConf( jobConf );
    }

  /**
   * Method isMapper returns true if this part of the FlowProcess is a MapReduce mapper. If false, it is a reducer.
   *
   * @return boolean
   */
  public boolean isMapper()
    {
    return isMapper;
    }

  public int getCurrentNumMappers()
    {
    return getJobConf().getNumMapTasks();
    }

  public int getCurrentNumReducers()
    {
    return getJobConf().getNumReduceTasks();
    }

  /**
   * Method getCurrentTaskNum returns the task number of this task. Task 0 is the first task.
   *
   * @return int
   */
  @Override
  public int getCurrentTaskNum()
    {
    return getJobConf().getInt( "mapred.task.partition", 0 );
    }

  @Override
  public int getNumConcurrentTasks()
    {
    if( isMapper() )
      return getCurrentNumMappers();
    else
      return getCurrentNumReducers();
    }

  /**
   * Method setReporter sets the reporter of this HadoopFlowProcess object.
   *
   * @param reporter the reporter of this HadoopFlowProcess object.
   */
  public void setReporter( Reporter reporter )
    {
    this.reporter = reporter;
    }

  /**
   * Method getReporter returns the reporter of this HadoopFlowProcess object.
   *
   * @return the reporter (type Reporter) of this HadoopFlowProcess object.
   */
  public Reporter getReporter()
    {
    return reporter;
    }

  private final Reporter getReporterOrFail()
    {
    if( reporter == null )
      throw new IllegalStateException( "unable to access the hadoop reporter, it is not available until the first map/reduce invocation" );

    return reporter;
    }

  public void setOutputCollector( OutputCollector outputCollector )
    {
    this.outputCollector = outputCollector;
    }

  public OutputCollector getOutputCollector()
    {
    return outputCollector;
    }

  /** @see cascading.flow.FlowProcess#getProperty(String) */
  public Object getProperty( String key )
    {
    return jobConf.get( key );
    }

  /** @see cascading.flow.FlowProcess#keepAlive() */
  public void keepAlive()
    {
    getReporterOrFail().progress();
    }

  /** @see cascading.flow.FlowProcess#increment(Enum, int) */
  public void increment( Enum counter, int amount )
    {
    getReporterOrFail().incrCounter( counter, amount );
    }

  /** @see cascading.flow.FlowProcess#increment(String, String, int) */
  public void increment( String group, String counter, int amount )
    {
    getReporterOrFail().incrCounter( group, counter, amount );
    }

  /** @see cascading.flow.FlowProcess#setStatus(String) */
  public void setStatus( String status )
    {
    getReporterOrFail().setStatus( status );
    }

  /** @see cascading.flow.FlowProcess#isCounterStatusInitialized() */
  public boolean isCounterStatusInitialized()
    {
    return getReporter() != null;
    }

  /** @see cascading.flow.FlowProcess#openTapForRead(Tap) */
  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return tap.openForRead( this );
    }

  /** @see cascading.flow.FlowProcess#openTapForWrite(Tap) */
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( this, outputCollector );
    }

  @Override
  public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
    {
    JobConf jobConf = new JobConf( getJobConf() );

    int id = jobConf.getInt( "cascading.flow.step.id", 0 );
    String partname;

    if( jobConf.getBoolean( "mapred.task.is.map", true ) )
      partname = String.format( "-m-%05d-", id );
    else
      partname = String.format( "-r-%05d-", id );

    jobConf.set( "cascading.tapcollector.partname", "%s%spart" + partname + "%05d" );

    return trap.openForWrite( new HadoopFlowProcess( this, jobConf ), null );
    }

  @Override
  public TupleEntryCollector openSystemIntermediateForWrite() throws IOException
    {
    return new TupleEntryCollector( Fields.size( 2 ) )
    {
    @Override
    protected void collect( TupleEntry tupleEntry )
      {
      try
        {
        getOutputCollector().collect( tupleEntry.get( 0 ), tupleEntry.get( 1 ) );
        }
      catch( IOException exception )
        {
        throw new CascadingException( "failed collecting key and value", exception );
        }
      }
    };
    }

  @Override
  public SpillableTupleList createSpillableTupleList()
    {
    return new HadoopSpillableTupleList( getLong( SPILL_THRESHOLD, defaultThreshold ), jobConf, codec, this );
    }

  private long getLong( String key, long defaultValue )
    {
    String value = (String) getProperty( key );

    if( value == null || value.length() == 0 )
      return defaultValue;

    return Long.parseLong( value );
    }

  public CompressionCodec getCompressionCodec()
    {
    String compress = (String) getProperty( SPILL_COMPRESS );

    if( compress != null && !Boolean.parseBoolean( compress ) )
      return null;

    String codecs = (String) getProperty( SPILL_CODECS );

    if( codecs == null || codecs.length() == 0 )
      codecs = defaultCodecs;

    Class<? extends CompressionCodec> codecClass = null;

    for( String codec : codecs.split( "[,\\s]+" ) )
      {
      try
        {
        LOG.info( "attempting to load codec: {}", codec );
        codecClass = Thread.currentThread().getContextClassLoader().loadClass( codec ).asSubclass( CompressionCodec.class );

        if( codecClass != null )
          {
          LOG.info( "found codec: {}", codec );

          break;
          }
        }
      catch( ClassNotFoundException exception )
        {
        // do nothing
        }
      }

    if( codecClass == null )
      {
      LOG.warn( "codecs set, but unable to load any: {}", codecs );
      return null;
      }

    return ReflectionUtils.newInstance( codecClass, jobConf );
    }

  @Override
  public JobConf copyConfig( JobConf jobConf )
    {
    return new JobConf( jobConf );
    }

  @Override
  public Map<String, String> diffConfigIntoMap( JobConf defaultConfig, JobConf updatedConfig )
    {
    return HadoopUtil.getConfig( defaultConfig, updatedConfig );
    }

  @Override
  public JobConf mergeMapIntoConfig( JobConf defaultConfig, Map<String, String> map )
    {
    return HadoopUtil.mergeConf( defaultConfig, map, false );
    }
  }
