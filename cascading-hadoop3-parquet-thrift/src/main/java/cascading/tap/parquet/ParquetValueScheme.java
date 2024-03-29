/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tap.parquet;

import java.io.IOException;
import java.io.Serializable;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.mapred.Container;
import org.apache.parquet.hadoop.thrift.ParquetThriftInputFormat;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * A Cascading Scheme that returns a simple Tuple with a single value, the "value" object
 * coming out of the underlying InputFormat.
 * <p>
 * This is an abstract class; implementations are expected to set up their Input/Output Formats
 * correctly in the respective Init methods.
 */
public abstract class ParquetValueScheme<T> extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>
  {

  public static final class Config<T> implements Serializable
    {
    private final FilterPredicate filterPredicate;
    private final String deprecatedProjectionString;
    private final String strictProjectionString;
    private final Class<T> klass;

    private Config( Class<T> klass, FilterPredicate filterPredicate, String deprecatedProjectionString, String strictProjectionString )
      {
      this.filterPredicate = filterPredicate;
      this.deprecatedProjectionString = deprecatedProjectionString;
      this.strictProjectionString = strictProjectionString;
      this.klass = klass;
      }

    public Config()
      {
      filterPredicate = null;
      deprecatedProjectionString = null;
      strictProjectionString = null;
      klass = null;
      }

    public FilterPredicate getFilterPredicate()
      {
      return filterPredicate;
      }

    @Deprecated
    public String getProjectionString()
      {
      return deprecatedProjectionString;
      }

    public String getStrictProjectionString()
      {
      return strictProjectionString;
      }

    public Class<T> getKlass()
      {
      return klass;
      }

    public Config<T> withFilterPredicate( FilterPredicate f )
      {
      return new Config<T>( this.klass, checkNotNull( f, "filterPredicate" ), this.deprecatedProjectionString, this.strictProjectionString );
      }

    @Deprecated
    public Config<T> withProjectionString( String p )
      {
      return new Config<T>( this.klass, this.filterPredicate, checkNotNull( p, "projectionString" ), this.strictProjectionString );
      }

    public Config<T> withStrictProjectionString( String p )
      {
      return new Config<T>( this.klass, this.filterPredicate, this.deprecatedProjectionString, checkNotNull( p, "projectionString" ) );
      }

    public Config<T> withRecordClass( Class<T> klass )
      {
      return new Config<T>( checkNotNull( klass, "recordClass" ), this.filterPredicate, this.deprecatedProjectionString, this.strictProjectionString );
      }
    }

  private static final long serialVersionUID = 157560846420730043L;
  protected final Config<T> config;

  public ParquetValueScheme()
    {
    this( new Config<T>() );
    }

  public ParquetValueScheme( FilterPredicate filterPredicate )
    {
    this( new Config<T>().withFilterPredicate( filterPredicate ) );
    }

  public ParquetValueScheme( Config<T> config )
    {
    this.config = config;
    }

  @Deprecated
  private void setProjectionPushdown( JobConf jobConf )
    {
    if( this.config.deprecatedProjectionString != null )
      {
      ThriftReadSupport.setProjectionPushdown( jobConf, this.config.deprecatedProjectionString );
      }
    }

  private void setStrictProjectionPushdown( JobConf jobConf )
    {
    if( this.config.strictProjectionString != null )
      {
      ThriftReadSupport.setStrictFieldProjectionFilter( jobConf, this.config.strictProjectionString );
      }
    }

  private void setPredicatePushdown( JobConf jobConf )
    {
    if( this.config.filterPredicate != null )
      {
      ParquetInputFormat.setFilterPredicate( jobConf, this.config.filterPredicate );
      }
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap, JobConf jobConf )
    {
    setPredicatePushdown( jobConf );
    setProjectionPushdown( jobConf );
    setStrictProjectionPushdown( jobConf );
    setRecordClass( jobConf );
    }

  private void setRecordClass( JobConf jobConf )
    {
    if( config.klass != null )
      {
      ParquetThriftInputFormat.setThriftClass( jobConf, config.klass );
      }
    }

  @SuppressWarnings("unchecked")
  @Override
  public boolean source( FlowProcess<? extends JobConf> fp, SourceCall<Object[], RecordReader> sc )
    throws IOException
    {
    Container<T> value = (Container<T>) sc.getInput().createValue();
    boolean hasNext = sc.getInput().next( null, value );
    if( !hasNext ){return false;}

    // Skip nulls
    if( value == null ){return true;}

    sc.getIncomingEntry().setTuple( new Tuple( value.get() ) );
    return true;
    }

  @SuppressWarnings("unchecked")
  @Override
  public void sink( FlowProcess<? extends JobConf> fp, SinkCall<Object[], OutputCollector> sc )
    throws IOException
    {
    TupleEntry tuple = sc.getOutgoingEntry();

    if( tuple.size() != 1 )
      {
      throw new RuntimeException( "ParquetValueScheme expects tuples with an arity of exactly 1, but found " + tuple.getFields() );
      }

    T value = (T) tuple.getObject( 0 );
    OutputCollector output = sc.getOutput();
    output.collect( null, value );
    }

  }
