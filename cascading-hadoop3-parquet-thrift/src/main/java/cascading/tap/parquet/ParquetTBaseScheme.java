/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.thrift.TBaseWriteSupport;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;
import org.apache.parquet.thrift.TBaseRecordConverter;
import org.apache.thrift.TBase;

@Deprecated
public class ParquetTBaseScheme<T extends TBase<?, ?>> extends ParquetValueScheme<T>
  {

  // In the case of reads, we can read the thrift class from the file metadata
  public ParquetTBaseScheme()
    {
    this( new Config<T>() );
    }

  public ParquetTBaseScheme( Class<T> thriftClass )
    {
    this( new Config<T>().withRecordClass( thriftClass ) );
    }

  public ParquetTBaseScheme( FilterPredicate filterPredicate )
    {
    this( new Config<T>().withFilterPredicate( filterPredicate ) );
    }

  public ParquetTBaseScheme( FilterPredicate filterPredicate, Class<T> thriftClass )
    {
    this( new Config<T>().withRecordClass( thriftClass ).withFilterPredicate( filterPredicate ) );
    }

  public ParquetTBaseScheme( Config<T> config )
    {
    super( config );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends JobConf> fp,
                              Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf )
    {
    super.sourceConfInit( fp, tap, jobConf );
    jobConf.setInputFormat( DeprecatedParquetInputFormat.class );
    ParquetInputFormat.setReadSupportClass( jobConf, ThriftReadSupport.class );
    ThriftReadSupport.setRecordConverterClass( jobConf, TBaseRecordConverter.class );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends JobConf> fp,
                            Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf )
    {

    if( this.config.getKlass() == null )
      {
      throw new IllegalArgumentException( "To use ParquetTBaseScheme as a sink, you must specify a thrift class in the constructor" );
      }

    DeprecatedParquetOutputFormat.setAsOutputFormat( jobConf );
    DeprecatedParquetOutputFormat.setWriteSupportClass( jobConf, TBaseWriteSupport.class );
    TBaseWriteSupport.<T>setThriftClass( jobConf, this.config.getKlass() );
    }
  }
