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

package cascading.tap.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import cascading.flow.FlowProcess;
import cascading.tap.SourceTap;
import cascading.tap.hadoop.io.CombineFileRecordReaderWrapper;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tap.type.FileType;
import cascading.tuple.TupleEntryIterator;

/**
 * CombinedHfs can be used in lieu of {@link Hfs} when a number of input files should be combined to form bigger
 * hadoop splits. Like {@link Hfs}, CombinedHfs may only be used with the
 * {@link cascading.flow.hadoop.HadoopFlowConnector} when creating Hadoop executable {@link cascading.flow.Flow}
 * instances.
 * <p/>
 * A CombinedHfs is a wrapper around {@link Hfs}. Create an {@link Hfs} instance, and wrap the {@link Hfs}
 * instance with the appropriate input format class that should be used to handle individual files.
 *
 * @see org.apache.hadoop.mapred.lib.CombineFileInputFormat
 */
public class CombinedHfs extends SourceTap<JobConf,RecordReader> implements FileType<JobConf>
  {
  private final Hfs hfs;
  private final Class<? extends InputFormat> individualInputFormatCls;

  /**
   * Constructor that wraps an {@link Hfs} instance to create a CombinedHfs instance.
   *
   * @param hfs the {@link Hfs} instance to be wrapped
   * @param individualInputFormatCls the input format class for handling individual input files
   */
  public CombinedHfs( Hfs hfs, Class<? extends InputFormat> individualInputFormatCls )
    {
    super( hfs.getScheme() );
    this.hfs = hfs;
    this.individualInputFormatCls = individualInputFormatCls;
    }

  public String getIdentifier()
    {
    return hfs.getIdentifier();
    }

  public TupleEntryIterator openForRead( FlowProcess<JobConf> flowProcess, RecordReader input )
    throws IOException
    {
    return new HadoopTupleEntrySchemeIterator( flowProcess, this, input );
    }

  public boolean resourceExists( JobConf conf ) throws IOException
    {
    return hfs.resourceExists( conf );
    }

  public long getModifiedTime( JobConf conf ) throws IOException
    {
    return hfs.getModifiedTime( conf );
    }

  @Override
  public void sourceConfInit( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    // let the hfs does its thing first
    hfs.sourceConfInit( flowProcess, conf );
    // set the individual input format onto the job conf so that it can be used by CombinedInputFormat
    conf.set( CombineFileRecordReaderWrapper.INDIVIDUAL_INPUT_FORMAT, individualInputFormatCls.getName() );

    // override the input format class
    conf.setInputFormat( CombinedInputFormat.class );
    }

  @Override
  public String getFullIdentifier( JobConf conf )
    {
    return hfs.getFullIdentifier( conf );
    }

  public boolean isDirectory( JobConf conf ) throws IOException
    {
    return hfs.isDirectory( conf );
    }

  public String[] getChildIdentifiers( JobConf conf ) throws IOException
    {
    return hfs.getChildIdentifiers( conf );
    }

  public long getSize( JobConf conf ) throws IOException
    {
    return hfs.getSize( conf );
    }

  /**
   * Returns the underlying Hfs instance.
   */
  public Hfs getHfs()
    {
    return hfs;
    }
  }

/**
 * Combined input format that uses the underlying individual input format to combine multiple files into a single split.
 */
class CombinedInputFormat extends CombineFileInputFormat
  {
  public RecordReader getRecordReader( InputSplit split, JobConf job, Reporter reporter )
    throws IOException
    {
    return new CombineFileRecordReader( job, (CombineFileSplit) split, reporter, CombineFileRecordReaderWrapper.class );
    }
  }