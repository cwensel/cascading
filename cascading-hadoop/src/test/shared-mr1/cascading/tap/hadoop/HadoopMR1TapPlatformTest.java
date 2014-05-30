/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import java.io.Serializable;

import cascading.PlatformTestCase;
import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import data.InputData;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import static data.InputData.inputFileLower;
import static data.InputData.inputFileUpper;

/**
 *
 */
public class HadoopMR1TapPlatformTest extends PlatformTestCase implements Serializable
  {
  public HadoopMR1TapPlatformTest()
    {
    super( true );
    }

  @Test
  public void testCombinedHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Hfs sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputFileLower );
    Hfs sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputFileUpper );

    // create a CombinedHfs instance on these files
    Tap source = new MultiSourceTap<Hfs, JobConf, RecordReader>( sourceLower, sourceUpper );

    FlowProcess<JobConf> process = getPlatform().getFlowProcess();
    JobConf conf = process.getConfigCopy();

    // set the combine flag
    conf.setBoolean( HfsProps.COMBINE_INPUT_FILES, true );

    // test the input format and the split
    source.sourceConfInit( process, conf );

    InputFormat inputFormat = conf.getInputFormat();

    assertEquals( Hfs.CombinedInputFormat.class, inputFormat.getClass() );
    InputSplit[] splits = inputFormat.getSplits( conf, 1 );

    assertEquals( 1, splits.length );

    validateLength( source.openForRead( process ), 10 );
    }
  }
