/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.scheme.hadoop;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static data.InputData.inputFileApache;

public class WritableSequenceFilePlatformTest extends PlatformTestCase
  {
  public WritableSequenceFilePlatformTest()
    {
    super( true );
    }

  @Test
  public void testWritable() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "keyvalue" );

    pipe = new Each( pipe, new Fields( "offset" ), new ExpressionFunction( Fields.ARGS, "new org.apache.hadoop.io.LongWritable($0)", long.class ), Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new ExpressionFunction( Fields.ARGS, "new org.apache.hadoop.io.Text($0)", String.class ), Fields.REPLACE );

    Tap tapKeyValue = new Hfs( new WritableSequenceFile( new Fields( "offset", "line" ), LongWritable.class, Text.class ), getOutputPath( "keyvalue" ), SinkMode.REPLACE );
    Tap tapKey = new Hfs( new WritableSequenceFile( new Fields( "offset" ), LongWritable.class, null ), getOutputPath( "key" ), SinkMode.REPLACE );
    Tap tapValue = new Hfs( new WritableSequenceFile( new Fields( "line" ), Text.class ), getOutputPath( "value" ), SinkMode.REPLACE );

    Flow flowKeyValue = getPlatform().getFlowConnector( getProperties() ).connect( source, tapKeyValue, pipe );
    Flow flowKey = getPlatform().getFlowConnector( getProperties() ).connect( tapKeyValue, tapKey, new Pipe( "key" ) );
    Flow flowValue = getPlatform().getFlowConnector( getProperties() ).connect( tapKeyValue, tapValue, new Pipe( "value" ) );

    Cascade cascade = new CascadeConnector( getProperties() ).connect( "keyvalues", flowKeyValue, flowKey, flowValue );

    cascade.complete();

    validateLength( flowKeyValue, 10, 2 );
    validateLength( flowKey, 10, 1 );
    validateLength( flowValue, 10, 1 );
    }
  }