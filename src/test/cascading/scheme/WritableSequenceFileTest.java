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

package cascading.scheme;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import static data.InputData.inputFileApache;

@PlatformTest(platforms = {"hadoop"})
public class WritableSequenceFileTest extends PlatformTestCase
  {
  public WritableSequenceFileTest()
    {
    super( true );
    }

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

    Flow flowKeyValue = new HadoopFlowConnector( getProperties() ).connect( source, tapKeyValue, pipe );
    Flow flowKey = new HadoopFlowConnector( getProperties() ).connect( tapKeyValue, tapKey, new Pipe( "key" ) );
    Flow flowValue = new HadoopFlowConnector( getProperties() ).connect( tapKeyValue, tapValue, new Pipe( "value" ) );

    Cascade cascade = new CascadeConnector().connect( "keyvalues", flowKeyValue, flowKey, flowValue );

    cascade.complete();

    validateLength( flowKeyValue, 10, 2 );
    validateLength( flowKey, 10, 1 );
    validateLength( flowValue, 10, 1 );
    }

  }