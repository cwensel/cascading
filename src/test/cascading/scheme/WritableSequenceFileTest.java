/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;

import cascading.ClusterTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WritableSequenceFileTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String outputPath = "build/test/output/writablesequence/";

  public WritableSequenceFileTest()
    {
    super( "use tap collector tests", true );
    }

  public void testWritable() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "keyvalue" );

    pipe = new Each( pipe, new Fields( "offset" ), new ExpressionFunction( Fields.ARGS, "new org.apache.hadoop.io.LongWritable($0)", long.class ), Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new ExpressionFunction( Fields.ARGS, "new org.apache.hadoop.io.Text($0)", String.class ), Fields.REPLACE );

    Tap tapKeyValue = new Hfs( new WritableSequenceFile( new Fields( "offset", "line" ), LongWritable.class, Text.class ), outputPath + "/keyvalue", true );
    Tap tapKey = new Hfs( new WritableSequenceFile( new Fields( "offset" ), LongWritable.class, null ), outputPath + "/key", true );
    Tap tapValue = new Hfs( new WritableSequenceFile( new Fields( "line" ), Text.class ), outputPath + "/value", true );

    Flow flowKeyValue = new FlowConnector( getProperties() ).connect( source, tapKeyValue, pipe );
    Flow flowKey = new FlowConnector( getProperties() ).connect( tapKeyValue, tapKey, new Pipe( "key" ) );
    Flow flowValue = new FlowConnector( getProperties() ).connect( tapKeyValue, tapValue, new Pipe( "value" ) );

    Cascade cascade = new CascadeConnector().connect( "keyvalues", flowKeyValue, flowKey, flowValue );

    cascade.complete();

    validateLength( flowKeyValue, 10, 2 );
    validateLength( flowKey, 10, 1 );
    validateLength( flowValue, 10, 1 );
    }

  }