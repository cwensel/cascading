/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;

/** @version $Id: //depot/calku/cascading/src/test/cascading/FieldedPipesTest.java#4 $ */
public class SerializedPipesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";

  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";

  String outputPath = "build/test/output/tuple/";

  public static class InsertBytes extends BaseOperation implements Function
    {
    String asBytes;

    public InsertBytes( Fields fieldDeclaration, String asBytes )
      {
      super( fieldDeclaration );
      this.asBytes = asBytes;
      }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      functionCall.getOutputCollector().add( new Tuple( new BytesWritable( asBytes.getBytes() ) ) );
      }
    }

  public static class InsertBoolean extends BaseOperation implements Function
    {
    boolean asBoolean;

    public InsertBoolean( Fields fieldDeclaration, boolean asBoolean )
      {
      super( fieldDeclaration );
      this.asBoolean = asBoolean;
      }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      functionCall.getOutputCollector().add( new Tuple( new BooleanWritable( asBoolean ) ) );
      }
    }

  public SerializedPipesTest()
    {
    super( "serialized pipes", true ); // leave cluster testing enabled
    }

  public void testSimpleGroup() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Each( pipe, new InsertBytes( new Fields( "bytes" ), "inserted text as bytes" ), Fields.ALL );

    pipe = new Group( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new InsertBoolean( new Fields( "boolean" ), false ), Fields.ALL );

    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), outputPath + "/hadoop/serialization", true );

    Map<Object, Object> jobProperties = getProperties();

    TupleSerialization.addSerializationToken( jobProperties, 1000, BooleanWritable.class.getName() );

    Flow flow = new FlowConnector( jobProperties ).connect( source, sink, pipe );

//    flow.writeDOT( "groupcount.dot" );

    flow.complete();

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 8, null );
    }

  public void testCoGroupWritableAsKeyValue() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), outputPath + "/hadoop/writablekeyvalue", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Each( pipeLower, new InsertBytes( new Fields( "group" ), "inserted text as bytes" ), Fields.ALL );
    pipeLower = new Each( pipeLower, new InsertBytes( new Fields( "value" ), "inserted text as bytes" ), Fields.ALL );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new InsertBytes( new Fields( "group" ), "inserted text as bytes" ), Fields.ALL );
    pipeUpper = new Each( pipeUpper, new InsertBytes( new Fields( "value" ), "inserted text as bytes" ), Fields.ALL );

    Pipe splice = new CoGroup( pipeLower, new Fields( "group" ), pipeUpper, new Fields( "group" ), Fields.size( 8 ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 25, null );
    }
  }