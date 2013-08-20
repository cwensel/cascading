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

package cascading.tuple.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.collect.SpillableTupleList;
import cascading.tuple.hadoop.util.BytesComparator;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static data.InputData.*;


public class SerializedPipesPlatformTest extends PlatformTestCase
  {
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

  public static class ReplaceAsBytes extends BaseOperation implements Function
    {
    public ReplaceAsBytes( Fields fieldDeclaration )
      {
      super( fieldDeclaration );
      }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      functionCall.getOutputCollector().add( new Tuple( new BytesWritable( functionCall.getArguments().getString( 0 ).getBytes() ) ) );
      }
    }

  public static class InsertRawBytes extends BaseOperation<Long> implements Function<Long>
    {
    String asBytes;
    private boolean increment = false;
    private boolean randomIncrement = false;

    public InsertRawBytes( Fields fieldDeclaration, String asBytes, boolean increment, boolean randomIncrement )
      {
      super( fieldDeclaration );
      this.asBytes = asBytes;
      this.increment = increment;
      this.randomIncrement = randomIncrement;
      }

    public InsertRawBytes( Fields fieldDeclaration, String asBytes, boolean increment )
      {
      super( fieldDeclaration );
      this.asBytes = asBytes;
      this.increment = increment;
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<Long> operationCall )
      {
      operationCall.setContext( increment ? getIncrement( 0L ) : -1L );
      }

    private long getIncrement( long value )
      {
      if( randomIncrement )
        return value + (long) ( Math.random() * new Object().hashCode() );

      return value + 1;
      }

    public void operate( FlowProcess flowProcess, FunctionCall<Long> functionCall )
      {
      String string = asBytes;

      if( functionCall.getContext() != -1 )
        {
        string = functionCall.getContext() + string;
        functionCall.setContext( functionCall.getContext() + 1 );
        }

      functionCall.getOutputCollector().add( new Tuple( (Object) string.getBytes() ) );
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

  public static class Container implements Serializable, Comparable<String>
    {
    String value;

    public Container( String value )
      {
      this.value = value;
      }

    @Override
    public int compareTo( String o )
      {
      return value.compareTo( o );
      }
    }

  public static class InsertTestText extends BaseOperation<Long> implements Function<Long>
    {
    private String testText;
    private boolean increment;
    private int moduloValueIsNull;
    private int moduloResultIsNull;

    public InsertTestText( Fields fieldDeclaration, String testText, boolean increment )
      {
      this( fieldDeclaration, testText, increment, -1, -1 );
      }

    public InsertTestText( Fields fieldDeclaration, String testText, boolean increment, int moduloValueIsNull, int moduloResultIsNull )
      {
      super( fieldDeclaration );
      this.testText = testText;
      this.increment = increment;
      this.moduloValueIsNull = moduloValueIsNull;
      this.moduloResultIsNull = moduloResultIsNull;
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<Long> operationCall )
      {
      operationCall.setContext( increment ? 0L : -1L );
      }

    public void operate( FlowProcess flowProcess, FunctionCall<Long> functionCall )
      {
      String string = testText;

      if( functionCall.getContext() != -1 )
        {
        string = functionCall.getContext() + string;

        functionCall.setContext( functionCall.getContext() + 1 );

        if( moduloValueIsNull != -1 && functionCall.getContext() % moduloValueIsNull == 0 )
          string = null;
        }

      TestText result = null;

      if( moduloResultIsNull != -1 && functionCall.getContext() % moduloResultIsNull != 0 )
        result = new TestText( string );

      functionCall.getOutputCollector().add( new Tuple( result ) );
      }
    }

  public SerializedPipesPlatformTest()
    {
    super( true, 4, 2 ); // leave cluster testing enabled, reducers > 1 to test bytes hasher
    }

  @Test
  public void testSimpleGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Each( pipe, new InsertBytes( new Fields( "bytes" ), "inserted text as bytes" ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new InsertBoolean( new Fields( "boolean" ), false ), Fields.ALL );

    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), getOutputPath( "serialization" ), SinkMode.REPLACE );

    Map<Object, Object> jobProperties = getProperties();

    TupleSerialization.addSerializationToken( jobProperties, 1000, BooleanWritable.class.getName() );

    Flow flow = new HadoopFlowConnector( jobProperties ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 8, null );
    }

  @Test
  public void testSimpleGroupOnBytes() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Each( pipe, new InsertRawBytes( new Fields( "bytes" ), "inserted text as bytes", true, true ), Fields.ALL );

    Fields bytes = new Fields( "bytes" );
    bytes.setComparator( "bytes", new BytesComparator() );
    pipe = new GroupBy( pipe, bytes );

    pipe = new Every( pipe, new Count(), new Fields( "bytes", "count" ) );

    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), getOutputPath( "grouponbytes" ), SinkMode.REPLACE );

    Map<Object, Object> properties = getProperties();

    TupleSerializationProps.addSerialization( properties, BytesSerialization.class.getName() );

    Flow flow = new HadoopFlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10 ); // 10 unique counts
    }

  @Test
  public void testCoGroupWritableAsKeyValue() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), getOutputPath( "writablekeyvalue" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Each( pipeLower, new InsertBytes( new Fields( "group" ), "inserted text as bytes" ), Fields.ALL );
    pipeLower = new Each( pipeLower, new InsertBytes( new Fields( "value" ), "inserted text as bytes" ), Fields.ALL );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new InsertBytes( new Fields( "group" ), "inserted text as bytes" ), Fields.ALL );
    pipeUpper = new Each( pipeUpper, new InsertBytes( new Fields( "value" ), "inserted text as bytes" ), Fields.ALL );

    Pipe splice = new CoGroup( pipeLower, new Fields( "group" ), pipeUpper, new Fields( "group" ), Fields.size( 8 ) );

    Flow countFlow = new HadoopFlowConnector( getProperties() ).connect( sources, sink, splice );

    countFlow.complete();

    validateLength( countFlow, 25 );
    }

  @Test
  public void testCoGroupBytesWritableAsKeyValue() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine( new Fields( "line" ) ), getOutputPath( "byteswritablekeyvalue" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Each( pipeLower, new Fields( "char" ), new ReplaceAsBytes( new Fields( "char" ) ), Fields.REPLACE );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new Fields( "char" ), new ReplaceAsBytes( new Fields( "char" ) ), Fields.REPLACE );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow flow = new HadoopFlowConnector( getProperties() ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\t61\t1\t41" ) ) );
    }

  @Test
  public void testCoGroupSpillCustomWritable() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), getOutputPath( "customerwritable" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Each( pipeLower, new InsertTestText( new Fields( "group" ), "inserted text as bytes", false ), Fields.ALL );
    pipeLower = new Each( pipeLower, new InsertTestText( new Fields( "value" ), "inserted text as bytes", false ), Fields.ALL );
    pipeLower = new Each( pipeLower, new InsertTestText( new Fields( "text" ), "inserted text as custom text", false ), Fields.ALL );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new InsertTestText( new Fields( "group" ), "inserted text as bytes", false ), Fields.ALL );
    pipeUpper = new Each( pipeUpper, new InsertTestText( new Fields( "value" ), "inserted text as bytes", false ), Fields.ALL );
    pipeUpper = new Each( pipeUpper, new InsertTestText( new Fields( "text" ), "inserted text as custom text", false ), Fields.ALL );

    Pipe splice = new CoGroup( pipeLower, new Fields( "group" ), pipeUpper, new Fields( "group" ), Fields.size( 10 ) );

    Map<Object, Object> properties = getProperties();

    properties.put( SpillableTupleList.SPILL_THRESHOLD, 1 );
//    String serializations = MultiMapReducePlanner.getJobConf( properties ).get( "io.serializations" );
//    serializations = Util.join( ",", serializations, JavaSerialization.class.getName() );
//    System.out.println( "serializations = " + serializations );
//    MultiMapReducePlanner.getJobConf( properties ).set( "io.serializations",serializations );
    properties.put( "io.serializations", TestSerialization.class.getName() );

    Flow flow = new HadoopFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 25 );
    }

  @Test
  public void testCoGroupRawAsKeyValue() throws Exception
    {
    invokeRawAsKeyValue( false, true, false, false );
    }

  @Test
  public void testCoGroupRawAsKeyValueDefault() throws Exception
    {
    invokeRawAsKeyValue( true, true, false, false );
    }

  @Test
  public void testCoGroupRawAsKeyValueDefaultIgnoreToken() throws Exception
    {
    invokeRawAsKeyValue( true, true, true, false );
    }

  @Test
  public void testCoGroupRawAsKeyValueDefaultIgnoreTokenCompositeGrouping() throws Exception
    {
    invokeRawAsKeyValue( true, true, true, true );
    }

  @Test
  public void testCoGroupRawAsKeyValueNoSecondary() throws Exception
    {
    invokeRawAsKeyValue( false, false, false, false );
    }

  @Test
  public void testCoGroupRawAsKeyValueDefaultNoSecondary() throws Exception
    {
    invokeRawAsKeyValue( true, false, false, false );
    }

  @Test
  public void testCoGroupRawAsKeyValueDefaultNoSecondaryCompositeGrouping() throws Exception
    {
    invokeRawAsKeyValue( true, false, false, true );
    }

  private void invokeRawAsKeyValue( boolean useDefaultComparator, boolean secondarySortOnValue, boolean ignoreSerializationToken, boolean compositeGrouping )
    throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Fields fields = new Fields( "num", "char", "group", "value", "num2", "char2", "group2", "value2" );
    Tap sink = new Hfs( new SequenceFile( fields ), getOutputPath( "/rawbyteskeyvalue/" + useDefaultComparator + "/" + secondarySortOnValue + "/" + ignoreSerializationToken + "/" + compositeGrouping ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Each( pipeLower, new InsertTestText( new Fields( "group" ), "inserted text as bytes", true, 3, 4 ), Fields.ALL );
    pipeLower = new Each( pipeLower, new InsertRawBytes( new Fields( "value" ), "inserted text as bytes", true ), Fields.ALL );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new InsertTestText( new Fields( "group" ), "inserted text as bytes", true, 3, 4 ), Fields.ALL );
    pipeUpper = new Each( pipeUpper, new InsertRawBytes( new Fields( "value" ), "inserted text as bytes", true ), Fields.ALL );

    Fields groupFields = new Fields( "group" );

    if( compositeGrouping )
      groupFields = new Fields( "group", "num" );

    if( !useDefaultComparator )
      groupFields.setComparator( "group", new TestTextComparator() );

    Fields declaredFields = new Fields( "num", "char", "group", "value", "num2", "char2", "group2", "value2" );
    Pipe splice = new CoGroup( pipeLower, groupFields, pipeUpper, groupFields, declaredFields );

    // test sorting comparison
    Fields valueFields = new Fields( "value" );

    if( !useDefaultComparator )
      valueFields.setComparator( "value", new BytesComparator() );

    if( secondarySortOnValue )
      splice = new GroupBy( splice, groupFields, valueFields );
    else
      splice = new GroupBy( splice, groupFields );

    Map<Object, Object> properties = getProperties();

    if( !ignoreSerializationToken )
      {
      TupleSerialization.addSerialization( properties, TestSerialization.class.getName() );
      TupleSerialization.addSerialization( properties, BytesSerialization.class.getName() );
      }
    else
      {
      TupleSerialization.addSerialization( properties, NoTokenTestSerialization.class.getName() );
      TupleSerialization.addSerialization( properties, NoTokenTestBytesSerialization.class.getName() );
      }

    properties.put( "mapred.map.tasks", 1 );

    Flow flow = new HadoopFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    // test the ordering
    TupleEntryIterator iterator = flow.openSink();
    TestText target = (TestText) iterator.next().getObject( "group" );
    String value = target == null ? null : target.value;
//    System.out.println( "value = " + value );

    while( iterator.hasNext() )
      {
      TestText nextTarget = (TestText) iterator.next().getObject( "group" );
      String next = nextTarget == null ? null : nextTarget.value;

      if( value != null && value.compareTo( next ) >= 0 )
        fail( "not increasing: " + value + " " + value );

      value = next;
//      System.out.println( "value = " + value );
      }

    iterator.close();
    }

  @Test
  public void testBigDecimal() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Fields sourceFields = new Fields( "offset", "line" ).applyTypes( Coercions.BIG_DECIMAL, String.class );

    Tap source = new Hfs( new TextLine( sourceFields ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "offset", "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "offset" ) );

    pipe = new Every( pipe, new Count(), new Fields( "offset", "count" ) );

    Fields sinkFields = new Fields("offset","count").applyTypes( Coercions.BIG_DECIMAL, long.class );
    Tap sink = new Hfs( new SequenceFile( sinkFields ), getOutputPath( "bigdecimal" ), SinkMode.REPLACE );

    Map<Object, Object> jobProperties = getProperties();

    TupleSerialization.addSerialization( jobProperties, BigDecimalSerialization.class.getName() );

    Flow flow = new HadoopFlowConnector( jobProperties ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10 );
    }

  }