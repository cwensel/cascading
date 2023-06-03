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

package cascading;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.stream.graph.StreamGraph;
import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleListCollector;
import cascading.util.Util;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

/**
 * Class CascadingTestCase is the base class for all Cascading tests.
 * <p>
 * It included a few helpful utility methods for testing Cascading applications.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public abstract class CascadingTestCase extends TestCase implements Serializable
  {
  public static final String ROOT_OUTPUT_PATH = "test.output.root";
  public static final String ROOT_PLAN_PATH = "test.plan.root";
  public static final String TEST_TRACEPLAN_ENABLED = "test.traceplan.enabled";

  private String outputPath;
  private String planPath;

  @Rule
  public transient TestName name = new TestName();

  static class TestFlowProcess extends FlowProcess.NullFlowProcess
    {
    private final Map<Object, Object> properties;

    public TestFlowProcess( Map<Object, Object> properties )
      {
      this.properties = properties;
      }

    @Override
    public Object getProperty( String key )
      {
      return properties.get( key );
      }
    }

  public CascadingTestCase()
    {
    }

  public CascadingTestCase( String name )
    {
    super( name );
    }

  @Override
  @Before
  public void setUp() throws Exception
    {
    super.setUp();

    if( Boolean.getBoolean( TEST_TRACEPLAN_ENABLED ) )
      {
      System.setProperty( FlowPlanner.TRACE_PLAN_PATH, Util.join( "/", getPlanPath(), "planner" ) );
      System.setProperty( FlowPlanner.TRACE_PLAN_TRANSFORM_PATH, Util.join( "/", getPlanPath(), "planner" ) );
      System.setProperty( FlowPlanner.TRACE_STATS_PATH, Util.join( "/", getPlanPath(), "planner" ) );
      System.setProperty( "platform." + StreamGraph.DOT_FILE_PATH, Util.join( "/", getPlanPath(), "stream" ) ); // pass down
      }
    }

  @Override
  @After
  public void tearDown() throws Exception
    {
    super.tearDown();
    }

  protected static String getTestOutputRoot()
    {
    return System.getProperty( ROOT_OUTPUT_PATH, "build/test/output" ).replace( ":", "_" );
    }

  protected static String getTestPlanRoot()
    {
    return System.getProperty( ROOT_PLAN_PATH, "build/test/plan" ).replace( ":", "_" );
    }

  protected String[] getOutputPathElements()
    {
    return new String[]{getTestOutputRoot(), getTestCaseName(), getTestName()};
    }

  protected String[] getPlanPathElements()
    {
    return new String[]{getTestPlanRoot(), getTestCaseName(), getTestName()};
    }

  protected String getOutputPath()
    {
    if( outputPath == null )
      outputPath = Util.join( getOutputPathElements(), File.separator );

    return outputPath;
    }

  protected String getPlanPath()
    {
    if( planPath == null )
      planPath = Util.join( getPlanPathElements(), File.separator );

    return planPath;
    }

  public String getTestCaseName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)Test.*$", "$1" ).toLowerCase();
    }

  public String getTestName()
    {
    return name.getMethodName();
    }

  public static void validateLength( Flow flow, int numTuples ) throws IOException
    {
    validateLength( flow, numTuples, -1 );
    }

  public static void validateLength( Flow flow, int numTuples, String name ) throws IOException
    {
    validateLength( flow, numTuples, -1, null, name );
    }

  public static void validateLength( Flow flow, int numTuples, int tupleSize ) throws IOException
    {
    validateLength( flow, numTuples, tupleSize, null, null );
    }

  public static void validateLength( Flow flow, int numTuples, int tupleSize, Pattern regex ) throws IOException
    {
    validateLength( flow, numTuples, tupleSize, regex, null );
    }

  public static void validateLength( Flow flow, int numTuples, Pattern regex, String name ) throws IOException
    {
    validateLength( flow, numTuples, -1, regex, name );
    }

  public static void validateLength( Flow flow, int numTuples, int tupleSize, Pattern regex, String name ) throws IOException
    {
    TupleEntryIterator iterator = name == null ? flow.openSink() : flow.openSink( name );
    validateLength( iterator, numTuples, tupleSize, regex );
    }

  public static void validateLength( TupleEntryIterator iterator, int numTuples )
    {
    validateLength( iterator, numTuples, -1, null );
    }

  public static void validateLength( TupleEntryIterator iterator, int numTuples, int tupleSize )
    {
    validateLength( iterator, numTuples, tupleSize, null );
    }

  public static void validateLength( TupleEntryIterator iterator, int numTuples, Pattern regex )
    {
    validateLength( iterator, numTuples, -1, regex );
    }

  public static void validateLength( TupleEntryIterator iterator, int numTuples, int tupleSize, Pattern regex )
    {
    int count = 0;

    while( iterator.hasNext() )
      {
      TupleEntry tupleEntry = iterator.next();

      if( tupleSize != -1 )
        assertEquals( "wrong number of elements", tupleSize, tupleEntry.size() );

      if( regex != null )
        assertTrue( "regex: " + regex + " does not match: " + tupleEntry.getTuple().toString(), regex.matcher( tupleEntry.getTuple().toString() ).matches() );

      count++;
      }

    try
      {
      iterator.close();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( exception );
      }

    assertEquals( "wrong number of lines", numTuples, count );
    }

  public static TupleListCollector invokeFunction( Function function, Tuple arguments, Fields resultFields )
    {
    return CascadingTesting.invokeFunction( function, new TupleEntry( arguments ), resultFields );
    }

  public static TupleListCollector invokeFunction( Function function, Tuple arguments, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFunction( function, new TupleEntry( arguments ), resultFields, properties );
    }

  public static TupleListCollector invokeFunction( Function function, TupleEntry arguments, Fields resultFields )
    {
    return CascadingTesting.invokeFunction( function, arguments, resultFields, new HashMap<Object, Object>() );
    }

  public static TupleListCollector invokeFunction( Function function, TupleEntry arguments, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFunction( function, arguments, resultFields, properties );
    }

  public static TupleListCollector invokeFunction( Function function, Tuple[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeFunction( function, argumentsArray, resultFields );
    }

  public static TupleListCollector invokeFunction( Function function, Tuple[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFunction( function, argumentsArray, resultFields, properties );
    }

  public static TupleListCollector invokeFunction( Function function, TupleEntry[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeFunction( function, argumentsArray, resultFields, new HashMap<>() );
    }

  public static TupleListCollector invokeFunction( Function function, TupleEntry[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFunction( function, argumentsArray, resultFields, properties );
    }

  public static boolean invokeFilter( Filter filter, Tuple arguments )
    {
    return CascadingTesting.invokeFilter( filter, new TupleEntry( arguments ) );
    }

  public static boolean invokeFilter( Filter filter, Tuple arguments, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFilter( filter, new TupleEntry( arguments ), properties );
    }

  public static boolean invokeFilter( Filter filter, TupleEntry arguments )
    {
    return CascadingTesting.invokeFilter( filter, arguments, new HashMap<Object, Object>() );
    }

  public static boolean invokeFilter( Filter filter, TupleEntry arguments, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFilter( filter, arguments, properties );
    }

  public static boolean[] invokeFilter( Filter filter, Tuple[] argumentsArray )
    {
    return CascadingTesting.invokeFilter( filter, argumentsArray);
    }

  public static boolean[] invokeFilter( Filter filter, Tuple[] argumentsArray, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFilter( filter, argumentsArray, properties );
    }

  public static boolean[] invokeFilter( Filter filter, TupleEntry[] argumentsArray )
    {
    return CascadingTesting.invokeFilter( filter, argumentsArray, Collections.emptyMap() );
    }

  public static boolean[] invokeFilter( Filter filter, TupleEntry[] argumentsArray, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeFilter( filter, argumentsArray, properties );
    }

  public static TupleListCollector invokeAggregator( Aggregator aggregator, Tuple[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeAggregator( aggregator, argumentsArray, resultFields );
    }

  public static TupleListCollector invokeAggregator( Aggregator aggregator, Tuple[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeAggregator( aggregator, argumentsArray, resultFields, properties );
    }

  public static TupleListCollector invokeAggregator( Aggregator aggregator, TupleEntry[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeAggregator( aggregator, null, argumentsArray, resultFields );
    }

  public static TupleListCollector invokeAggregator( Aggregator aggregator, TupleEntry[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeAggregator( aggregator, null, argumentsArray, resultFields, properties );
    }

  public static TupleListCollector invokeAggregator( Aggregator aggregator, TupleEntry group, TupleEntry[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeAggregator( aggregator, group, argumentsArray, resultFields, Collections.emptyMap() );
    }

  public static TupleListCollector invokeAggregator( Aggregator aggregator, TupleEntry group, TupleEntry[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeAggregator( aggregator, group, argumentsArray, resultFields, properties );
    }

  public static TupleListCollector invokeBuffer( Buffer buffer, Tuple[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeBuffer( buffer, argumentsArray, resultFields );
    }

  public static TupleListCollector invokeBuffer( Buffer buffer, Tuple[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeBuffer( buffer, argumentsArray, resultFields, properties );
    }

  public static TupleListCollector invokeBuffer( Buffer buffer, TupleEntry[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeBuffer( buffer, null, argumentsArray, resultFields );
    }

  public static TupleListCollector invokeBuffer( Buffer buffer, TupleEntry[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeBuffer( buffer, null, argumentsArray, resultFields, properties );
    }

  public static TupleListCollector invokeBuffer( Buffer buffer, TupleEntry group, TupleEntry[] argumentsArray, Fields resultFields )
    {
    return CascadingTesting.invokeBuffer( buffer, group, argumentsArray, resultFields, Collections.emptyMap() );
    }

  public static TupleListCollector invokeBuffer( Buffer buffer, TupleEntry group, TupleEntry[] argumentsArray, Fields resultFields, Map<Object, Object> properties )
    {
    return CascadingTesting.invokeBuffer( buffer, group, argumentsArray, resultFields, properties );
    }

  public static List<Tuple> getSourceAsList( Flow flow ) throws IOException
    {
    return CascadingTesting.getSourceAsList( flow );
    }

  public static List<Tuple> getSinkAsList( Flow flow ) throws IOException
    {
    return CascadingTesting.getSinkAsList( flow );
    }

  public static List<Tuple> asList( Flow flow, Tap tap ) throws IOException
    {
    return CascadingTesting.asList( flow, tap );
    }

  public static List<Tuple> asList( Flow flow, Tap tap, Fields selector ) throws IOException
    {
    return CascadingTesting.asList( flow, tap, selector );
    }

  public static Set<Tuple> asSet( Flow flow, Tap tap ) throws IOException
    {
    return CascadingTesting.asSet( flow, tap );
    }

  public static Set<Tuple> asSet( Flow flow, Tap tap, Fields selector ) throws IOException
    {
    return CascadingTesting.asSet( flow, tap, selector );
    }

  public static <C extends Collection<Tuple>> C asCollection( Flow flow, Tap tap, C collection ) throws IOException
    {
    return CascadingTesting.asCollection( flow, tap, Fields.ALL, collection );
    }

  public static <C extends Collection<Tuple>> C asCollection( Flow flow, Tap tap, Fields selector, C collection ) throws IOException
    {
    return CascadingTesting.asCollection( flow, tap, selector, collection );
    }

  public static <C extends Collection<Tuple>> C asCollection( TupleEntryIterator iterator, C result )
    {
    return CascadingTesting.asCollection( iterator, Fields.ALL, result );
    }

  public static <C extends Collection<Tuple>> C asCollection( TupleEntryIterator iterator, Fields selector, C result )
    {
    return CascadingTesting.asCollection( iterator, selector, result );
    }
  }
