/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.scheme.util;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import cascading.InputData;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.type.DateType;
import cascading.tuple.util.TupleViews;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 *
 */
@State(Scope.Thread)
public abstract class DelimitedParserBench
  {
  public DelimitedParser delimitedParser;
  public List<String> lines;
  public Appendable buffer;
  public Tuple tuple;
  public TupleEntry tupleEntry;

  protected abstract Fields getFields();

  @Setup
  public void setUp() throws Exception
    {
    tuple = TupleViews.createObjectArray();
    tupleEntry = new TupleEntry( getFields(), tuple );

    delimitedParser = new DelimitedParser( ",", "\"", null, true, true );
    delimitedParser.reset( getFields(), getFields() );

    FileReader reader = new FileReader( getInputFileName() );

    LineNumberReader lineReader = new LineNumberReader( reader );

    lines = lineReader.lines().collect( Collectors.toList() );

    buffer = new NullAppendable();
    }

  protected abstract String getInputFileName();

  @BenchmarkMode({Mode.Throughput})
  @OutputTimeUnit(TimeUnit.MINUTES)
  @Benchmark
  public Appendable measureReaderWriter()
    {
    for( String line : lines )
      {
      Object[] split = delimitedParser.parseLine( line );
      TupleViews.reset( tuple, split );
      Iterable<String> strings = tupleEntry.asIterableOf( String.class );
      delimitedParser.joinLine( strings, buffer );
      }

    return buffer;
    }

  /**
   * DelimitedParserBench.PrimitivesDelimitedParserBench.measureReaderWriter  thrpt    5  44.454 ± 0.336  ops/min
   * DelimitedParserBench.PrimitivesDelimitedParserBench.measureReaderWriter  thrpt    5  45.654 ± 0.487  ops/min
   */
  public static class PrimitivesDelimitedParserBench extends DelimitedParserBench
    {
    @Override
    public Fields getFields()
      {
      // 10001,"1953-09-02","Georgi","Facello","M","1986-06-26"
      return Fields.NONE
        .append( new Fields( "id", Integer.TYPE ) )
        .append( new Fields( "dob", String.class ) )
        .append( new Fields( "first", String.class ) )
        .append( new Fields( "last", String.class ) )
        .append( new Fields( "gender", String.class ) )
        .append( new Fields( "hired", String.class ) );
      }

    @Override
    protected String getInputFileName()
      {
      return InputData.employees;
      }
    }

  /**
   * DelimitedParserBench.DatesDelimitedParserBench.measureReaderWriter       thrpt    5  27.684 ± 1.025  ops/min
   * DelimitedParserBench.DatesDelimitedParserBench.measureReaderWriter       thrpt    5  27.601 ± 0.991  ops/min
   */
  public static class DatesDelimitedParserBench extends DelimitedParserBench
    {
    DateType date = new DateType( "yyyy-MM-dd" );

    @Override
    public Fields getFields()
      {
      // 10001,"1953-09-02","Georgi","Facello","M","1986-06-26"
      return Fields.NONE
        .append( new Fields( "id", Integer.TYPE ) )
        .append( new Fields( "dob", date ) )
        .append( new Fields( "first", String.class ) )
        .append( new Fields( "last", String.class ) )
        .append( new Fields( "gender", String.class ) )
        .append( new Fields( "hired", date ) );
      }

    @Override
    protected String getInputFileName()
      {
      return InputData.employees;
      }
    }
  }
