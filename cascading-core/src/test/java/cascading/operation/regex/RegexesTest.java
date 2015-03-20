/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.regex;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;
import org.junit.Test;

/**
 *
 */
public class RegexesTest extends CascadingTestCase
  {
  public RegexesTest()
    {
    }

  @Test
  public void testSplitter() throws IOException
    {
    RegexSplitter splitter = new RegexSplitter( "\t" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.getObject( 1 ) );
    }

  @Test
  public void testSplitterGenerator() throws IOException
    {
    RegexSplitGenerator splitter = new RegexSplitGenerator( new Fields( "word" ), "\\s+" );
    Tuple arguments = new Tuple( "foo\t  bar" );
    Fields resultFields = new Fields( "field" );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 2, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    assertEquals( "not equal: iterator.next().get(0)", "foo", iterator.next().getObject( 0 ) );
    assertEquals( "not equal: iterator.next().get(0)", "bar", iterator.next().getObject( 0 ) );
    }

  @Test
  public void testReplace() throws IOException
    {
    RegexReplace splitter = new RegexReplace( new Fields( "words" ), "\\s+", "-", true );
    Tuple arguments = new Tuple( "foo\t bar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo-bar", tuple.getObject( 0 ) );
    }

  @Test
  public void testParserDeclared() throws IOException
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs", "rhs" ), "(\\S+)\\s+(\\S+)", new int[]{1, 2} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 2 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.getObject( 1 ) );
    }

  @Test
  public void testParserDeclared2() throws IOException
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs", "rhs" ), "(\\S+)\\s+(\\S+)" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 2 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.getObject( 1 ) );
    }

  @Test
  public void testParserDeclared3() throws IOException
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "(\\S+)\\s+\\S+" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    }

  @Test
  public void testParserDeclared4() throws IOException
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "\\S+\\s+\\S+" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo\tbar", tuple.getObject( 0 ) );
    }

  /** Contributed by gicode */
  @Test
  public void testParserDeclared5() throws IOException
    {
    RegexParser splitter = new RegexParser( new Fields( "bar" ), "^GET /foo\\?bar=([^\\&]+)&" );
    Tuple arguments = new Tuple( "GET /foo?bar=z123&baz=2" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tuple size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "z123", tuple.getObject( 0 ) );
    }

  @Test
  public void testParserDeclared6() throws IOException
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "(\\S+)\\s+\\S+", new int[]{1} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    }

  @Test
  public void testParserUnknown() throws IOException
    {
    RegexParser splitter = new RegexParser( Fields.UNKNOWN, "(\\S+)\\s+(\\S+)", new int[]{1, 2} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.getObject( 1 ) );
    }

  @Test
  public void testParserUnknown2() throws IOException
    {
    RegexParser splitter = new RegexParser( "(\\S+)\\s+(\\S+)", new int[]{1, 2} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.getObject( 1 ) );
    }

  @Test
  public void testParserUnknown3() throws IOException
    {
    RegexParser splitter = new RegexParser( "(\\S+)\\s+(\\S+)" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.getObject( 1 ) );
    }

  @Test
  public void testFilter()
    {
    Tuple arguments = new Tuple( "foo", "bar" );
    RegexFilter filter = new RegexFilter( "foo\tbar" );

    boolean isRemove = invokeFilter( filter, arguments );

    assertTrue( "was not remove", !isRemove );
    }
  }