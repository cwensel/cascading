/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.regex;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

import java.util.Iterator;

/**
 *
 */
public class RegexesTest extends CascadingTestCase
  {
  public RegexesTest()
    {
    super( "regex test" );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    }

  public void testSplitter()
    {
    RegexSplitter splitter = new RegexSplitter( "\t" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testSplitterGenerator()
    {
    RegexSplitGenerator splitter = new RegexSplitGenerator( new Fields( "word" ), "\\s+" );
    Tuple arguments = new Tuple( "foo\t  bar" );
    Fields resultFields = new Fields( "field" );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 2, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    assertEquals( "not equal: iterator.next().get(0)", "foo", iterator.next().get( 0 ) );
    assertEquals( "not equal: iterator.next().get(0)", "bar", iterator.next().get( 0 ) );
    }


  public void testReplace()
    {
    RegexReplace splitter = new RegexReplace( new Fields( "words" ), "\\s+", "-", true );
    Tuple arguments = new Tuple( "foo\t bar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo-bar", tuple.get( 0 ) );
    }

  public void testParserDeclared()
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs", "rhs" ), "(\\S+)\\s+(\\S+)", new int[]{1, 2} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 2 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserDeclared2()
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs", "rhs" ), "(\\S+)\\s+(\\S+)" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 2 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserDeclared3()
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "(\\S+)\\s+\\S+" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    }

  public void testParserDeclared4()
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "\\S+\\s+\\S+" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo\tbar", tuple.get( 0 ) );
    }

  /** Contributed by gicode */
  public void testParserDeclared5()
    {
    RegexParser splitter = new RegexParser( new Fields( "bar" ), "^GET /foo\\?bar=([^\\&]+)&" );
    Tuple arguments = new Tuple( "GET /foo?bar=z123&baz=2" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tuple size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "z123", tuple.get( 0 ) );
    }

  public void testParserDeclared6()
    {
    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "(\\S+)\\s+\\S+", new int[]{1} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.size( 1 );

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    }


  public void testParserUnknown()
    {
    RegexParser splitter = new RegexParser( Fields.UNKNOWN, "(\\S+)\\s+(\\S+)", new int[]{1, 2} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserUnknown2()
    {
    RegexParser splitter = new RegexParser( "(\\S+)\\s+(\\S+)", new int[]{1, 2} );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserUnknown3()
    {
    RegexParser splitter = new RegexParser( "(\\S+)\\s+(\\S+)" );
    Tuple arguments = new Tuple( "foo\tbar" );
    Fields resultFields = Fields.UNKNOWN;

    TupleListCollector collector = invokeFunction( splitter, arguments, resultFields );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testFilter()
    {
    Tuple arguments = new Tuple( "foo", "bar" );
    RegexFilter filter = new RegexFilter( "foo\tbar" );

    boolean isRemove = invokeFilter( filter, arguments );

    assertTrue( "was not remove", !isRemove );
    }
  }