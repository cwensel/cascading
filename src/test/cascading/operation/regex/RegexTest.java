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

package cascading.operation.regex;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.operation.ConcreteCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

/**
 *
 */
public class RegexTest extends CascadingTestCase
  {
  private ConcreteCall operationCall;

  public RegexTest()
    {
    super( "regex test" );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    operationCall = new ConcreteCall();
    }

  public void testSplitter()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexSplitter splitter = new RegexSplitter( "\t" );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testSplitterGenerator()
    {
    TupleListCollector collector = new TupleListCollector( new Fields( "field" ) );

    RegexSplitGenerator splitter = new RegexSplitGenerator( new Fields( "word" ), "\\s+" );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\t  bar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 2, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    assertEquals( "not equal: iterator.next().get(0)", "foo", iterator.next().get( 0 ) );
    assertEquals( "not equal: iterator.next().get(0)", "bar", iterator.next().get( 0 ) );
    }


  public void testReplace()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexReplace splitter = new RegexReplace( new Fields( "words" ), "\\s+", "-", true );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\t bar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo-bar", tuple.get( 0 ) );
    }

  public void testParserDeclared()
    {
    TupleListCollector collector = new TupleListCollector( Fields.size( 2 ) );

    RegexParser splitter = new RegexParser( new Fields( "lhs", "rhs" ), "(\\S+)\\s+(\\S+)", new int[]{1, 2} );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserDeclared2()
    {
    TupleListCollector collector = new TupleListCollector( Fields.size( 2 ) );

    RegexParser splitter = new RegexParser( new Fields( "lhs", "rhs" ), "(\\S+)\\s+(\\S+)" );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserDeclared3()
    {
    TupleListCollector collector = new TupleListCollector( Fields.size( 1 ) );

    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "(\\S+)\\s+\\S+" );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    }

  public void testParserDeclared4()
    {
    TupleListCollector collector = new TupleListCollector( Fields.size( 1 ) );

    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "\\S+\\s+\\S+" );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo\tbar", tuple.get( 0 ) );
    }

  /** Contributed by gicode */
  public void testParserDeclared5()
    {
    TupleListCollector collector = new TupleListCollector( Fields.size( 1 ) );

    RegexParser splitter = new RegexParser( new Fields( "bar" ), "^GET /foo\\?bar=([^\\&]+)&" );

    operationCall.setArguments( new TupleEntry( new Tuple( "GET /foo?bar=z123&baz=2" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tuple size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "z123", tuple.get( 0 ) );
    }

  public void testParserDeclared6()
    {
    TupleListCollector collector = new TupleListCollector( Fields.size( 1 ) );

    RegexParser splitter = new RegexParser( new Fields( "lhs" ), "(\\S+)\\s+\\S+", new int[]{1} );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "wrong tupel size", 1, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    }


  public void testParserUnknown()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexParser splitter = new RegexParser( Fields.UNKNOWN, "(\\S+)\\s+(\\S+)", new int[]{1, 2} );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserUnknown2()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexParser splitter = new RegexParser( "(\\S+)\\s+(\\S+)", new int[]{1, 2} );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  public void testParserUnknown3()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexParser splitter = new RegexParser( "(\\S+)\\s+(\\S+)" );

    operationCall.setArguments( new TupleEntry( new Tuple( "foo\tbar" ) ) );
    operationCall.setOutputCollector( collector );
    splitter.prepare( null, operationCall );
    splitter.operate( null, operationCall );
    splitter.cleanup( null, operationCall );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  }