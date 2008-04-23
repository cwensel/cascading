/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.operation.regex;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

/**
 *
 */
public class RegexTest extends CascadingTestCase
  {
  public RegexTest()
    {
    super( "regex test" );
    }

  public void testSplitter()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexSplitter splitter = new RegexSplitter( "\t" );

    splitter.operate( new TupleEntry( new Tuple( "foo\tbar" ) ), collector );

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

    splitter.operate( new TupleEntry( new Tuple( "foo\t  bar" ) ), collector );

    assertEquals( "wrong size", 2, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    assertEquals( "not equal: iterator.next().get(0)", "foo", iterator.next().get( 0 ) );
    assertEquals( "not equal: iterator.next().get(0)", "bar", iterator.next().get( 0 ) );
    }


  public void testReplace()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexReplace splitter = new RegexReplace( new Fields( "words" ), "\\s+", "-", true );

    splitter.operate( new TupleEntry( new Tuple( "foo\t bar" ) ), collector );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo-bar", tuple.get( 0 ) );
    }

  public void testParser()
    {
    TupleListCollector collector = new TupleListCollector( Fields.UNKNOWN );

    RegexParser splitter = new RegexParser( Fields.UNKNOWN, "(\\S+)\\s+(\\S+)", new int[]{1, 2} );

    splitter.operate( new TupleEntry( new Tuple( "foo\tbar" ) ), collector );

    assertEquals( "wrong size", 1, collector.size() );

    Iterator<Tuple> iterator = collector.iterator();

    Tuple tuple = iterator.next();

    assertEquals( "not equal: tuple.get(0)", "foo", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "bar", tuple.get( 1 ) );
    }

  }