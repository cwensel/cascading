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

package cascading.operation.xml;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
@PlatformTest(platforms = {"none"})
public class XPathTest extends CascadingTestCase
  {
  public XPathTest()
    {
    }

  public void testParserMultipleXPaths() throws IOException
    {
    String[][] namespaces = {new String[]{"a", "http://foo.com/a"},
                             new String[]{"b", "http://bar.com/b"}};

    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
      "<document xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:a=\"http://foo.com/a\" xmlns:b=\"http://bar.com/b\" xmlns=\"http://baz.com/c\">" +
      "    <a:top>" +
      "      <a:first>first-value</a:first>" +
      "      <a:second>" +
      "        <b:top-first>nested-top-first</b:top-first>" +
      "        <b:top-second>nested-top-second</b:top-second>" +
      "      </a:second>" +
      "    </a:top>" +
      "</document>";

    Function function = new XPathParser( new Fields( "first", "second" ), namespaces, "//a:top/a:first/text()", "//a:second/b:top-second/text()" );

    Tuple tuple = invokeFunction( function, new Tuple( xml ), new Fields( "first", "second" ) ).iterator().next();

    assertEquals( "first-value", tuple.getString( 0 ) );
    assertEquals( "nested-top-second", tuple.getString( 1 ) );
    }

  public void testGeneratorMultipleXPaths() throws IOException
    {
    String[][] namespaces = {new String[]{"a", "http://foo.com/a"},
                             new String[]{"b", "http://bar.com/b"}};

    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
      "<document xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:a=\"http://foo.com/a\" xmlns:b=\"http://bar.com/b\" xmlns=\"http://baz.com/c\">" +
      "    <a:top>" +
      "      <a:first>first-value</a:first>" +
      "      <a:first>first-value</a:first>" +
      "      <a:second>" +
      "        <b:top-first>nested-top-first</b:top-first>" +
      "        <b:top-second>nested-top-second</b:top-second>" +
      "      </a:second>" +
      "    </a:top>" +
      "</document>";

    Function function = new XPathGenerator( new Fields( "first" ), namespaces, "//a:top/a:first/text()", "//a:second/b:top-second/text()" );

    Iterator<Tuple> tupleIterator = invokeFunction( function, new Tuple( xml ), new Fields( "first" ) ).iterator();

    assertEquals( "first-value", tupleIterator.next().getString( 0 ) );
    assertEquals( "first-value", tupleIterator.next().getString( 0 ) );
    assertEquals( "nested-top-second", tupleIterator.next().getString( 0 ) );
    }

  public void testFilterXPaths()
    {
    String[][] namespaces = {new String[]{"a", "http://foo.com/a"},
                             new String[]{"b", "http://bar.com/b"}};

    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
      "<document xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:a=\"http://foo.com/a\" xmlns:b=\"http://bar.com/b\" xmlns=\"http://baz.com/c\">" +
      "    <a:top>" +
      "      <a:first>first-value</a:first>" +
      "      <a:second>" +
      "        <b:top-first>nested-top-first</b:top-first>" +
      "        <b:top-second>nested-top-second</b:top-second>" +
      "      </a:second>" +
      "    </a:top>" +
      "</document>";

    Filter filter = new XPathFilter( namespaces, "//a:top/a:first/text() != 'first-value'" );

    assertTrue( invokeFilter( filter, new Tuple( xml ) ) );
    }
  }
