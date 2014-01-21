/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.xml;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

/**
 *
 */
public class XPathTest extends CascadingTestCase
  {
  public XPathTest()
    {
    }

  @Test
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

  @Test
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

  @Test
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
