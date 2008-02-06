/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import java.io.StringReader;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import cascading.operation.Function;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryListIterator;
import org.apache.log4j.Logger;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * XPathParser will extract a value from the passed Tuple argument into a new Tuple field. One field
 * for every given XPath expression will be created. This function effectively converts an XML document into
 * a table.
 * <p/>
 * If the returned value of the expression is a NodeList, only the first Node is used. The Node is converted to a new
 * XML document and converted to a String. If only the text values are required, search on the text() nodes, or consider
 * using {@link XPathGenerator} to handle multiple NodeList values.
 */
public class XPathParser extends XPathOperation implements Function
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( XPathParser.class );

  /**
   * Constructor XPathParser creates a new XPathParser instance.
   *
   * @param fieldDeclaration of type Fields
   * @param namespaces       of type String[][]
   * @param paths            of type String...
   */
  public XPathParser( Fields fieldDeclaration, String[][] namespaces, String... paths )
    {
    super( 1, fieldDeclaration, namespaces, paths );
    }

  /**
   * Constructor XPathParser creates a new XPathParser instance.
   *
   * @param fieldDeclaration of type Fields
   * @param paths            of type String...
   */
  public XPathParser( Fields fieldDeclaration, String... paths )
    {
    super( 1, fieldDeclaration, null, paths );
    }

  /** @see Function#operate(TupleEntry, TupleEntryListIterator) */
  public void operate( TupleEntry input, TupleEntryListIterator outputCollector )
    {
    Tuple tuple = new Tuple();
    InputSource source = new InputSource( new StringReader( (String) input.get( 0 ) ) );

    for( int i = 0; i < getExpressions().size(); i++ )
      {
      try
        {
        NodeList value = (NodeList) getExpressions().get( i ).evaluate( source, XPathConstants.NODESET );

        if( LOG.isDebugEnabled() )
          LOG.debug( "xpath: " + paths[ i ] + " was: " + ( value != null && value.getLength() != 0 ) );

        if( value != null && value.getLength() != 0 )
          tuple.add( writeAsXML( value.item( 0 ) ) );
        else
          tuple.add( "" );

        }
      catch( XPathExpressionException exception )
        {
        throw new OperationException( "could not evaluate xpath expression: " + paths[ i ], exception );
        }
      }

    outputCollector.add( tuple );
    }
  }
