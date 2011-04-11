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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * XPathParser will extract a value from the passed Tuple argument into a new Tuple field. One field
 * for every given XPath expression will be created. This function effectively converts an XML document into
 * a table.
 * <p/>
 * If the returned value of the expression is a NodeList, only the first Node is used. The Node is converted to a new
 * XML document and converted to a String. If only the text values are required, search on the text() nodes, or consider
 * using {@link XPathGenerator} to handle multiple NodeList values.
 */
public class XPathParser extends XPathOperation implements Function<DocumentBuilder>
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

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != paths.length )
      throw new IllegalArgumentException( "declared fields and given xpath expressions are not the same size: " + fieldDeclaration.print() + " paths: " + paths.length );
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

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != paths.length )
      throw new IllegalArgumentException( "declared fields and given xpath expressions are not the same size: " + fieldDeclaration.print() + " paths: " + paths.length );
    }

  /** @see Function#operate(cascading.flow.FlowProcess, cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall<DocumentBuilder> functionCall )
    {
    Tuple tuple = new Tuple();
    String argument = functionCall.getArguments().getString( 0 );
    Document document = parseDocument( functionCall.getContext(), argument );

    for( int i = 0; i < getExpressions().size(); i++ )
      {
      try
        {
        NodeList value = (NodeList) getExpressions().get( i ).evaluate( document, XPathConstants.NODESET );

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

    functionCall.getOutputCollector().add( tuple );
    }
  }
