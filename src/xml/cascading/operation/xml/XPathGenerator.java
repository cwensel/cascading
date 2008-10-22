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

package cascading.operation.xml;

import java.io.StringReader;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.log4j.Logger;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * XPathGenerator is a Generator function that will emit a new Tuple for every Node returned by
 * the given XPath expression.
 */
public class XPathGenerator extends XPathOperation implements Function
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( XPathGenerator.class );

  /**
   * Constructor XPathGenerator creates a new XPathGenerator instance.
   *
   * @param fieldDeclaration of type Fields
   * @param namespaces       of type String[][]
   * @param paths            of type String...
   */
  public XPathGenerator( Fields fieldDeclaration, String[][] namespaces, String... paths )
    {
    super( 1, fieldDeclaration, namespaces, paths );

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "only one field can be declared: " + fieldDeclaration.print() );

    }

  /** @see Function#operate(cascading.flow.FlowProcess,cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry input = functionCall.getArguments();

    if( input.get( 0 ) == null || !( input.get( 0 ) instanceof String ) )
      return;

    String value = (String) input.get( 0 );

    if( value.length() == 0 ) // intentionally not trim()ing this value
      return;

    InputSource source = new InputSource( new StringReader( value ) );

    for( int i = 0; i < getExpressions().size(); i++ )
      {
      try
        {
        NodeList nodeList = (NodeList) getExpressions().get( i ).evaluate( source, XPathConstants.NODESET );

        if( LOG.isDebugEnabled() )
          LOG.debug( "xpath: " + paths[ i ] + " was: " + ( nodeList != null && nodeList.getLength() != 0 ) );

        for( int j = 0; j < nodeList.getLength(); j++ )
          functionCall.getOutputCollector().add( new Tuple( writeAsXML( nodeList.item( j ) ) ) );

        }
      catch( XPathExpressionException exception )
        {
        throw new OperationException( "could not evaluate xpath expression: " + paths[ i ], exception );
        }
      }
    }
  }
