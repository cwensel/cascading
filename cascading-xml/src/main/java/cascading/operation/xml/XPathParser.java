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
import cascading.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class XPathParser extends XPathOperation implements Function<Pair<DocumentBuilder, Tuple>>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( XPathParser.class );

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

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<DocumentBuilder, Tuple>> functionCall )
    {
    Tuple tuple = functionCall.getContext().getRhs();

    tuple.clear();

    String argument = functionCall.getArguments().getString( 0 );
    Document document = parseDocument( functionCall.getContext().getLhs(), argument );

    for( int i = 0; i < getExpressions().size(); i++ )
      {
      try
        {
        NodeList value = (NodeList) getExpressions().get( i ).evaluate( document, XPathConstants.NODESET );

        if( LOG.isDebugEnabled() )
          LOG.debug( "xpath: {} was: {}", paths[ i ], value != null && value.getLength() != 0 );

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
