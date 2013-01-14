/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import cascading.tuple.TupleEntry;
import cascading.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * XPathGenerator is a Generator function that will emit a new Tuple for every Node returned by
 * the given XPath expression.
 */
public class XPathGenerator extends XPathOperation implements Function<Pair<DocumentBuilder, Tuple>>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( XPathGenerator.class );

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

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<DocumentBuilder, Tuple>> functionCall )
    {
    TupleEntry input = functionCall.getArguments();

    if( input.getObject( 0 ) == null || !( input.getObject( 0 ) instanceof String ) )
      return;

    String value = input.getString( 0 );

    if( value.length() == 0 ) // intentionally not trim()ing this value
      return;

    Document document = parseDocument( functionCall.getContext().getLhs(), value );

    for( int i = 0; i < getExpressions().size(); i++ )
      {
      try
        {
        NodeList nodeList = (NodeList) getExpressions().get( i ).evaluate( document, XPathConstants.NODESET );

        if( LOG.isDebugEnabled() )
          LOG.debug( "xpath: {} was: {}", paths[ i ], nodeList != null && nodeList.getLength() != 0 );

        if( nodeList == null )
          continue;

        for( int j = 0; j < nodeList.getLength(); j++ )
          {
          functionCall.getContext().getRhs().set( 0, writeAsXML( nodeList.item( j ) ) );
          functionCall.getOutputCollector().add( functionCall.getContext().getRhs() );
          }

        }
      catch( XPathExpressionException exception )
        {
        throw new OperationException( "could not evaluate xpath expression: " + paths[ i ], exception );
        }
      }
    }
  }
