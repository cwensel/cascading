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
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationException;
import cascading.tuple.Tuple;
import cascading.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

/**
 * XPathFilter will filter out a Tuple if the given XPath expression returns false. Set removeMatch to true
 * if the filter should be reversed.
 */
public class XPathFilter extends XPathOperation implements Filter<Pair<DocumentBuilder, Tuple>>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( XPathFilter.class );

  /** Field removeMatch */
  private boolean removeMatch = false;

  /**
   * Constructor XPathFilter creates a new XPathFilter instance.
   *
   * @param namespaces of type String[][]
   * @param path       of type String
   */
  public XPathFilter( String[][] namespaces, String path )
    {
    super( 1, namespaces, path );
    }

  /**
   * Constructor XPathFilter creates a new XPathFilter instance.
   *
   * @param removeMatch of type boolean
   * @param namespaces  of type String[][]
   * @param path        of type String
   */
  public XPathFilter( boolean removeMatch, String[][] namespaces, String path )
    {
    super( 1, namespaces, path );
    this.removeMatch = removeMatch;
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Pair<DocumentBuilder, Tuple>> filterCall )
    {
    String argument = filterCall.getArguments().getString( 0 );
    Document document = parseDocument( filterCall.getContext().getLhs(), argument );
    XPathExpression expression = getExpressions().get( 0 );

    try
      {
      boolean value = (Boolean) expression.evaluate( document, XPathConstants.BOOLEAN );

      LOG.debug( "xpath: {} matches: {}", paths[ 0 ], value );

      return value == removeMatch;
      }
    catch( XPathExpressionException exception )
      {
      throw new OperationException( "could not evaluate xpath expression: " + paths[ 0 ], exception );
      }
    }
  }