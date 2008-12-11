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

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.namespace.NamespaceContext;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import cascading.operation.BaseOperation;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import org.apache.log4j.Logger;
import org.w3c.dom.Node;

/** Class XPathOperation is the base class for all XPath operations. */
public class XPathOperation extends BaseOperation
  {
  /** Field NAMESPACE_XHTML */
  public static final String[][] NAMESPACE_XHTML = new String[][]{new String[]{"xhtml", "http://www.w3.org/1999/xhtml"}};

  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( XPathOperation.class );

  /** Field namespaces */
  protected final String[][] namespaces;
  /** Field paths */
  protected final String[] paths;

  /** Field xPath */
  private transient XPath xPath;
  /** Field transformer */
  private transient Transformer transformer;
  /** Field expressions */
  private transient List<XPathExpression> expressions;

  protected XPathOperation( int numArgs, Fields fieldDeclaration, String[][] namespaces, String... paths )
    {
    super( numArgs, fieldDeclaration );
    this.namespaces = namespaces;
    this.paths = paths;

    if( paths == null || paths.length == 0 )
      throw new IllegalArgumentException( "a xpath expression must be given" );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != paths.length )
      throw new IllegalArgumentException( "declared fields and given xpath expressions are not the same size: " + fieldDeclaration.print() + " paths: " + paths.length );
    }

  protected XPathOperation( int numArgs, String[][] namespaces, String... paths )
    {
    super( numArgs );
    this.namespaces = namespaces;
    this.paths = paths;

    if( paths == null || paths.length == 0 )
      throw new IllegalArgumentException( "a xpath expression must be given" );
    }

  /**
   * Method getXPath returns the XPath of this XPathOperation object.
   *
   * @return the XPath (type XPath) of this XPathOperation object.
   */
  public XPath getXPath()
    {
    if( xPath != null )
      return xPath;

    XPathFactory factory = XPathFactory.newInstance();

    xPath = factory.newXPath();

    if( namespaces != null )
      {
      MutableNamespaceContext namespaceContext = new MutableNamespaceContext();

      for( String[] namespace : namespaces )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "adding namespace: " + namespace[ 0 ] + ":" + namespace[ 1 ] );

        namespaceContext.addNamespace( namespace[ 0 ], namespace[ 1 ] );
        }

      xPath.setNamespaceContext( namespaceContext );
      }

    return xPath;
    }

  /**
   * Method getTransformer returns the transformer of this XPathOperation object.
   *
   * @return the transformer (type Transformer) of this XPathOperation object.
   * @throws TransformerConfigurationException
   *          when
   */
  public Transformer getTransformer() throws TransformerConfigurationException
    {
    if( transformer != null )
      return transformer;

    transformer = TransformerFactory.newInstance().newTransformer();

    transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );

    return transformer;
    }

  protected String writeAsXML( Node node )
    {
    StringWriter stringWriter = new StringWriter();
    Result result = new StreamResult( stringWriter );
    Source source = new DOMSource( node );

    try
      {
      getTransformer().transform( source, result );
      }
    catch( TransformerException exception )
      {
      throw new OperationException( "writing to xml failed", exception );
      }

    return stringWriter.toString();
    }

  protected List<XPathExpression> getExpressions()
    {
    if( expressions != null )
      return expressions;

    expressions = new ArrayList<XPathExpression>();

    for( String path : paths )
      {
      try
        {
        expressions.add( getXPath().compile( path ) );
        }
      catch( XPathExpressionException exception )
        {
        throw new OperationException( "could not compile xpath expression", exception );
        }
      }

    return expressions;
    }

  class MutableNamespaceContext implements NamespaceContext
    {

    private final Map<String, String> map = new HashMap<String, String>();

    public MutableNamespaceContext()
      {
      }

    public void addNamespace( String prefix, String namespaceURI )
      {
      map.put( prefix, namespaceURI );
      }

    public String getNamespaceURI( String prefix )
      {
      return map.get( prefix );
      }

    public String getPrefix( String namespaceURI )
      {
      for( String prefix : map.keySet() )
        {
        if( map.get( prefix ).equals( namespaceURI ) )
          {
          return prefix;
          }
        }
      return null;
      }

    public Iterator getPrefixes( String namespaceURI )
      {
      List<String> prefixes = new ArrayList<String>();

      for( String prefix : map.keySet() )
        {
        if( map.get( prefix ).equals( namespaceURI ) )
          prefixes.add( prefix );
        }

      return prefixes.iterator();
      }
    }
  }
