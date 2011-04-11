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
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/** Class XPathOperation is the base class for all XPath operations. */
public class XPathOperation extends BaseOperation<DocumentBuilder>
  {
  /** Field NAMESPACE_XHTML */
  public static final String[][] NAMESPACE_XHTML = new String[][]{
    new String[]{"xhtml", "http://www.w3.org/1999/xhtml"}};

  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( XPathOperation.class );

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
    }

  protected XPathOperation( int numArgs, String[][] namespaces, String... paths )
    {
    super( numArgs );
    this.namespaces = namespaces;
    this.paths = paths;

    if( paths == null || paths.length == 0 )
      throw new IllegalArgumentException( "a xpath expression must be given" );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<DocumentBuilder> operationCall )
    {
    try
      {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

      factory.setNamespaceAware( true );

      operationCall.setContext( factory.newDocumentBuilder() );
      }
    catch( ParserConfigurationException exception )
      {
      throw new OperationException( "could not create document builder", exception );
      }
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
          LOG.debug( "adding namespace: {}:{}", namespace[ 0 ], namespace[ 1 ] );

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

  protected Document parseDocument( DocumentBuilder documentBuilder, String argument )
    {
    Document document;
    try
      {
      document = documentBuilder.parse( new InputSource( new StringReader( argument ) ) );
      }
    catch( SAXException exception )
      {
      throw new OperationException( "could not parse xml document", exception );
      }
    catch( IOException exception )
      {
      throw new OperationException( "could not parse xml document", exception );
      }
    return document;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof XPathOperation ) )
      return false;
    if( !super.equals( object ) )
      return false;

    XPathOperation that = (XPathOperation) object;

    if( expressions != null ? !expressions.equals( that.expressions ) : that.expressions != null )
      return false;
    if( !Arrays.equals( paths, that.paths ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( paths != null ? Arrays.hashCode( paths ) : 0 );
    result = 31 * result + ( expressions != null ? expressions.hashCode() : 0 );
    return result;
    }
  }
