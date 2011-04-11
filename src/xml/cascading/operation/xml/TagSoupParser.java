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

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.log4j.Logger;
import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.XMLWriter;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

/**
 * Class TagSoupParser uses the <a href="http://home.ccil.org/~cowan/XML/tagsoup/">Tag Soup</a> library to convert
 * incoming HTML to clean XHTML.
 */
public class TagSoupParser extends BaseOperation implements Function
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TagSoupParser.class );

  /** Field features */
  private Map<String, Boolean> features;
  /** Field schema */
  private transient HTMLSchema schema;
  /** Field parser */
  private transient Parser parser;

  /**
   * Constructor TagSoupParser creates a new TagSoupParser instance.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public TagSoupParser( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field name: " + fieldDeclaration.print() );
    }

  private HTMLSchema getSchema()
    {
    if( schema == null )
      schema = new HTMLSchema();

    return schema;
    }

  private Parser getParser() throws SAXNotSupportedException, SAXNotRecognizedException
    {
    if( parser != null )
      return parser;

    parser = new Parser();
    parser.setProperty( Parser.schemaProperty, getSchema() );

    if( features != null )
      {
      for( Map.Entry<String, Boolean> entry : features.entrySet() )
        parser.setFeature( entry.getKey(), entry.getValue() );
      }

    return parser;
    }

  /**
   * Method setFeature allows the user to set 'features' directly on the TagSoup parser, {@link Parser#setFeature}.
   * <p/>
   * Note, all features are lazily added when the Parser is instantiated.
   *
   * @param feature of type String
   * @param value   of type boolean
   */
  public void setFeature( String feature, boolean value )
    {
    if( features == null )
      features = new HashMap<String, Boolean>();

    features.put( feature, value );
    }

  /** @see cascading.operation.Function#operate(cascading.flow.FlowProcess, cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    try
      {
      StringWriter writer = new StringWriter();
      XMLWriter xmlWriter = new XMLWriter( writer );

      xmlWriter.setPrefix( getSchema().getURI(), "" );
      xmlWriter.setOutputProperty( XMLWriter.OMIT_XML_DECLARATION, "yes" );

      InputSource source = new InputSource( new StringReader( (String) functionCall.getArguments().getObject( 0 ) ) );

      getParser().setContentHandler( xmlWriter );

      getParser().parse( source );

      functionCall.getOutputCollector().add( new Tuple( writer.getBuffer().toString() ) );
      }
    catch( SAXNotRecognizedException exception )
      {
      LOG.warn( "ignoring TagSoup exception", exception );
      }
    catch( SAXNotSupportedException exception )
      {
      LOG.warn( "ignoring TagSoup exception", exception );
      }
    catch( IOException exception )
      {
      LOG.warn( "ignoring TagSoup exception", exception );
      }
    catch( SAXException exception )
      {
      LOG.warn( "ignoring TagSoup exception", exception );
      }
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof TagSoupParser ) )
      return false;
    if( !super.equals( object ) )
      return false;

    TagSoupParser that = (TagSoupParser) object;

    if( features != null ? !features.equals( that.features ) : that.features != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( features != null ? features.hashCode() : 0 );
    return result;
    }
  }
