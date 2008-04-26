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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import cascading.operation.Function;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;
import org.apache.log4j.Logger;
import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.XMLWriter;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

/**
 * Class TagSoupParser used the <a href="http://home.ccil.org/~cowan/XML/tagsoup/">Tag Soup</a> library to convert
 * incoming HTML to clean XHTML.
 */
public class TagSoupParser extends Operation implements Function
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TagSoupParser.class );

  /** Field schema */
  private HTMLSchema schema;
  /** Field parser */
  private Parser parser;

  /**
   * Constructor TagSoupParser creates a new TagSoupParser instance.
   *
   * @param fieldDeclaration of type Fields
   */
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

    return parser;
    }

  /** @see cascading.operation.Function#operate(cascading.tuple.TupleEntry,cascading.tuple.TupleCollector) */
  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    try
      {
      StringWriter writer = new StringWriter();
      XMLWriter xmlWriter = new XMLWriter( writer );

      xmlWriter.setPrefix( getSchema().getURI(), "" );
      xmlWriter.setOutputProperty( XMLWriter.OMIT_XML_DECLARATION, "yes" );

      InputSource source = new InputSource( new StringReader( (String) input.get( 0 ) ) );

      getParser().setContentHandler( xmlWriter );

      getParser().parse( source );

      outputCollector.add( new Tuple( writer.getBuffer().toString() ) );
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
  }
