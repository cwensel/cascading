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

package cascading.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.List;

import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import org.apache.commons.codec.binary.Base64;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.MatrixExporter;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;

/** Class Util provides reusable operations. */
public class Util
  {
  public static String serializeBase64( Object object )
    {
    try
      {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream( bytes );

      out.writeObject( object );

      return new String( Base64.encodeBase64( bytes.toByteArray() ) );
      }
    catch( IOException exception )
      {
      exception.printStackTrace();
      }

    return null;
    }

  public static Object deserializeBase64( String string )
    {
    if( string == null || string.length() == 0 )
      return null;

    try
      {
      ByteArrayInputStream bytes = new ByteArrayInputStream( Base64.decodeBase64( string.getBytes() ) );
      ObjectInputStream in = new ObjectInputStream( bytes );

      return in.readObject();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to deserialize data", exception );
      }
    catch( ClassNotFoundException exception )
      {
      throw new FlowException( "unable to deserialize data", exception );
      }
    }

  public static String join( int[] list, String delim )
    {
    StringBuffer buffer = new StringBuffer();

    for( Object s : list )
      {
      if( buffer.length() != 0 )
        buffer.append( delim );

      buffer.append( s );
      }

    return buffer.toString();
    }

  public static String join( Object[] list, String delim )
    {
    StringBuffer buffer = new StringBuffer();

    for( Object s : list )
      {
      if( buffer.length() != 0 )
        buffer.append( delim );

      buffer.append( s );
      }

    return buffer.toString();
    }

  public static String join( Collection collection )
    {
    return join( collection, "\t" );
    }

  public static String join( Collection collection, String delim )
    {
    StringBuffer buffer = new StringBuffer();

    join( buffer, collection, delim );

    return buffer.toString();
    }

  public static void join( StringBuffer buffer, Collection collection, String delim )
    {
    for( Object s : collection )
      {
      if( buffer.length() != 0 )
        buffer.append( delim );

      buffer.append( s );
      }
    }

  public static String printGraph( SimpleDirectedGraph graph )
    {
    StringWriter writer = new StringWriter();

    printGraph( writer, graph );

    return writer.toString();
    }

  public static void printGraph( PrintStream out, SimpleDirectedGraph graph )
    {
    PrintWriter printWriter = new PrintWriter( out );

    printGraph( printWriter, graph );
    }

  public static void printGraph( String filename, SimpleDirectedGraph graph )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      printGraph( writer, graph );

      writer.close();
      }
    catch( IOException exception )
      {
      exception.printStackTrace();
      }
    }

  @SuppressWarnings({"unchecked"})
  private static void printGraph( Writer writer, SimpleDirectedGraph graph )
    {
    DOTExporter dot = new DOTExporter( new IntegerNameProvider(), new VertexNameProvider()
    {
    public String getVertexName( Object object )
      {
      return object.toString().replaceAll( "\"", "\'" );
      }
    }, new EdgeNameProvider<Object>()
    {
    public String getEdgeName( Object object )
      {
      return object.toString().replaceAll( "\"", "\'" );
      }
    } );

    dot.export( writer, graph );
    }

  public static String toNull( Object object )
    {
    if( object == null )
      return "";

    return object.toString();
    }

  public static void printMatrix( PrintStream out, SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    new MatrixExporter().exportAdjacencyMatrix( new PrintWriter( out ), graph );
    }

  /**
   * Remove all nulls from the given List.
   *
   * @param list
   */
  @SuppressWarnings({"StatementWithEmptyBody"})
  public static void removeAllNulls( List list )
    {
    while( list.remove( null ) )
      ;
    }
  }
