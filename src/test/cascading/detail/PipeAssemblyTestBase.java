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

package cascading.detail;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.TapIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Util;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.log4j.Logger;

/** @version : IntelliJGuide,v 1.13 2001/03/22 22:35:22 SYSTEM Exp $ */
public abstract class PipeAssemblyTestBase extends CascadingTestCase
  {
  private static final Logger LOG = Logger.getLogger( PipeAssemblyTestBase.class );

  static String inputFile = "build/test/data/nums.20.txt";

  static String outputPath = "build/test/output/assembly/";

  static Fields[] OP_ARGS_FIELDS = new Fields[]{new Fields( -1 ), new Fields( 0 ), Fields.ALL};
  static Fields[] OP_DECL_FIELDS = new Fields[]{new Fields( "field" ), Fields.UNKNOWN, Fields.VALUES, Fields.ARGS};
  static Fields[] OP_SELECT_FIELDS = new Fields[]{new Fields( -1 ), new Fields( "field" ), Fields.RESULTS, Fields.ALL};
  static final String OP_VALUE = "value";

  static Fields[] LHS_ARGS_FIELDS = new Fields[]{new Fields( -1 ), Fields.ALL};
  static Fields[] LHS_DECL_FIELDS = new Fields[]{new Fields( "field" ), Fields.UNKNOWN, Fields.ARGS};
  static Fields[] LHS_SELECT_FIELDS = new Fields[]{Fields.RESULTS, Fields.ALL};
  static final String LHS_VALUE = "value";

  static Fields[] RHS_ARGS_FIELDS = new Fields[]{new Fields( -1 ), new Fields( 0 ), Fields.ALL};
  static Fields[] RHS_DECL_FIELDS = new Fields[]{new Fields( "field2" ), Fields.UNKNOWN, Fields.VALUES, Fields.ARGS};
  static Fields[] RHS_SELECT_FIELDS = new Fields[]{new Fields( -1 ), new Fields( "field2" ), new Fields( "field" ), Fields.RESULTS, Fields.ALL};
  static final String RHS_VALUE = "value2";

  public static void makeSuites( Properties properties, Map<String, Pipe> pipes, TestSuite suite, Class type ) throws IllegalAccessException, InvocationTargetException, InstantiationException
    {
    for( String name : pipes.keySet() )
      {
      if( isUNDEFINED( properties, name ) )
        LOG.debug( "skipping: " + name );
      else
        suite.addTest( (Test) type.getConstructors()[ 0 ].newInstance( properties, name, pipes.get( name ) ) );
      }
    }

  public static Properties loadProperties( String type ) throws IOException
    {
    String path = PipeAssemblyTestBase.class.getPackage().getName().replace( ".", "/" ) + "/" + type;
    InputStream input = PipeAssemblyTestBase.class.getClassLoader().getResourceAsStream( path );

    Properties properties = new Properties();

    properties.load( input );

    return properties;
    }

  private static String runOnly( Properties properties )
    {
    return properties.getProperty( "run.only" );
    }

  private static boolean isUNDEFINED( Properties properties, String name )
    {
    return !properties.containsKey( name + ".ERROR" ) && !properties.containsKey( name + ".tuple" );
    }

  public static Map<String, Pipe> buildOpPipes( Properties properties, String prefix, Pipe pipe, AssemblyFactory assemblyFactory, Fields[] args_fields, Fields[] decl_fields, Fields[] select_fields, String functionValue )
    {
    Map<String, Pipe> pipes = new LinkedHashMap<String, Pipe>();

    String runOnly = runOnly( properties );

    for( int arg = 0; arg < args_fields.length; arg++ )
      {
      Fields argFields = args_fields[ arg ];

      for( int decl = 0; decl < decl_fields.length; decl++ )
        {
        Fields declFields = decl_fields[ decl ];

        for( int select = 0; select < select_fields.length; select++ )
          {
          Fields selectFields = select_fields[ select ];

          String name;
          if( prefix != null )
            name = prefix + "." + Util.join( Fields.fields( argFields, declFields, selectFields ), "_" );
          else
            name = Util.join( Fields.fields( argFields, declFields, selectFields ), "_" );

          if( runOnly != null && !runOnly.equalsIgnoreCase( name ) )
            continue;

          pipes.put( name, assemblyFactory.createAssembly( pipe, argFields, declFields, functionValue, selectFields ) );
          }
        }
      }

    return pipes;
    }

  Properties properties;
  private Pipe pipe;
  Fields argFields;
  Fields declFields;
  Fields selectFields;
  Tuple resultTuple;
  int resultLength;

  public PipeAssemblyTestBase( Properties properties, String name, Pipe pipe )
    {
    super( name );
    this.properties = properties;
    this.pipe = pipe;
    this.resultTuple = getResultTuple();
    this.resultLength = getResultLength();
    }

  Tuple getResultTuple()
    {
    return Tuple.parse( (String) properties.get( getName() + ".tuple" ) );
    }

  boolean isWriteDOT()
    {
    return properties.containsKey( getName() + ".writedot" );
    }

  int getResultLength()
    {
    return Integer.parseInt( properties.getProperty( getName() + ".length", properties.getProperty( "default.length" ) ) );
    }

  boolean isError()
    {
    return properties.containsKey( getName() + ".ERROR" );
    }

  public void runTest() throws Exception
    {
    Tap source = new Hfs( new TextLine(), inputFile );
    Tap sink = new Hfs( new TextLine(), outputPath + "/" + getName(), true );

    Flow flow = null;

    try
      {
      flow = new FlowConnector().connect( source, sink, pipe );

      if( isWriteDOT() )
        flow.writeDOT( getName() + ".dot" );

      flow.complete();

      if( isError() )
        fail( "did not throw asserted error" );
      }
    catch( Exception exception )
      {
      if( isError() )
        return;
      else
        throw exception;
      }

    if( resultLength != -1 )
      validateLength( flow, resultLength, null );

    TapIterator iterator = flow.openSink();
    Comparable result = iterator.next().get( 1 );

    if( resultTuple != null )
      assertEquals( "not equal: ", resultTuple.toString(), result );
    else if( resultTuple == null )
      fail( "no result assertion made for:" + getName() + " with result: " + result );
    }
  }
