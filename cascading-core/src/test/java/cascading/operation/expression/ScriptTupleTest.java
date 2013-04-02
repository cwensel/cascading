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

package cascading.operation.expression;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class ScriptTupleTest extends CascadingTestCase
  {
  public ScriptTupleTest()
    {
    }

  public void testSimpleScript()
    {
    String[] names = new String[]{"a", "b"};
    Class[] types = new Class[]{long.class, int.class};

    assertEquals( new Tuple( 3l ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", names, types, getEntry( 1, 2 ) ) );
    assertEquals( new Tuple( 3l ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", names, types, getEntry( 1.0, 2.0 ) ) );
    assertEquals( new Tuple( 3l ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", names, types, getEntry( "1", 2.0 ) ) );

    types = new Class[]{double.class, int.class};

    assertEquals( new Tuple( 3d ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", names, types, getEntry( 1, 2 ) ) );
    assertEquals( new Tuple( 3d ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", names, types, getEntry( 1.0, 2.0 ) ) );
    assertEquals( new Tuple( 3d ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", names, types, getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b"};
    types = new Class[]{String.class, int.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple((a != null) && (b > 0));", names, types, getEntry( "1", 2.0 ) ) );

    names = new String[]{"$0", "$1"};
    types = new Class[]{String.class, int.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple(($0 != null) && ($1 > 0));", names, types, getEntry( "1", 2.0 ) ) );

    names = new String[]{"a", "b", "c"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple(b.equals(\"1\") && (a == 2.0) && c.equals(\"2\"));", names, types, getEntry( 2.0, "1", "2" ) ) );

    names = new String[]{"a", "b", "$2"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple(b.equals(\"1\") && (a == 2.0) && $2.equals(\"2\"));", names, types, getEntry( 2.0, "1", "2" ) ) );

    String script = "";
    script += "boolean first = b.equals(\"1\");\n";
    script += "return cascading.tuple.Tuples.tuple(first && (a == 2.0) && $2.equals(\"2\"));\n";
    assertEquals( new Tuple( true ), evaluate( script, names, types, getEntry( 2.0, "1", "2" ) ) );
    }

  public void testSimpleScriptTyped()
    {
    Class returnType = Long.class;
    String[] names = new String[]{"a", "b"};
    Class[] types = new Class[]{long.class, int.class};

    assertEquals( new Tuple( 3l ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", returnType, getEntry( names, types, 1L, 2 ) ) );

    returnType = Double.class;
    types = new Class[]{double.class, int.class};

    assertEquals( new Tuple( 3d ), evaluate( "return cascading.tuple.Tuples.tuple(a + b);", returnType, getEntry( names, types, 1.0d, 2 ) ) );

    returnType = Boolean.TYPE;
    names = new String[]{"a", "b"};
    types = new Class[]{String.class, float.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple((a != null) && (b > 0));", returnType, getEntry( names, types, "1", 2.0 ) ) );

    names = new String[]{"$0", "$1"};
    types = new Class[]{String.class, float.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple(($0 != null) && ($1 > 0));", returnType, getEntry( names, types, "1", 2.0 ) ) );

    names = new String[]{"a", "b", "c"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple(b.equals(\"1\") && (a == 2.0) && c.equals(\"2\"));", returnType, getEntry( names, types, 2.0, "1", "2" ) ) );

    names = new String[]{"a", "b", "$2"};
    types = new Class[]{float.class, String.class, String.class};
    assertEquals( new Tuple( true ), evaluate( "return cascading.tuple.Tuples.tuple(b.equals(\"1\") && (a == 2.0) && $2.equals(\"2\"));", returnType, getEntry( names, types, 2.0, "1", "2" ) ) );

    String script = "";
    script += "boolean first = b.equals(\"1\");\n";
    script += "return cascading.tuple.Tuples.tuple(first && (a == 2.0) && $2.equals(\"2\"));\n";
    assertEquals( new Tuple( true ), evaluate( script, returnType, getEntry( names, types, 2.0, "1", "2" ) ) );
    }

  private Object evaluate( String expression, String[] names, Class[] types, TupleEntry tupleEntry )
    {
    ScriptTupleFunction function = new ScriptTupleFunction( new Fields( "result" ), expression, names, types );

    ConcreteCall<ExpressionOperation.Context> call = new ConcreteCall<ExpressionOperation.Context>( tupleEntry.getFields(), function.getFieldDeclaration() );
    function.prepare( FlowProcess.NULL, call );

    return function.evaluate( call.getContext(), tupleEntry );
    }

  private Object evaluate( String expression, Class returnType, TupleEntry tupleEntry )
    {
    ScriptTupleFunction function = new ScriptTupleFunction( new Fields( "result", returnType ), expression );

    ConcreteCall<ExpressionOperation.Context> call = new ConcreteCall<ExpressionOperation.Context>( tupleEntry.getFields(), function.getFieldDeclaration() );
    function.prepare( FlowProcess.NULL, call );

    return function.evaluate( call.getContext(), tupleEntry );
    }

  private TupleEntry getEntry( Comparable lhs, Comparable rhs )
    {
    Fields fields = new Fields( "a", "b" );
    Tuple parameters = new Tuple( lhs, rhs );

    return new TupleEntry( fields, parameters );
    }

  private TupleEntry getEntry( String[] names, Class[] types, Object... values )
    {
    Fields fields = new Fields( names ).applyTypes( types );
    Tuple parameters = new Tuple( values );

    return new TupleEntry( fields, parameters );
    }

  private TupleEntry getEntry( Comparable f, Comparable s, Comparable t )
    {
    Fields fields = new Fields( "a", "b", "c" );
    Tuple parameters = new Tuple( f, s, t );

    return new TupleEntry( fields, parameters );
    }
  }
