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

package cascading.tuple;

import cascading.CascadingTestCase;
import cascading.tuple.util.TupleViews;
import org.junit.Test;

import static cascading.tuple.util.TupleViews.createComposite;
import static cascading.tuple.util.TupleViews.createNarrow;

/**
 *
 */
public class TupleViewTest extends CascadingTestCase
  {
  public TupleViewTest()
    {
    }

  @Test
  public void testArgumentSelector()
    {
    Fields declarator = new Fields( "1", "2", "3", "4" );
    Tuple incoming = new Tuple( 1, 2, 3, 4 );
    Fields selector = new Fields( "3", "2" );

    assertTuple( incoming.get( declarator, selector ) );

    int[] pos = incoming.getPos( declarator, selector );

    assertTuple( incoming.get( pos ) );

    assertTuple( TupleViews.createNarrow( pos, incoming ) );
    }

  private void assertTuple( Tuple result )
    {
    assertEquals( new Tuple( 3, 2 ), result );
    assertEquals( new Tuple( 3, 2 ), new Tuple( result ) );
    assertEquals( 3, result.getObject( 0 ) );
    assertEquals( 2, result.getObject( 1 ) );
    }

  // where outgoing fields are ALL
  @Test
  public void testSelectorAll()
    {
//    if( getOutputSelector().isAll() )
//      return inputEntry.getTuple().append( output );

    Fields incomingFields = new Fields( "1", "2", "3", "4" );
    Tuple incoming = new Tuple( 1, 2, 3, 4 );
    Fields resultFields = new Fields( "5", "6", "7" );
    Tuple result = new Tuple( 5, 6, 7 );

    Tuple view = TupleViews.createComposite( incoming, result );

    assertEquals( new Tuple( 1, 2, 3, 4, 5, 6, 7 ), view );
    assertEquals( new Tuple( 1, 2, 3, 4, 5, 6, 7 ), new Tuple( view ) );

    Fields allFields = Fields.join( incomingFields, resultFields );
    Fields selector = new Fields( "3", "2" );

    assertTuple( view.get( allFields, selector ) );
    }

  // where outgoing fields are REPLACE
  @Test
  public void testSelectorReplace()
    {
//    if( getOutputSelector().isReplace() )
//      {
//      Tuple result = new Tuple( inputEntry.getTuple() );
//
//      handle case where operation is declaring the argument fields, and they are positional
//      if( getFieldDeclaration().isArguments() )
//        result.set( inputEntry.getFields(), argumentSelector, output );
//      else
//        result.set( inputEntry.getFields(), declaredEntry.getFields(), output );
//
//      return result;
//      }

    Fields incomingFields = new Fields( "1", "2", "3", "4" );
    Tuple incoming = new Tuple( 1, 2, 3, 4 );
    Fields resultFields = new Fields( "3", "2" );
    Tuple result = new Tuple( 5, 6 );

    int[] setPos = incomingFields.getPos( resultFields );
    Tuple view = TupleViews.createOverride( incomingFields.getPos(), incoming, setPos, result );

    assertEquals( new Tuple( 1, 6, 5, 4 ), view );
    assertEquals( new Tuple( 1, 6, 5, 4 ), new Tuple( view ) );
    }

  // where outgoing fields are a combination of incoming and results
  @Test
  public void testSelectorMixed()
    {
//    declaredEntry.setTuple( output );
//
//    return TupleEntry.select( outgoingSelector, inputEntry, declaredEntry );

    Fields incomingFields = new Fields( "1", "2", "3", "4" );
    Tuple incoming = new Tuple( 1, 2, 3, 4 );
    Fields resultFields = new Fields( "5", "6", "7" );
    Tuple result = new Tuple( 5, 6, 7 );

    Tuple bottomView = TupleViews.createComposite( incoming, result );

    assertEquals( new Tuple( 1, 2, 3, 4, 5, 6, 7 ), bottomView );
    assertEquals( new Tuple( 1, 2, 3, 4, 5, 6, 7 ), new Tuple( bottomView ) );

    Fields allFields = Fields.join( incomingFields, resultFields );
    Fields selector = new Fields( "3", "2" );

    Tuple view = TupleViews.createNarrow( allFields.getPos( selector ), bottomView );

    assertTuple( view );
    }

  // where outgoing fields are SWAP
  @Test
  public void testSelectorSwap()
    {
//    if( getOutputSelector().isSwap() )
//      {
//      if( remainderFields.size() == 0 ) // the same as Fields.RESULTS
//        return output;
//      else
//        return inputEntry.selectTuple( remainderFields ).append( output );
//      }

    Fields incomingFields = new Fields( "1", "2", "3", "4" );
    Tuple incoming = new Tuple( 1, 2, 3, 4 );

    Fields remainderFields = new Fields( "1", "4" );

    Tuple bottomView = TupleViews.createNarrow( incomingFields.getPos( remainderFields ), incoming );

    assertEquals( new Tuple( 1, 4 ), bottomView );
    assertEquals( new Tuple( 1, 4 ), new Tuple( bottomView ) );

    Fields resultFields = new Fields( "5", "6", "7" );
    Tuple result = new Tuple( 5, 6, 7 );

    Tuple view = TupleViews.createComposite( bottomView, result );

    assertEquals( new Tuple( 1, 4, 5, 6, 7 ), view );
    assertEquals( new Tuple( 1, 4, 5, 6, 7 ), new Tuple( view ) );

    Tuple fieldView = TupleViews.createComposite( remainderFields, resultFields );

    TupleViews.reset( fieldView, view, result );

    assertEquals( new Tuple( 1, 4, 5, 6, 7 ), fieldView );
    assertEquals( new Tuple( 1, 4, 5, 6, 7 ), new Tuple( fieldView ) );
    }

  // tests actual swap algebra
  @Test
  public void testSelectorSwap2()
    {
    Fields incomingFields = new Fields( "0", "1", 2, 3 );
    Tuple incoming = new Tuple( 0, 1, 2, 3 );

    Fields resultFields = new Fields( "0", "1" );
    Tuple result = new Tuple( 0, 1 );

    Fields remainderFields = incomingFields.subtract( resultFields );

    Tuple remainderView = createNarrow( incomingFields.getPos( remainderFields ) );
    Tuple outgoingTuple = createComposite( Fields.asDeclaration( remainderFields ), resultFields );

    TupleViews.reset( remainderView, incoming );
    TupleViews.reset( outgoingTuple, remainderView, result );

    assertEquals( new Tuple( 2, 3, 0, 1 ), outgoingTuple );
    assertEquals( new Tuple( 2, 3, 0, 1 ), new Tuple( outgoingTuple ) );
    }
  }
