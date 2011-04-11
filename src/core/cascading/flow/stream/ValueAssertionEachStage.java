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

package cascading.flow.stream;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.ValueAssertion;
import cascading.pipe.Each;
import cascading.pipe.OperatorException;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class ValueAssertionEachStage extends EachStage
  {
  private ValueAssertion valueAssertion;

  public ValueAssertionEachStage( FlowProcess flowProcess, Each each )
    {
    super( flowProcess, each );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    valueAssertion = each.getValueAssertion();
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    argumentsEntry.setTuple( incomingEntry.selectTuple( argumentsSelector ) );

    try
      {
      valueAssertion.doAssert( flowProcess, operationCall );

      next.receive( this, incomingEntry );
      }
    catch( CascadingException exception )
      {
      handleException( exception, incomingEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( each, "operator Each failed executing operation", throwable ), incomingEntry );
      }
    }
  }
