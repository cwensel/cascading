/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe;

import cascading.CascadingException;
import cascading.tuple.Fields;
import cascading.util.TraceUtil;

/** Class OperatorException is thrown during field name resolution during planning */
public class OperatorException extends CascadingException
  {
  enum Kind
    {
      argument, grouping, sorting, joining
    }

  private Fields incomingFields;
  private Fields argumentSelector;
  private Fields groupingSelector;
  private Fields sortingSelector;
  private Fields declaredFields;
  private Fields outputSelector;

  /** @see cascading.CascadingException#CascadingException() */
  public OperatorException()
    {
    }

  /**
   * Constructor OperatorException creates a new OperatorException instance.
   *
   * @param pipe   of type Pipe
   * @param string of type String
   */
  public OperatorException( Pipe pipe, String string )
    {
    super( TraceUtil.formatTrace( pipe, string ) );
    }

  /**
   * Constructor OperatorException creates a new OperatorException instance.
   *
   * @param pipe      of type Pipe
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public OperatorException( Pipe pipe, String string, Throwable throwable )
    {
    super( TraceUtil.formatTrace( pipe, string ), throwable );
    }

  /** @see cascading.CascadingException#CascadingException(String) */
  protected OperatorException( String string )
    {
    super( string );
    }

  /** @see cascading.CascadingException#CascadingException(String, Throwable) */
  protected OperatorException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /** @see cascading.CascadingException#CascadingException(Throwable) */
  protected OperatorException( Throwable throwable )
    {
    super( throwable );
    }

  /**
   * Constructor OperatorException creates a new OperatorException instance.
   *
   * @param pipe           of type Pipe
   * @param incomingFields of type Fields
   * @param declaredFields of type Fields
   * @param outputSelector of type Fields
   * @param throwable      of type Throwable
   */
  public OperatorException( Pipe pipe, Fields incomingFields, Fields declaredFields, Fields outputSelector, Throwable throwable )
    {
    super( createMessage( pipe, incomingFields, declaredFields, outputSelector ), throwable );

    this.incomingFields = incomingFields;
    this.declaredFields = declaredFields;
    this.outputSelector = outputSelector;
    }

  /**
   * Constructor OperatorException creates a new OperatorException instance.
   *
   * @param pipe           of type Pipe
   * @param kind           of type Kind
   * @param incomingFields of type Fields
   * @param selectorFields of type Fields
   * @param throwable      of type Throwable
   */
  public OperatorException( Pipe pipe, Kind kind, Fields incomingFields, Fields selectorFields, Throwable throwable )
    {
    super( createMessage( pipe, kind, incomingFields, selectorFields ), throwable );

    this.incomingFields = incomingFields;

    if( kind == Kind.argument )
      this.argumentSelector = selectorFields;
    else if( kind == Kind.grouping )
      this.groupingSelector = selectorFields;
    else
      this.sortingSelector = selectorFields;
    }

  /**
   * Method getIncomingFields returns the incomingFields of this OperatorException object.
   *
   * @return the incomingFields (type Fields) of this OperatorException object.
   */
  public Fields getIncomingFields()
    {
    return incomingFields;
    }

  /**
   * Method getArgumentSelector returns the argumentSelector of this OperatorException object.
   *
   * @return the argumentSelector (type Fields) of this OperatorException object.
   */
  public Fields getArgumentSelector()
    {
    return argumentSelector;
    }

  /**
   * Method getGroupingSelector returns the groupingSelector of this OperatorException object.
   *
   * @return the groupingSelector (type Fields) of this OperatorException object.
   */
  public Fields getGroupingSelector()
    {
    return groupingSelector;
    }

  /**
   * Method getSortingSelector returns the sortingSelector of this OperatorException object.
   *
   * @return the sortingSelector (type Fields) of this OperatorException object.
   */
  public Fields getSortingSelector()
    {
    return sortingSelector;
    }

  /**
   * Method getDeclaredFields returns the declaredFields of this OperatorException object.
   *
   * @return the declaredFields (type Fields) of this OperatorException object.
   */
  public Fields getDeclaredFields()
    {
    return declaredFields;
    }

  /**
   * Method getOutputSelector returns the outputSelector of this OperatorException object.
   *
   * @return the outputSelector (type Fields) of this OperatorException object.
   */
  public Fields getOutputSelector()
    {
    return outputSelector;
    }

  private static String createMessage( Pipe pipe, Fields incomingFields, Fields declaredFields, Fields outputSelector )
    {
    String message = "unable to resolve output selector: " + outputSelector.printVerbose() +
      ", with incoming: " + incomingFields.printVerbose() + " and declared: " + declaredFields.printVerbose();

    return TraceUtil.formatTrace( pipe, message );
    }

  private static String createMessage( Pipe pipe, Kind kind, Fields incomingFields, Fields argumentSelector )
    {
    String message = "unable to resolve " + kind + " selector: " + argumentSelector.printVerbose() +
      ", with incoming: " + incomingFields.printVerbose();

    return TraceUtil.formatTrace( pipe, message );
    }

  }
