/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation;

import java.beans.ConstructorProperties;
import java.io.PrintStream;

import cascading.flow.FlowProcess;

/**
 * Class Debug is a {@link Filter} that will never remove an item from a stream, but will print the Tuple to either
 * stdout or stderr.
 * <p/>
 * Currently, if printFields is true, they will print every 10 Tuples.
 * <p/>
 * The frequency that fields and tuples are printed can be set via {@link #setPrintFieldsEvery(int)} and
 * {@link #setPrintTupleEvery(int)} methods, respectively.
 */
public class Debug extends BaseOperation<Long> implements Filter<Long>, PlannedOperation<Long>
  {
  static public enum Output
    {
      STDOUT, STDERR
    }

  /** Field output */
  private Output output = Output.STDERR;
  /** Field prefix */
  private String prefix = null;
  /** Field printFields */
  private boolean printFields = false;

  /** Field printFieldsEvery */
  private int printFieldsEvery = 10;
  /** Field printTupleEvery */
  private int printTupleEvery = 1;

  /**
   * Constructor Debug creates a new Debug instance that prints to stderr by default, and does not print
   * the Tuple instance field names.
   */
  public Debug()
    {
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to stderr by default, and does not print
   * the Tuple instance field names.
   *
   * @param prefix of type String
   */
  @ConstructorProperties({"prefix"})
  public Debug( String prefix )
    {
    this.prefix = prefix;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to stderr and will print the current
   * Tuple instance field names if printFields is true.
   *
   * @param prefix      of type String
   * @param printFields of type boolean
   */
  @ConstructorProperties({"prefix", "printFields"})
  public Debug( String prefix, boolean printFields )
    {
    this.prefix = prefix;
    this.printFields = printFields;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to stderr and will print the current
   * Tuple instance field names if printFields is true.
   *
   * @param printFields of type boolean
   */
  @ConstructorProperties({"printFields"})
  public Debug( boolean printFields )
    {
    this.printFields = printFields;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to the declared stream and does not print the Tuple
   * field names.
   *
   * @param output of type Output
   */
  @ConstructorProperties({"output"})
  public Debug( Output output )
    {
    this.output = output;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to the declared stream and does not print the Tuple
   * field names.
   *
   * @param output of type Output
   * @param prefix of type String
   */
  @ConstructorProperties({"output", "prefix"})
  public Debug( Output output, String prefix )
    {
    this.output = output;
    this.prefix = prefix;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to the declared stream and will print the Tuple instances
   * field names if printFields is true.
   *
   * @param output      of type Output
   * @param prefix      of type String
   * @param printFields of type boolean
   */
  @ConstructorProperties({"output", "prefix", "printFields"})
  public Debug( Output output, String prefix, boolean printFields )
    {
    this.output = output;
    this.prefix = prefix;
    this.printFields = printFields;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to the declared stream and will print the Tuple instances
   * field names if printFields is true.
   *
   * @param output      of type Output
   * @param printFields of type boolean
   */
  @ConstructorProperties({"output", "printFields"})
  public Debug( Output output, boolean printFields )
    {
    this.output = output;
    this.printFields = printFields;
    }

  /**
   * Method getPrintFieldsEvery returns the printFieldsEvery interval value of this Debug object.
   *
   * @return the printFieldsEvery (type int) of this Debug object.
   */
  public int getPrintFieldsEvery()
    {
    return printFieldsEvery;
    }

  /**
   * Method setPrintFieldsEvery sets the printFieldsEvery interval value of this Debug object.
   *
   * @param printFieldsEvery the printFieldsEvery of this Debug object.
   */
  public void setPrintFieldsEvery( int printFieldsEvery )
    {
    this.printFieldsEvery = printFieldsEvery;
    }

  /**
   * Method getPrintTupleEvery returns the printTupleEvery interval value of this Debug object.
   *
   * @return the printTupleEvery (type int) of this Debug object.
   */
  public int getPrintTupleEvery()
    {
    return printTupleEvery;
    }

  /**
   * Method setPrintTupleEvery sets the printTupleEvery interval value of this Debug object.
   *
   * @param printTupleEvery the printTupleEvery of this Debug object.
   */
  public void setPrintTupleEvery( int printTupleEvery )
    {
    this.printTupleEvery = printTupleEvery;
    }

  @Override
  public boolean supportsPlannerLevel( PlannerLevel plannerLevel )
    {
    return plannerLevel instanceof DebugLevel;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Long> operationCall )
    {
    super.prepare( flowProcess, operationCall );

    operationCall.setContext( 0L );
    }

  /** @see Filter#isRemove(cascading.flow.FlowProcess, FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Long> filterCall )
    {
    PrintStream stream = output == Output.STDOUT ? System.out : System.err;

    if( printFields && filterCall.getContext() % printFieldsEvery == 0 )
      print( stream, filterCall.getArguments().getFields().print() );

    if( filterCall.getContext() % printTupleEvery == 0 )
      print( stream, filterCall.getArguments().getTuple().print() );

    filterCall.setContext( filterCall.getContext() + 1 );

    return false;
    }

  private void print( PrintStream stream, String message )
    {
    if( prefix != null )
      {
      stream.print( prefix );
      stream.print( ": " );
      }

    stream.println( message );
    }
  }
