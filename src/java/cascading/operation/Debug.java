/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import java.io.PrintStream;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class Debug is a {@link Filter} that will never remove an item from a stream, but will print the Tuple to either
 * stdout or stderr.
 */
public class Debug extends Operation implements Filter
  {
  static public enum Output
    {
      STDOUT, STDERR
    }

  /** Field output */
  private Output output = Output.STDERR;
  /** Field printFields */
  private boolean printFields = false;


  /**
   * Constructor Debug creates a new Debug instance that prints to stderr by default, and does not print
   * the Tuple instance field names.
   */
  public Debug()
    {
    super( ANY, Fields.ALL );
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to stderr and will print the current
   * Tuple instance field names if printFields is true.
   *
   * @param printFields of type boolean
   */
  public Debug( boolean printFields )
    {
    this();
    this.printFields = printFields;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to the declared stream and does not print the Tuple
   * field names.
   *
   * @param output of type Output
   */
  public Debug( Output output )
    {
    this();
    this.output = output;
    }

  /**
   * Constructor Debug creates a new Debug instance that prints to the declared stream and will print the Tuple instances
   * field names if printFields is true.
   *
   * @param output of type Output
   */
  public Debug( Output output, boolean printFields )
    {
    this();
    this.output = output;
    this.printFields = printFields;
    }

  /** @see Filter#isRemove(TupleEntry) */
  public boolean isRemove( TupleEntry input )
    {
    PrintStream stream = output == Output.STDOUT ? System.out : System.err;

    if( printFields )
      stream.println( input.getFields().print() );

    stream.println( input.getTuple().print() );

    return false;
    }
  }
