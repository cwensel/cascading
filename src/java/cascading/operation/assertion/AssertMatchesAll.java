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

package cascading.operation.assertion;

import cascading.operation.ValueAssertion;
import cascading.operation.regex.RegexMatcher;
import cascading.tuple.TupleEntry;

/**
 * Class AssertMatchesAll matches the given regular expression patternString against each argument
 * {@link cascading.tuple.Tuple} element individually. See {@link AssertMatches} if you need to match the patternString regex against
 * the tuple as a whole.
 */
public class AssertMatchesAll extends RegexMatcher implements ValueAssertion
  {
  /** Field message */
  private final String message = "argument '%s' value was: %s, did not match: %s, in tuple: %s";

  /**
   * Constructor AssertMatchesAll creates a new AssertMatchesAll instance.
   *
   * @param patternString of type String
   */
  public AssertMatchesAll( String patternString )
    {
    super( patternString, false );
    }

  /**
   * Constructor AssertMatchesAll creates a new AssertMatchesAll instance.
   *
   * @param patternString of type String
   * @param negateMatch   of type boolean
   */
  public AssertMatchesAll( String patternString, boolean negateMatch )
    {
    super( patternString, negateMatch );
    }

  /** @see cascading.operation.ValueAssertion#doAssert(TupleEntry) */
  public void doAssert( TupleEntry input )
    {
    int pos = matchEachElementPos( input.getTuple() );

    if( pos != -1 )
      BaseAssertion.throwFail( message, input.getFields().get( pos ), input.get( pos ), patternString, input.getTuple().print() );
    }
  }