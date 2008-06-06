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

package cascading.operation.regex;

import cascading.operation.Filter;
import cascading.tuple.TupleEntry;

/**
 * Class RegexFilter will apply the regex patternString against every input Tuple value and filter
 * the Tuple stream accordingly.
 * <p/>
 * By default, Tuples that match the given pattern are kept, and Tuples that do not
 * match are filtered out. This can be changed by setting removeMatch to true.
 * <p/>
 * Also, by default, the whole Tuple is matched against the given patternString (tab delimited). If matchEachElement
 * is set to true, the pattern is applied to each Tuple value individually.
 */
public class RegexFilter extends RegexMatcher implements Filter
  {
  /** Field matchEachElement */
  protected final boolean matchEachElement;

  /**
   * Constructor RegexFilter creates a new RegexFilter instance.
   *
   * @param patternString of type String
   */
  public RegexFilter( String patternString )
    {
    super( patternString );
    this.matchEachElement = false;
    }

  /**
   * Constructor RegexFilter creates a new RegexFilter instance.
   *
   * @param patternString of type String
   * @param removeMatch   of type boolean
   */
  public RegexFilter( String patternString, boolean removeMatch )
    {
    super( patternString, removeMatch );
    this.matchEachElement = false;

    }

  /**
   * @param patternString    of type String
   * @param removeMatch      of type boolean, set to true if a match should be filtered
   * @param matchEachElement of type boolean, set to true if each element should be matched individually
   */
  public RegexFilter( String patternString, boolean removeMatch, boolean matchEachElement )
    {
    super( patternString, removeMatch );
    this.matchEachElement = matchEachElement;
    }

  /** @see Filter#isRemove(TupleEntry) */
  public boolean isRemove( TupleEntry input )
    {
    if( matchEachElement )
      return matchEachElement( input.getTuple() );
    else
      return matchWholeTuple( input.getTuple() );
    }

  }
