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

package cascading.operation.regex;

import java.beans.ConstructorProperties;
import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

/**
 * Class RegexFilter will apply the regex patternString against every input Tuple value and filter
 * the Tuple stream accordingly.
 * <p/>
 * By default, Tuples that match the given pattern are kept, and Tuples that do not
 * match are filtered out. This can be changed by setting removeMatch to true.
 * <p/>
 * Also, by default, the whole Tuple is matched against the given patternString (tab delimited). If matchEachElement
 * is set to true, the pattern is applied to each Tuple value individually.
 * <p/>
 * This operation uses {@link java.util.regex.Matcher} internally, specifically the method {@link java.util.regex.Matcher#find()}.
 *
 * @see java.util.regex.Matcher
 * @see java.util.regex.Pattern
 */
public class RegexFilter extends RegexMatcher implements Filter<Matcher>
  {
  /** Field matchEachElement */
  protected final boolean matchEachElement;

  /**
   * Constructor RegexFilter creates a new RegexFilter instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
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
  @ConstructorProperties({"patternString", "removeMatch"})
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
  @ConstructorProperties({"patternString", "removeMatch", "matchEachElement"})
  public RegexFilter( String patternString, boolean removeMatch, boolean matchEachElement )
    {
    super( patternString, removeMatch );
    this.matchEachElement = matchEachElement;
    }

  /** @see Filter#isRemove(cascading.flow.FlowProcess,cascading.operation.FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Matcher> filterCall )
    {
    if( matchEachElement )
      return matchEachElement( filterCall.getContext(), filterCall.getArguments().getTuple() );
    else
      return matchWholeTuple( filterCall.getContext(), filterCall.getArguments().getTuple() );
    }

  }
