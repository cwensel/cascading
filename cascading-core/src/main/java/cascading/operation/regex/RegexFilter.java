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

  public boolean isMatchEachElement()
    {
    return matchEachElement;
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Matcher> filterCall )
    {
    if( matchEachElement )
      return matchEachElement( filterCall.getContext(), filterCall.getArguments().getTuple() );
    else
      return matchWholeTuple( filterCall.getContext(), filterCall.getArguments().getTuple() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof RegexFilter ) )
      return false;
    if( !super.equals( object ) )
      return false;

    RegexFilter that = (RegexFilter) object;

    if( matchEachElement != that.matchEachElement )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( matchEachElement ? 1 : 0 );
    return result;
    }
  }
