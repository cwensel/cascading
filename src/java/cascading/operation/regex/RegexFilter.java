/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import java.util.regex.Matcher;

import cascading.operation.Filter;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.log4j.Logger;

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
public class RegexFilter extends RegexOperation implements Filter
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( RegexFilter.class );

  /** Field matchEachElement */
  private boolean matchEachElement = false;
  /** Field removeMatch */
  private boolean removeMatch = false;

  /**
   * Constructor RegexFilter creates a new RegexFilter instance.
   *
   * @param patternString of type String
   */
  public RegexFilter( String patternString )
    {
    super( ANY, Fields.ALL, patternString );
    }

  /**
   * Constructor RegexFilter creates a new RegexFilter instance.
   *
   * @param patternString of type String
   * @param removeMatch   of type boolean
   */
  public RegexFilter( String patternString, boolean removeMatch )
    {
    super( ANY, Fields.ALL, patternString );
    this.removeMatch = removeMatch;
    }

  /**
   * @param patternString    of type String
   * @param removeMatch      of type boolean, set to true if a match should be filtered
   * @param matchEachElement of type boolean, set to true if each element should be matched individually
   */
  public RegexFilter( String patternString, boolean removeMatch, boolean matchEachElement )
    {
    super( ANY, patternString );
    this.removeMatch = removeMatch;
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

  /**
   * Method matchWholeTuple ...
   *
   * @param input of type Tuple
   * @return boolean
   */
  private boolean matchWholeTuple( Tuple input )
    {
    Matcher matcher = getPattern().matcher( input.toString() );

    if( LOG.isDebugEnabled() )
      LOG.debug( "pattern: " + getPattern() + ", matches: " + matcher.matches() );

    return matcher.matches() == removeMatch;
    }

  /**
   * Method matchEachElement ...
   *
   * @param input of type Tuple
   * @return boolean
   */
  private boolean matchEachElement( Tuple input )
    {
    for( Object value : input )
      {
      if( value == null )
        value = "";

      Matcher matcher = getPattern().matcher( value.toString() );

      if( LOG.isDebugEnabled() )
        LOG.debug( "pattern: " + getPattern() + ", matches: " + matcher.matches() + ", element: '" + value + "'" );

      if( matcher.matches() == removeMatch )
        return true;
      }

    return false;
    }
  }
