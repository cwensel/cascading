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

import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import org.apache.log4j.Logger;

/**
 * Class RegexMatcher is the base class for common regular expression operations.
 * <p/>
 * This operation uses {@link java.util.regex.Matcher} internally, specifically the method {@link java.util.regex.Matcher#find()}.
 *
 * @see java.util.regex.Matcher
 * @see java.util.regex.Pattern
 */
public class RegexMatcher extends RegexOperation<Matcher>
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( RegexMatcher.class );
  /** Field removeMatch */
  protected final boolean negateMatch;

  protected RegexMatcher( String patternString )
    {
    super( patternString );
    this.negateMatch = false;
    }

  protected RegexMatcher( String patternString, boolean negateMatch )
    {
    super( patternString );
    this.negateMatch = negateMatch;
    }

  @Override
  public void prepare( FlowProcess<Matcher> flowProcess )
    {
    flowProcess.setContext( getPattern().matcher( "" ) );
    }

  /**
   * Method matchWholeTuple ...
   *
   * @param flowProcess
   * @param input       of type Tuple @return boolean
   */
  protected boolean matchWholeTuple( FlowProcess<Matcher> flowProcess, Tuple input )
    {
    Matcher matcher = flowProcess.getContext().reset( input.toString( "\t" ) );

    boolean matchFound = matcher.find();

    if( LOG.isDebugEnabled() )
      LOG.debug( "pattern: " + getPattern() + ", matches: " + matchFound );

    return matchFound == negateMatch;
    }

  /**
   * Method matchEachElement ...
   *
   * @param flowProcess
   * @param input       of type Tuple @return boolean
   */
  protected boolean matchEachElement( FlowProcess<Matcher> flowProcess, Tuple input )
    {
    return matchEachElementPos( flowProcess, input ) != -1;
    }

  protected int matchEachElementPos( FlowProcess<Matcher> flowProcess, Tuple input )
    {
    int pos = 0;
    for( Object value : input )
      {
      if( value == null )
        value = "";

      Matcher matcher = flowProcess.getContext().reset( value.toString() );

      boolean matchFound = matcher.find();

      if( LOG.isDebugEnabled() )
        LOG.debug( "pattern: " + getPattern() + ", matches: " + matchFound + ", element: '" + value + "'" );

      if( matchFound == negateMatch )
        return pos;

      pos++;
      }

    return -1;
    }
  }
