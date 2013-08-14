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
import java.util.Arrays;
import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/**
 * Class RegexParser is used to extract a matched regex from an incoming argument value.
 * <p/>
 * Sometimes its useful to parse out a value from a key/value pair in a string, if the key exists. If the key does
 * not exist, returning an empty string instead of failing is typically expected.
 * <p/>
 * The following regex can extract a value from {@code key1=value1&key2=value2} if key1 exists, otherwise an
 * empty string is returned:<br/>
 * <pre>(?<=key1=)[^&]*|$</pre>
 */
public class RegexParser extends RegexOperation<Pair<Matcher, Tuple>> implements Function<Pair<Matcher, Tuple>>
  {
  /** Field groups */
  private int[] groups = null;

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the argument Tuple value is matched and returned
   * in a new Tuple.
   * <p/>
   * If the given patternString declares regular expression groups, each group will be returned as a value in the
   * resulting Tuple. If no groups are declared, the match will be returned as the only value in the resulting Tuple.
   * <p/>
   * The fields returned will be {@link Fields#UNKNOWN}, so a variable number of values may be emitted based on the
   * regular expression given.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
  public RegexParser( String patternString )
    {
    super( 1, patternString );
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the argument Tuple value is matched and returned
   * as the given Field.
   * <p/>
   * If the given patternString declares regular expression groups, each group will be returned as a value in the
   * resulting Tuple. If no groups are declared, the match will be returned as the only value in the resulting Tuple.
   * <p/>
   * If the number of fields in the fieldDeclaration does not match the number of groups matched, an {@link OperationException}
   * will be thrown during runtime.
   * <p/>
   * To overcome this, either use the constructors that take an array of groups, or use the {@code (?: ...)} sequence
   * to tell the regular expression matcher to not capture the group.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   */
  @ConstructorProperties({"fieldDeclaration", "pattenString"})
  public RegexParser( Fields fieldDeclaration, String patternString )
    {
    super( 1, fieldDeclaration, patternString );
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the patternString is a regular expression
   * with match groups and whose groups designated by {@code groups} are stored in the appropriate number of new fields.
   * <p/>
   * The number of resulting fields will match the number of groups given ({@code groups.length}).
   *
   * @param patternString of type String
   * @param groups        of type int[]
   */
  @ConstructorProperties({"patternString", "groups"})
  public RegexParser( String patternString, int[] groups )
    {
    super( 1, Fields.size( verifyReturnLength( groups ) ), patternString );

    this.groups = Arrays.copyOf( groups, groups.length );
    }

  private static int verifyReturnLength( int[] groups )
    {
    if( groups == null || groups.length == 0 )
      throw new IllegalArgumentException( "groups may not be null or 0 length" );

    return groups.length;
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the patternString is a regular expression
   * with match groups and whose groups designated by {@code groups} are stored in the named fieldDeclarations.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   * @param groups           of type int[]
   */
  @ConstructorProperties({"fieldDeclaration", "patternString", "groups"})
  public RegexParser( Fields fieldDeclaration, String patternString, int[] groups )
    {
    super( 1, fieldDeclaration, patternString );

    verifyReturnLength( groups );

    this.groups = Arrays.copyOf( groups, groups.length );

    if( !fieldDeclaration.isUnknown() && fieldDeclaration.size() != groups.length )
      throw new IllegalArgumentException( "fieldDeclaration must equal number of groups to be captured, fields: " + fieldDeclaration.print() );
    }

  public int[] getGroups()
    {
    if( groups == null )
      return null;

    return Arrays.copyOf( groups, groups.length );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Pair<Matcher, Tuple>> operationCall )
    {
    operationCall.setContext( new Pair<Matcher, Tuple>( getPattern().matcher( "" ), new Tuple() ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<Matcher, Tuple>> functionCall )
    {
    String value = functionCall.getArguments().getString( 0 );

    if( value == null )
      value = "";

    Matcher matcher = functionCall.getContext().getLhs().reset( value );

    if( !matcher.find() )
      throw new OperationException( "could not match pattern: [" + getPatternString() + "] with value: [" + value + "]" );

    Tuple output = functionCall.getContext().getRhs();

    output.clear();

    if( groups != null )
      onGivenGroups( functionCall, matcher, output );
    else
      onFoundGroups( functionCall, matcher, output );
    }

  private final void onFoundGroups( FunctionCall<Pair<Matcher, Tuple>> functionCall, Matcher matcher, Tuple output )
    {
    int count = matcher.groupCount();

    if( count == 0 )
      {
      output.add( matcher.group( 0 ) );
      }
    else
      {
      for( int i = 0; i < count; i++ )
        output.add( matcher.group( i + 1 ) ); // skip group 0
      }

    functionCall.getOutputCollector().add( output );
    }

  private final void onGivenGroups( FunctionCall<Pair<Matcher, Tuple>> functionCall, Matcher matcher, Tuple output )
    {
    for( int pos : groups )
      output.add( matcher.group( pos ) );

    functionCall.getOutputCollector().add( output );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof RegexParser ) )
      return false;
    if( !super.equals( object ) )
      return false;

    RegexParser that = (RegexParser) object;

    if( !Arrays.equals( groups, that.groups ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( groups != null ? Arrays.hashCode( groups ) : 0 );
    return result;
    }
  }
