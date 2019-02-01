/*
 * Copyright (c) 2018-2019 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.neo4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import iot.jcypher.database.internal.PlannerStrategy;
import iot.jcypher.query.JcQuery;
import iot.jcypher.query.api.IClause;
import iot.jcypher.query.api.pattern.Node;
import iot.jcypher.query.api.pattern.Relation;
import iot.jcypher.query.factories.clause.MERGE;
import iot.jcypher.query.factories.clause.ON_CREATE;
import iot.jcypher.query.values.JcNode;
import iot.jcypher.query.values.JcRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Current expects a fairly shallow document.
 * <p>
 * Any child Objects (map) will be flattened into the root using the name as a prefix for the key.
 * e.g. `tag.start` and `tag.end`.
 * <p>
 * A nested array will be handed to Neo as an Array. So far Maps nested in an Array work fine.
 */
public class Neo4jJSONStatement extends Neo4jStatement<JsonNode>
  {
  private static final Logger LOG = LoggerFactory.getLogger( Neo4jJSONStatement.class );

  JSONGraphSpec graphSpec;
  private ObjectMapper objectMapper = new ObjectMapper();

  public Neo4jJSONStatement( JSONGraphSpec graphSpec )
    {
    this.graphSpec = graphSpec;
    }

  @Override
  public JcQuery getStatement( JsonNode json )
    {
    List<IClause> clauses = new ArrayList<>();

    JcNode head = new JcNode( "n" );
    Node merge = MERGE.node( head );

    if( graphSpec.hasNodeLabel() )
      merge.label( graphSpec.getNodeLabel() );

    if( graphSpec.hasProperties() )
      applyProperties( json, merge, graphSpec.getProperties(), false );

    clauses.add( merge );

    Map<String, Object> propertyValues = asProperties( graphSpec.getValuesPointer().apply( json ) );

    for( Map.Entry<String, Object> entry : propertyValues.entrySet() )
      clauses.add( ON_CREATE.SET( head.property( entry.getKey() ) ).to( entry.getValue() ) );

    if( graphSpec.hasEdges() )
      {
      Set<JSONGraphSpec.EdgeSpec> edges = graphSpec.getEdges();

      int count = 0;

      for( JSONGraphSpec.EdgeSpec edge : edges )
        {
        JcNode target = new JcNode( "t".concat( Integer.toString( count ) ) );
        Node targetMerge = MERGE.node( target );

        if( !edge.hasTargetLabel() && !edge.hasTargetProperties() )
          {
          LOG.debug( "edge, {}, has no match patterns", edge.getEdgeType() );
          continue;
          }

        if( edge.hasTargetLabel() )
          targetMerge.label( edge.getTargetLabel() );

        if( edge.hasTargetProperties() )
          {
          int propertiesFound = applyProperties( json, targetMerge, edge.getTargetProperties(), true );

          if( propertiesFound == 0 )
            {
            LOG.debug( "edge, {}, has no match properties", edge.getEdgeType() );
            continue;
            }
          }

        JcRelation relation = new JcRelation( "r".concat( Integer.toString( count ) ) );
        Relation intermediate = MERGE.node( head ).relation( relation );

        if( edge.hasEdgeType() )
          intermediate.type( edge.getEdgeType() );

        Node relationMerge = intermediate.out().node( target );

        clauses.add( targetMerge );
        clauses.add( relationMerge );

        count++;
        }
      }

    JcQuery query = new JcQuery( PlannerStrategy.DEFAULT ); // force to default, otherwise warnings

    query.setClauses( clauses.toArray( new IClause[ 0 ] ) );

    return query;
    }

  public int applyProperties( JsonNode json, Node merge, Map<String, Function<JsonNode, Object>> properties, boolean ignoreNull )
    {
    int count = 0;

    for( Map.Entry<String, Function<JsonNode, Object>> entry : properties.entrySet() )
      {
      Object value = entry.getValue().apply( json );

      if( ignoreNull && value == null )
        continue;
      else if( value == null )
        throw new IllegalStateException( "property: " + entry.getKey() + ", many not be null" );

      merge.property( entry.getKey() ).value( value );
      count++;
      }

    return count;
    }

  public Map<String, Object> asProperties( JsonNode node )
    {
    Map<String, Object> result = new LinkedHashMap<>();

    Map<String, Object> map = objectMapper.convertValue( node, Map.class );

    nest( result, null, map );

    return result;
    }

  private void nest( Map<String, Object> result, String prefix, Map<String, Object> map )
    {
    for( Map.Entry<String, Object> entry : map.entrySet() )
      {
      String currentKey = clean( entry.getKey() );
      Object value = entry.getValue();

      String nestedKey = prefix == null ? currentKey : prefix + "." + currentKey;

      if( value instanceof Map )
        nest( result, nestedKey, (Map<String, Object>) value );
      else
        result.put( nestedKey, value ); // any nested lists stay lists
      }
    }

  private String clean( String key )
    {
    return key.replace( ':', '-' ).replace( '-', '_' );
    }
  }
