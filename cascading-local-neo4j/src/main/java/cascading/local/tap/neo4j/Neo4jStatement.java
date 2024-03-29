/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

import java.util.Map;

import cascading.util.LogUtil;
import iot.jcypher.database.util.QParamsUtil;
import iot.jcypher.query.JcQuery;
import iot.jcypher.query.writer.CypherWriter;
import iot.jcypher.query.writer.Format;
import iot.jcypher.query.writer.QueryParam;
import iot.jcypher.query.writer.WriterContext;
import iot.jcypher.util.Util;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Neo4j query generation.
 */
public abstract class Neo4jStatement<T>
  {
  private static final Logger LOG = LoggerFactory.getLogger( Neo4jStatement.class );

  /**
   * Method enableDebugLogging enables logging of the generated cypher query to the DEBUG log.
   */
  public static void enableDebugLogging()
    {
    LogUtil.setLog4jLevel( Neo4jStatement.class.getName(), "debug" );
    }

  public abstract JcQuery getStatement( T node );

  public StatementResult runStatement( Transaction tx, T node )
    {
    JcQuery query = getStatement( node );

    if( LOG.isDebugEnabled() )
      LOG.debug( "cypher: {}", Util.toCypher( query, Format.NONE ) );

    WriterContext context = new WriterContext();
    QueryParam.setExtractParams( query.isExtractParams(), context );
    CypherWriter.toCypherExpression( query, context );

    String cypher = context.buffer.toString();
    Map<String, Object> paramsMap = QParamsUtil.createQueryParams( context );

    return tx.run( cypher, paramsMap );
    }
  }
