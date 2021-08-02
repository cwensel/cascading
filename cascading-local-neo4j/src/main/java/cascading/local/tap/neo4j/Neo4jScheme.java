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

import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import org.neo4j.driver.v1.Session;

/**
 *
 */
public abstract class Neo4jScheme extends Scheme<Properties, Void, Session, Neo4jScheme.Context, Neo4jScheme.Context>
  {
  class Context<T>
    {
    Neo4jStatement<T> statement;

    public Context( Neo4jStatement<T> statement )
      {
      this.statement = statement;
      }
    }

  public Neo4jScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }
  }
