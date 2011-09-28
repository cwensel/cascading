/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.util.Set;

import cascading.tuple.Fields;

/** Interface FlowElement is a utility interface used internally to simplify DAG management. */
public interface FlowElement
  {
  /**
   * Method outgoingScopeFor returns the Scope this FlowElement hands off to the next FlowElement.
   *
   * @param incomingScopes of type Set<Scope>
   * @return Scope
   */
  Scope outgoingScopeFor( Set<Scope> incomingScopes );

  /**
   * Method resolveIncomingOperationFields resolves the incoming scopes to the actual incoming operation field names.
   *
   * @param incomingScope of type Scope
   * @return Fields
   */
  Fields resolveIncomingOperationFields( Scope incomingScope );

  /**
   * Method resolveFields returns the actual field names represented by the given Scope. The scope may be incoming
   * or outgoing in relation to this FlowElement instance.
   *
   * @param scope of type Scope
   * @return Fields
   */
  Fields resolveFields( Scope scope ); // todo: consider moving this functionality to Scope

  boolean isEquivalentTo( FlowElement element );
  }