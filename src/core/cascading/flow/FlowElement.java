/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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