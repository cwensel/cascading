/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.Set;

import cascading.flow.planner.Scope;
import cascading.property.ConfigDef;
import cascading.tuple.Fields;

/**
 * Interface FlowElement is a utility interface used internally to simplify DAG management. It is not intended
 * for users to interact with these methods directly.
 */
public interface FlowElement extends Serializable
  {
  /**
   * Method outgoingScopeFor returns the Scope this FlowElement hands off to the next FlowElement.
   *
   * @param incomingScopes of type Set<Scope>
   * @return Scope
   */
  Scope outgoingScopeFor( Set<Scope> incomingScopes );

  /**
   * Method resolveIncomingOperationArgumentFields returns the Fields outgoing from the previous FlowElement that
   * are consumable by this FlowElement when preparing Operation arguments.
   *
   * @param incomingScope of type Scope
   * @return Fields
   */
  Fields resolveIncomingOperationArgumentFields( Scope incomingScope );

  /**
   * Method resolveIncomingOperationPassThroughFields returns the Fields outgoing from the previous FlowElement that
   * are consumable by this FlowElement when preparing the Pipe outgoing tuple.
   *
   * @param incomingScope of type Scope
   * @return Fields
   */
  Fields resolveIncomingOperationPassThroughFields( Scope incomingScope );

  boolean isEquivalentTo( FlowElement element );

  ConfigDef getStepConfigDef();

  boolean hasStepConfigDef();

  ConfigDef getConfigDef();

  boolean hasConfigDef();
  }