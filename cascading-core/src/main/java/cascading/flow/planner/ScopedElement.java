/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner;

import java.util.Set;

import cascading.property.ConfigDef;
import cascading.tuple.Fields;

/**
 *
 */
public interface ScopedElement
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

  ConfigDef getConfigDef();

  boolean hasConfigDef();

  ConfigDef getNodeConfigDef();

  boolean hasNodeConfigDef();

  ConfigDef getStepConfigDef();

  boolean hasStepConfigDef();
  }