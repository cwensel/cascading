/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.rule;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;

/**
 *
 */
public class RuleRegistrySet
  {
  LinkedHashSet<RuleRegistry> ruleRegistries = new LinkedHashSet<>();

  public RuleRegistrySet( RuleRegistry... ruleRegistries )
    {
    Collections.addAll( this.ruleRegistries, ruleRegistries );
    }

  public RuleRegistrySet( Collection<RuleRegistry> ruleRegistries )
    {
    this.ruleRegistries.addAll( ruleRegistries );
    }

  public RuleRegistry findRegistryWith( String ruleName )
    {
    for( RuleRegistry ruleRegistry : ruleRegistries )
      {
      if( ruleRegistry.hasRule( ruleName ) )
        return ruleRegistry;
      }

    return null;
    }
  }
