/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import java.util.Iterator;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.pipe.Group;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class GroupReducerStackElement extends FlowReducerStackElement
  {
  private Group group;
  private Set<Scope> incomingScopes;
  private Scope thisScope;
  private final JobConf jobConf;

  public GroupReducerStackElement( Set<Scope> incomingScopes, Group group, Scope thisScope, Fields outGroupingFields, JobConf jobConf )
    {
    super( outGroupingFields );
    this.group = group;
    this.incomingScopes = incomingScopes;
    this.thisScope = thisScope;
    this.jobConf = jobConf;
    }

  public FlowElement getFlowElement()
    {
    return group;
    }

  public void collect( Tuple key, Iterator values )
    {
    operateGroup( key, values );
    }

  private void operateGroup( Tuple key, Iterator values )
    {
    // if a cogroup group instance...
    // an ungrouping iterator to partition the values back into a tuple so reduce stack can run
    // this can be one big tuple. the values iterator will have one Tuple of the format:
    // [ [key] [group1] [group2] ] where [groupX] == [ [...] [...] ...], a cogroup for each source
    // this can be nasty
    values = group.makeReduceValues( jobConf, incomingScopes, thisScope, key, values );

    values = new TupleEntryIterator( next.resolveIncomingOperationFields(), values );

    next.collect( key, values );
    }

  }
