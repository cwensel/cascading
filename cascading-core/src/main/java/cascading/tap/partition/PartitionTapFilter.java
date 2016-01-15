package cascading.tap.partition;

import java.io.Serializable;

import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

class PartitionTapFilter implements Serializable
  {
  private final Fields argumentFields;
  private final Filter filter;
  private transient FilterCall<?> filterCall;

  PartitionTapFilter( Fields argumentSelector, Filter filter )
    {
    this.argumentFields = argumentSelector;
    this.filter = filter;
    }

  protected FilterCall<?> getFilterCall()
    {
    if( filterCall == null )
      filterCall = new ConcreteCall( argumentFields );

    return filterCall;
    }

  private FilterCall<?> getFilterCallWith( TupleEntry arguments )
    {
    FilterCall<?> filterCall = getFilterCall();

    ( (ConcreteCall) filterCall ).setArguments( arguments );

    return filterCall;
    }

  void prepare( FlowProcess flowProcess )
    {
    filter.prepare( flowProcess, getFilterCall() );
    }

  boolean isRemove( FlowProcess flowProcess, TupleEntry partitionEntry )
    {
    return filter.isRemove( flowProcess, getFilterCallWith( partitionEntry.selectEntry( argumentFields ) ) );
    }

  void cleanup( FlowProcess flowProcess )
    {
    filter.cleanup( flowProcess, getFilterCall() );
    }
  }
