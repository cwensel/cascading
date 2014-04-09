package cascading.flow.planner.rule;

import java.io.File;
import java.util.List;
import java.util.Map;

import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.Transform;

public class TraceWriter
  {
  String transformTracePath;

  public TraceWriter()
    {
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, Transform transform )
    {
    if( transformTracePath == null )
      {
      return;
      }

    String ruleName = transform.getRuleName();

    ruleName = String.format( "%d-%s-%04d-%s", phase.ordinal(), phase, ruleOrdinal, ruleName );

    transform.writeDOTs( new File( transformTracePath, ruleName ).toString() );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, Partitions partition )
    {
    if( transformTracePath == null )
      {
      return;
      }

    String ruleName = partition.getRuleName();

    ruleName = String.format( "%d-%s-%04d-%s", phase.ordinal(), phase, ruleOrdinal, ruleName );

    partition.writeDOTs( new File( transformTracePath, ruleName ).toString() );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, Partitions partition )
    {
    if( transformTracePath == null )
      {
      return;
      }

    String ruleName = partition.getRuleName();

    ruleName = String.format( "%d-%s-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, ruleName );

    partition.writeDOTs( new File( transformTracePath, ruleName ).toString() );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, int nodeOrdinal, Partitions partition )
    {
    if( transformTracePath == null )
      {
      return;
      }

    String ruleName = partition.getRuleName();

    ruleName = String.format( "%d-%s-%04d-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, nodeOrdinal, ruleName );

    partition.writeDOTs( new File( transformTracePath, ruleName ).toString() );
    }

  void writePlan( FlowElementGraph flowElementGraph, String name )
    {
    if( transformTracePath == null )
      {
      return;
      }

    File file = new File( transformTracePath, name );

    RuleExec.LOG.info( "writing phase graph trace: {}, to: {}", name, file );

    flowElementGraph.writeDOT( file.toString() );
    }

  void writePlan( List<ElementGraph> flowElementGraphs, PlanPhase phase, String subName )
    {
    if( transformTracePath == null )
      {
      return;
      }

    for( int i = 0; i < flowElementGraphs.size(); i++ )
      {
      ElementGraph flowElementGraph = flowElementGraphs.get( i );
      String name = String.format( "%d-%s-%s-%04d.dot", phase.ordinal(), phase, subName, i );

      File file = new File( transformTracePath, name );

      RuleExec.LOG.info( "writing phase step sub-graph trace: {}, to: {}", name, file );

      flowElementGraph.writeDOT( file.toString() );
      }
    }

  void writePlan( Map<ElementGraph, List<ElementGraph>> parentGraphsMap, Map<ElementGraph, List<ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( transformTracePath == null )
      {
      return;
      }

    int stepCount = 0;
    for( Map.Entry<ElementGraph, List<ElementGraph>> entry : parentGraphsMap.entrySet() )
      {
      int nodeCount = 0;
      for( ElementGraph elementGraph : entry.getValue() )
        {
        List<ElementGraph> pipelineGraphs = subGraphsMap.get( elementGraph );

        for( int i = 0; i < pipelineGraphs.size(); i++ )
          {
          ElementGraph flowElementGraph = pipelineGraphs.get( i );
          String name = String.format( "%d-%s-%s-%04d-%04d-%04d.dot", phase.ordinal(), phase, subName, stepCount, nodeCount, i );

          File file = new File( transformTracePath, name );

          RuleExec.LOG.info( "writing phase node pipeline sub-graph trace: {}, to: {}", name, file );

          flowElementGraph.writeDOT( file.toString() );
          }

        nodeCount++;
        }

      stepCount++;
      }
    }

  void writePlan( Map<ElementGraph, List<ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( transformTracePath == null )
      {
      return;
      }

    int stepCount = 0;
    for( Map.Entry<ElementGraph, List<ElementGraph>> entry : subGraphsMap.entrySet() )
      {
      List<ElementGraph> flowElementGraphs = entry.getValue();

      for( int i = 0; i < flowElementGraphs.size(); i++ )
        {
        ElementGraph flowElementGraph = flowElementGraphs.get( i );
        String name = String.format( "%d-%s-%s-%04d-%04d.dot", phase.ordinal(), phase, subName, stepCount, i );

        File file = new File( transformTracePath, name );

        RuleExec.LOG.info( "writing phase node sub-graph trace: {}, to: {}", name, file );

        flowElementGraph.writeDOT( file.toString() );
        }

      stepCount++;
      }
    }
  }