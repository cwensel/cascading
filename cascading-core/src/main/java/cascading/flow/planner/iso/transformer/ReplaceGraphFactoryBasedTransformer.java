package cascading.flow.planner.iso.transformer;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.Match;

/**
 * GraphTransformer that uses the supplied factory to generate the replacement FlowElement.
 *
 * Note: This only works if exactly one FlowElement is being replaced.
 */
public class ReplaceGraphFactoryBasedTransformer extends ReplaceGraphTransformer {

  private final String factoryName;

  public ReplaceGraphFactoryBasedTransformer(
    GraphTransformer graphTransformer,
    ExpressionGraph filter,
    String factoryName) {

    super(graphTransformer, filter);
    this.factoryName = factoryName;
  }

  @Override
  protected boolean transformGraphInPlaceUsing(
    Transformed<ElementGraph> transformed,
    ElementGraph graph,
    Match match) {

    Set<FlowElement> captured = match.getCapturedElements(ElementCapture.Primary);
    if (captured.isEmpty()) {
      return false;
    } else if (captured.size() != 1) {
      throw new IllegalStateException("Expected one, but found multiple flow elements in the match expression: " + captured);
    } else {
      FlowElement replace = captured.iterator().next();
      ElementFactory elementFactory = transformed.getPlannerContext().getElementFactoryFor(factoryName);
      FlowElement replaceWith = elementFactory.create(graph, replace);
      ElementGraphs.replaceElementWith(graph, replace, replaceWith);
      return true;
    }
  }
}
