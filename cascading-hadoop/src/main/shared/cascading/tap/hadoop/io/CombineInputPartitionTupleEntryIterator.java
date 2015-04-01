package cascading.tap.hadoop.io;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowProcess;
import cascading.tap.TapException;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.tuple.util.TupleViews;

public class CombineInputPartitionTupleEntryIterator implements Iterator<Tuple> {

  private final TupleEntrySchemeIterator childIterator;
  private final FlowProcess<JobConf> flowProcess;
  private final TupleEntry partitionEntry;
  private final Fields sourceFields;
  private final Partition partition;
  private final String parentIdentifier;

  private Tuple base;
  private Tuple view;
  private String currentFile;

  public CombineInputPartitionTupleEntryIterator(FlowProcess<JobConf> flowProcess, Fields sourceFields,
      Partition partition, String parentIdentifier, @SuppressWarnings("rawtypes") TupleEntrySchemeIterator childIterator) {
    this.flowProcess = flowProcess;
    this.partition = partition;
    this.parentIdentifier = parentIdentifier;
    this.childIterator = childIterator;
    this.sourceFields = sourceFields;
    partitionEntry = new TupleEntry(partition.getPartitionFields(), Tuple.size(partition.getPartitionFields().size()));
  }

  @Override
  public boolean hasNext() {
    return childIterator.hasNext();
  }

  @Override
  public Tuple next() {
    String currentFile = getCurrentFile();
    if (this.currentFile == null || !this.currentFile.equals(currentFile)) {
      this.currentFile = currentFile;
      try {
        String childIdentifier = new Path(currentFile).getParent().toString(); // drop part-xxxx
        partition.toTuple(childIdentifier.substring(parentIdentifier.length() + 1), partitionEntry);
      } catch (Exception exception) {
        throw new TapException("unable to parse partition given parent: " + parentIdentifier + " and child: "
            + currentFile, exception);
      }

      base = TupleViews.createOverride(sourceFields, partitionEntry.getFields());

      TupleViews.reset(base, Tuple.size(sourceFields.size()), partitionEntry.getTuple());

      view = TupleViews.createOverride(sourceFields, childIterator.getFields());
    }
    Tuple tuple = childIterator.next().getTuple();
    TupleViews.reset(view, base, tuple);

    return view;
  }

  private String getCurrentFile() {
    String result = flowProcess.getStringProperty("mapreduce.map.input.file");
    if (result == null) {
      result = flowProcess.getStringProperty("map.input.file");
    }
    return result;
  }

  @Override
  public void remove() {
    childIterator.remove();
  }

}
