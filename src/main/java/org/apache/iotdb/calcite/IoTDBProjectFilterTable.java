package org.apache.iotdb.calcite;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;

public class IoTDBProjectFilterTable extends IoTDBTable implements ProjectableFilterableTable {

  public IoTDBProjectFilterTable(String storageGroup) {
    super(storageGroup);
  }

  public String toString() {
    return "IoTDBProjectFilterTable";
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
//    System.out.println(projects);
    String filterString = processFilters(filters);
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new IoTDBEnumerator<>(storageGroupName, cancelFlag, false, null,
            new IoTDBEnumerator.ArrayRowConverter(fieldTypes, projects));
      }
    };
  }

  private String processFilters(List<RexNode> filters) {
    StringBuilder filterString = new StringBuilder("where ");
    for (RexNode filterNode : filters) {
      RexCall filterCall = (RexCall) filterNode;
//      String filterPre = filterNode.toString();
      StringBuilder filterSubString = new StringBuilder();
      filterSubString
          .append(this.fieldNames.get(((RexInputRef) filterCall.operands.get(0)).getIndex()));
      filterSubString.append(filterCall.op.toString());
      filterSubString.append(filterCall.operands.get(1).toString());
      filterString.append(filterSubString);
    }
    System.out.println(filterString);
    return filterString.toString();
  }

//  private String process
}
