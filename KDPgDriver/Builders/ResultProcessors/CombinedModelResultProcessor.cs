using System;
using System.Collections.Generic;
using KDPgDriver.Types;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders.ResultProcessors
{
  public class CombinedModelSelectResultProcessor<TModel> : ISelectResultProcessor
  {
    private static readonly KdPgTableDescriptor Table = ModelsRegistry.GetTable<TModel>();

    // private bool _useAllColumns = true;
    private List<KdPgColumnDescriptor> _columns = Table.Columns;
    public int FieldsCount => 1 + _columns.Count;

    public object ParseResult(object[] values)
    {
      var obj = Activator.CreateInstance(Table.ModelType);

      var isNull = (bool)PgTypesConverter.ConvertFromRawSqlValue(KDPgValueTypeInstances.Boolean, values[0]);
      if (isNull)
        return null;

      for (var i = 0; i < _columns.Count; i++) {
        var col = _columns[i];
        var val = PgTypesConverter.ConvertFromRawSqlValue(col.Type, values[1 + i]);
        col.PropertyInfo.SetValue(obj, val);
      }

      return obj;
    }
  }
}