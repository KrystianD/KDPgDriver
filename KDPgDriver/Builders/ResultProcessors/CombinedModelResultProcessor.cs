﻿using System;
using System.Collections.Generic;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public class CombinedModelResultProcessor<TModel> : IResultProcessor
  {
    private static readonly KdPgTableDescriptor Table = Helper.GetTable<TModel>();

    // private bool _useAllColumns = true;
    private List<KdPgColumnDescriptor> _columns = Table.Columns;
    public int FieldsCount => 1 + _columns.Count;

    public object ParseResult(object[] values)
    {
      var obj = Activator.CreateInstance(Table.ModelType);

      var isNull = (bool) Helper.ConvertFromRawSqlValue(KDPgValueTypeInstances.Boolean, values[0]);
      if (isNull)
        return null;

      for (var i = 0; i < _columns.Count; i++) {
        var col = _columns[i];
        var val = Helper.ConvertFromRawSqlValue(col.Type, values[1 + i]);
        col.PropertyInfo.SetValue(obj, val);
      }

      return obj;
    }
  }
}