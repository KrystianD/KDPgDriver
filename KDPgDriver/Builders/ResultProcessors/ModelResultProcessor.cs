﻿using System;
using System.Collections.Generic;
using KDPgDriver.Types;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders.ResultProcessors
{
  public class ModelSelectResultProcessor<TModel> : ISelectResultProcessor
  {
    private static readonly KdPgTableDescriptor Table = ModelsRegistry.GetTable<TModel>();

    private bool _useAllColumns = true;
    private List<KdPgColumnDescriptor> _columns = Table.Columns;
    public int FieldsCount => _columns.Count;

    public object ParseResult(object[] values)
    {
      var obj = Activator.CreateInstance(Table.ModelType);

      for (var i = 0; i < _columns.Count; i++) {
        var col = _columns[i];
        var val = PgTypesConverter.ConvertFromRawSqlValue(col.Type, values[i]);
        col.PropertyInfo.SetValue(obj, val);
      }

      return obj;
    }

    public void UseColumn(KdPgColumnDescriptor column)
    {
      if (_useAllColumns) {
        _columns = new List<KdPgColumnDescriptor>();
        _useAllColumns = false;
      }

      _columns.Add(column);
    }
  }
}