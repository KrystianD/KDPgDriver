using System;
using System.Collections.Generic;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders.ResultProcessors
{
  public class AnonymousTypeResultProcessor<TModel> : IResultProcessor
  {
    public class Entry
    {
      public KdPgTableDescriptor MemberPgTable;
      public KDPgValueType MemberPgType;
    }

    private readonly List<Entry> Entries = new List<Entry>();

    private int fieldsCount = 0;

    public int FieldsCount => fieldsCount;

    public object ParseResult(object[] values)
    {
      object[] constructorParams = new object[Entries.Count];

      int columnIdx = 0;

      for (var i = 0; i < Entries.Count; i++) {
        var entry = Entries[i];

        if (entry.MemberPgTable != null) {
          var table = entry.MemberPgTable;

          var isNull = (bool)PgTypesConverter.ConvertFromRawSqlValue(KDPgValueTypeInstances.Boolean, values[columnIdx++]);
          if (isNull) {
            constructorParams[i] = null;

            columnIdx += table.Columns.Count;
          }
          else {
            var modelObj = Activator.CreateInstance(table.ModelType);

            foreach (var column in table.Columns) {
              var val = PgTypesConverter.ConvertFromRawSqlValue(column.Type, values[columnIdx++]);
              column.PropertyInfo.SetValue(modelObj, val);
            }

            constructorParams[i] = modelObj;
          }
        }
        else if (entry.MemberPgType != null) {
          constructorParams[i] = PgTypesConverter.ConvertFromRawSqlValue(entry.MemberPgType, values[columnIdx++]);
        }
        else
          throw new Exception();
      }

      return Activator.CreateInstance(typeof(TModel), constructorParams);
    }

    public void AddModelEntry(KdPgTableDescriptor table)
    {
      fieldsCount += 1 + table.Columns.Count;

      Entries.Add(new Entry {
          MemberPgTable = table,
      });
    }

    public void AddMemberEntry(KDPgValueType memberType)
    {
      fieldsCount += 1;

      Entries.Add(new Entry {
          MemberPgType = memberType,
      });
    }
  }
}