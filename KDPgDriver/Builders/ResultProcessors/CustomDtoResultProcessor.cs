using System;
using System.Collections.Generic;
using System.Reflection;
using KDPgDriver.Types;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders.ResultProcessors
{
  public class CustomDtoResultProcessor<TModel> : ISelectResultProcessor
  {
    public class Entry
    {
      public PropertyInfo PropertyInfo;
      public KdPgTableDescriptor MemberPgTable;
      public KDPgValueType MemberPgType;
      public bool WithNullIndicator;
    }

    private readonly List<Entry> Entries = new List<Entry>();

    private int fieldsCount = 0;

    public int FieldsCount => fieldsCount;

    public object ParseResult(object[] values)
    {
      int columnIdx = 0;

      var instance = Activator.CreateInstance(typeof(TModel));

      for (var i = 0; i < Entries.Count; i++) {
        var entry = Entries[i];

        if (entry.MemberPgTable != null) {
          var table = entry.MemberPgTable;

          var isNull = entry.WithNullIndicator && (bool)PgTypesConverter.ConvertFromRawSqlValue(KDPgValueTypeInstances.Boolean, values[columnIdx++]);
          if (isNull) {
            columnIdx += table.Columns.Count;

            entry.PropertyInfo.SetValue(instance, null);
          }
          else {
            var modelObj = Activator.CreateInstance(table.ModelType);

            foreach (var column in table.Columns) {
              var val = PgTypesConverter.ConvertFromRawSqlValue(column.Type, values[columnIdx++]);
              column.PropertyInfo.SetValue(modelObj, val);
            }

            entry.PropertyInfo.SetValue(instance, modelObj);
          }
        }
        else if (entry.MemberPgType != null) {
          entry.PropertyInfo.SetValue(instance, PgTypesConverter.ConvertFromRawSqlValue(entry.MemberPgType, values[columnIdx++]));
        }
        else
          throw new Exception();
      }

      return instance;
    }

    public void AddModelEntry(PropertyInfo propertyInfo, KdPgTableDescriptor table, bool withNullIndicator)
    {
      if (withNullIndicator)
        fieldsCount += 1 + table.Columns.Count;
      else
        fieldsCount += table.Columns.Count;

      Entries.Add(new Entry {
          PropertyInfo = propertyInfo,
          MemberPgTable = table,
          WithNullIndicator = withNullIndicator,
      });
    }

    public void AddMemberEntry(PropertyInfo propertyInfo, KDPgValueType memberType)
    {
      fieldsCount += 1;

      Entries.Add(new Entry {
          PropertyInfo = propertyInfo,
          MemberPgType = memberType,
      });
    }
  }
}