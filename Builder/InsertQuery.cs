﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using KDLib;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class InsertQueryInit<TModel> { }


  public class InsertQuery<TModel>
  {
    private List<KdPgColumnDescriptor> columns = new List<KdPgColumnDescriptor>();
    private List<string> extractors = new List<string>();

    private List<TModel> objects = new List<TModel>();

    private StringBuilder insertStr = new StringBuilder();

    public ParametersContainer Parameters { get; } = new ParametersContainer();

    private KdPgTableDescriptor TableModel = Helper.GetTable(typeof(TModel));

    private bool preparation = true;

    public InsertQuery() { }

    public void UseField(Expression<Func<TModel, object>> field)
    {
      PropertyInfo column = NodeVisitor.GetPropertyInfo(field);
      columns.Add(Helper.GetColumn(column));
    }

    // public UpdateQuery<TModel> Insert(TModel obj)
    // {
    //   return new InsertQuery<TModel>(this, Parameters, obj);
    // }
    //
    // public UpdateQuery<TModel> Insert(Action<InsertStatementsBuilder<TModel>> fn)
    // {
    //   var uq = new UpdateQuery<TModel>(this, Parameters);
    //   var us = new InsertStatementsBuilder<TModel>(uq);
    //   fn(us);
    //   return uq;
    // }

    public void Add(TModel obj)
    {
      preparation = false;

      if (columns.Count == 0) {
        columns.AddRange(Helper.GetTable(typeof(TModel)).Columns
                               .Where(x => (x.Flags & KDPgColumnFlagsEnum.PrimaryKey) == 0));
      }

      objects.Add(obj);

      if (insertStr.Length > 0)
        insertStr.Append(",");
      insertStr.Append("(");

      for (int i = 0; i < columns.Count; i++) {
        var column = columns[i];
        object val = Helper.GetModelValueByColumn(obj, column);

        var npgValue = Helper.ConvertToNpgsql(column, val);

        if (i > 0)
          insertStr.Append(",");

        insertStr.Append(val == null ? "NULL" : Parameters.GetNextParam(npgValue));
      }

      insertStr.Append(")");
    }

    public string GetQuery(Driver driver)
    {
      var columnsStr = columns.Select(x => x.Name).JoinString(",");
      
      string schema = Helper.GetTableSchema(typeof(TModel)) ?? driver.Schema;

      string tableName = Helper.GetTableName(typeof(TModel));
      string q = $"INSERT INTO \"{schema}\".\"{tableName}\"({columnsStr}) VALUES {insertStr}";

      if (TableModel.PrimaryKey != null) {
        q += $" RETURNING \"{TableModel.PrimaryKey.Name}\"";
      }

      return q;
    }
  }
}