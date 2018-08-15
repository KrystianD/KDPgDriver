using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface IInsertQuery : IQuery { }

  public enum OnInsertConflict
  {
    None = 0,
    DoNothing = 1,
  }

  public class InsertQuery<TModel> : IInsertQuery
  {
    private readonly string TableName = Helper.GetTableName(typeof(TModel));
    private readonly string SchemaName = Helper.GetTableSchema(typeof(TModel));

    private static readonly List<KdPgColumnDescriptor> AllColumnsWithoutAutoIncrement =
        Helper.GetTable(typeof(TModel)).Columns.Where(x => (x.Flags & KDPgColumnFlagsEnum.AutoIncrement) == 0).ToList();

    private static readonly KdPgTableDescriptor TableModel = Helper.GetTable(typeof(TModel));

    private OnInsertConflict _onInsertConflict = OnInsertConflict.None;
    private readonly List<KdPgColumnDescriptor> _columns = new List<KdPgColumnDescriptor>();

    private readonly List<TModel> _objects = new List<TModel>();


    public InsertQuery<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      _columns.Add(NodeVisitor.EvaluateExpressionToColumn(field));
      return this;
    }

    public InsertQuery<TModel> AddObject(TModel obj)
    {
      _objects.Add(obj);
      return this;
    }

    public InsertQuery<TModel> AddMany(IEnumerable<TModel> objs)
    {
      _objects.AddRange(objs);
      return this;
    }

    public InsertQuery<TModel> OnConflict(OnInsertConflict action)
    {
      _onInsertConflict = action;
      return this;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      RawQuery rq = new RawQuery();

      var columns = _columns.Count == 0 ? AllColumnsWithoutAutoIncrement : _columns;

      rq.Append("INSERT INTO ");

      rq.AppendTableName(TableName, SchemaName ?? defaultSchema);

      rq.Append("(").AppendColumnNames(columns.Select(x => x.Name)).Append(")");

      rq.Append(" VALUES ");

      bool first = true;
      foreach (var obj in _objects) {
        if (!first)
          rq.Append(",");
        rq.Append("(");

        for (int i = 0; i < columns.Count; i++) {
          var column = columns[i];
          object val = Helper.GetModelValueByColumn(obj, column);
          var npgValue = Helper.ConvertToNpgsql(column.Type, val);

          if (i > 0)
            rq.Append(",");

          rq.Append(npgValue);
        }

        rq.Append(")");
        first = false;
      }

      if (_onInsertConflict == OnInsertConflict.DoNothing) {
        rq.Append(" ON CONFLICT DO NOTHING ");
      }

      if (TableModel.PrimaryKey != null) {
        rq.Append(" RETURNING ");
        rq.AppendColumnName(TableModel.PrimaryKey.Name);
      }

      rq.SkipExplicitColumnTableNames();
      return rq;
    }
  }
}