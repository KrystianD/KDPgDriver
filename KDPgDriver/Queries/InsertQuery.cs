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

    private static readonly KdPgTableDescriptor TableModel = Helper.GetTable(typeof(TModel));

    private OnInsertConflict _onInsertConflict = OnInsertConflict.None;
    private readonly List<KdPgColumnDescriptor> _columns = new List<KdPgColumnDescriptor>();

    private readonly List<TModel> _objects = new List<TModel>();

    private readonly RawQuery _insertPartQuery = new RawQuery();

    public InsertQuery<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      _columns.Add(NodeVisitor.EvaluateExpressionToColumn(field));
      return this;
    }

    public InsertQuery<TModel> AddObject(TModel obj)
    {
      if (_columns.Count == 0) {
        _columns.AddRange(Helper.GetTable(typeof(TModel)).Columns
                                .Where(x => (x.Flags & KDPgColumnFlagsEnum.AutoIncrement) == 0));
      }

      _objects.Add(obj);

      if (!_insertPartQuery.IsEmpty)
        _insertPartQuery.Append(",");
      _insertPartQuery.Append("(");

      for (int i = 0; i < _columns.Count; i++) {
        var column = _columns[i];
        object val = Helper.GetModelValueByColumn(obj, column);
        var npgValue = Helper.ConvertToNpgsql(column.Type, val);

        if (i > 0)
          _insertPartQuery.Append(",");

        _insertPartQuery.Append(npgValue);
      }

      _insertPartQuery.Append(")");

      return this;
    }

    public InsertQuery<TModel> AddMany(IEnumerable<TModel> objs)
    {
      foreach (var obj in objs)
        AddObject(obj);
      return this;
    }

    public InsertQuery<TModel> OnConflict(OnInsertConflict action)
    {
      _onInsertConflict = action;
      return this;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      RawQuery q = new RawQuery();

      q.Append("INSERT INTO ");

      q.AppendTableName(TableName, SchemaName ?? defaultSchema);

      q.Append("(").AppendColumnNames(_columns.Select(x => x.Name)).Append(")");

      q.Append(" VALUES ");
      q.Append(_insertPartQuery);

      if (_onInsertConflict == OnInsertConflict.DoNothing) {
        q.Append(" ON CONFLICT DO NOTHING ");
      }

      if (TableModel.PrimaryKey != null) {
        q.Append(" RETURNING ");
        q.AppendColumnName(TableModel.PrimaryKey.Name);
      }

      return q;
    }
  }
}