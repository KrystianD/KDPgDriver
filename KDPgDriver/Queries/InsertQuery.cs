using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface IInsertQuery : IQuery
  {
  }

  internal enum OnInsertConflict
  {
    None = 0,
    DoNothing = 1,
    DoUpdate
  }

  public class InsertQuery<TModel> : IInsertQuery
  {
    private readonly KdPgTableDescriptor Table = Helper.GetTable<TModel>();

    private static readonly List<KdPgColumnDescriptor> AllColumnsWithoutAutoIncrement =
        Helper.GetTable<TModel>().Columns.Where(x => (x.Flags & KDPgColumnFlagsEnum.AutoIncrement) == 0).ToList();

    private static readonly KdPgTableDescriptor TableModel = Helper.GetTable<TModel>();

    private OnInsertConflict _onInsertConflict = OnInsertConflict.None;
    private Action<UpdateStatementsBuilder<TModel>> _onInsertConflictUpdate;
    private readonly List<KdPgColumnDescriptor> _columns = new List<KdPgColumnDescriptor>();

    private KdPgColumnDescriptor _idColumn, _idRefColumn;

    private string _outputVariable;

    private readonly List<TModel> _objects = new List<TModel>();
    public bool IsEmpty => _objects.Count == 0;

    public InsertQuery<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      _columns.Add(NodeVisitor.EvaluateExpressionToColumn(field));
      return this;
    }

    public InsertQuery<TModel> UsePreviousInsertId<TRefModel>(Expression<Func<TModel, object>> field, Expression<Func<TRefModel, int>> idField)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field);
      var refColumn = NodeVisitor.EvaluateExpressionToColumn(idField);

      if ((refColumn.Flags & KDPgColumnFlagsEnum.AutoIncrement) == 0)
        throw new ArgumentException("Reference field must be auto increment");

      _idColumn = column;
      _idRefColumn = refColumn;

      return this;
    }

    public InsertQuery<TModel> IntoVariable(string name)
    {
      if (!Regex.IsMatch(name, "^[a-zA-Z0-9_-]+$"))
        throw new Exception("invalid variable name, allowed [a-zA-Z0-9_-]");
      _outputVariable = name;
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

    public InsertQuery<TModel> OnConflictDoNothing()
    {
      _onInsertConflict = OnInsertConflict.DoNothing;
      return this;
    }

    public InsertQuery<TModel> OnConflictDoUpdate(Action<UpdateStatementsBuilder<TModel>> builder)
    {
      _onInsertConflict = OnInsertConflict.DoUpdate;
      _onInsertConflictUpdate = builder;
      return this;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      if (IsEmpty)
        return RawQuery.Create("SELECT 0");

      RawQuery rq = new RawQuery();

      var columns = _columns.Count == 0 ? AllColumnsWithoutAutoIncrement : _columns;

      rq.Append("INSERT INTO ");

      rq.AppendTableName(Table.Name, Table.Schema ?? defaultSchema);

      rq.Append("(");
      rq.AppendColumnNames(columns.Select(x => x.Name));
      if (_idColumn != null) {
        if (columns.Count > 0)
          rq.Append(",");
        rq.AppendColumnName(_idColumn.Name);
      }

      rq.Append(")");

      rq.Append(" VALUES ");

      bool first = true;
      foreach (var obj in _objects) {
        if (!first)
          rq.Append(",");
        rq.Append("(");

        for (int i = 0; i < columns.Count; i++) {
          var column = columns[i];
          object val = Helper.GetModelValueByColumn(obj, column);
          var npgValue = Helper.ConvertToPgValue(column.Type, val);

          if (i > 0)
            rq.Append(",");

          rq.Append(npgValue);
        }

        if (_idColumn != null) {
          if (columns.Count > 0)
            rq.Append(",");

          rq.Append(ExpressionBuilders.CurrSeqValueOfTable(_idRefColumn, defaultSchema).RawQuery);
        }

        rq.Append(")");
        first = false;
      }

      if (_onInsertConflict == OnInsertConflict.DoNothing) {
        rq.Append(" ON CONFLICT DO NOTHING ");
      }

      if (_onInsertConflict == OnInsertConflict.DoUpdate) {
        rq.Append(" ON CONFLICT DO UPDATE SET ");

        var updateStatementsBuilder = new UpdateStatementsBuilder<TModel>();
        _onInsertConflictUpdate(updateStatementsBuilder);

        bool first2 = true;
        foreach (var (column, typedExpression) in updateStatementsBuilder.UpdateParts) {
          if (!first2)
            rq.Append(", ");

          rq.AppendColumnName(column.Name)
            .Append(" = ")
            .Append(typedExpression.RawQuery);

          first2 = false;
        }
      }

      if (TableModel.PrimaryKey != null) {
        rq.Append(" RETURNING ");
        rq.AppendColumnName(TableModel.PrimaryKey.Name);
      }

      rq.Append(";");

      if (_outputVariable != null) {
        rq.Append($" SELECT ");
        rq.Append(ExpressionBuilders.SetConfigText(_outputVariable, ExpressionBuilders.LastVal(), true).RawQuery);
      }

      rq.SkipExplicitColumnTableNames();
      return rq;
    }
  }
}