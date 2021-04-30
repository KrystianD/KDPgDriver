using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using KDPgDriver.Builders;
using KDPgDriver.Results;
using KDPgDriver.Traverser;
using KDPgDriver.Types;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver.Queries
{
  public interface IInsertQuery : IQuery
  {
    Task<InsertQueryResult> ReadResultAsync(NpgsqlDataReader reader);
  }

  internal enum OnInsertConflict
  {
    None = 0,
    DoNothing = 1,
    DoUpdate = 2,
  }

  public class InsertQuery<TModel> : IInsertQuery
  {
    private struct InsertColumnMeta
    {
      public KdPgColumnDescriptor columnDescriptor;
      public ISelectSubquery subquery;
    }

    private readonly KdPgTableDescriptor Table = ModelsRegistry.GetTable<TModel>();

    private static readonly List<InsertColumnMeta> AllColumnsWithoutAutoIncrement =
        ModelsRegistry.GetTable<TModel>().Columns.Where(x => (x.Flags & KDPgColumnFlagsEnum.AutoIncrement) == 0)
                      .Select(x => new InsertColumnMeta() { columnDescriptor = x })
                      .ToList();

    private static readonly KdPgTableDescriptor TableModel = ModelsRegistry.GetTable<TModel>();

    private OnInsertConflict _onInsertConflict = OnInsertConflict.None;
    private Action<UpdateStatementsBuilder<TModel>> _onInsertConflictUpdate;
    private Action<FieldListBuilder<TModel>> _onInsertConflictUpdateFields;

    private readonly List<InsertColumnMeta> _columns = new List<InsertColumnMeta>();

    private KdPgColumnDescriptor _idColumn, _idRefColumn;

    private string _outputVariable;

    private readonly List<TModel> _objects = new List<TModel>();
    public bool IsEmpty => _objects.Count == 0;

    public InsertQuery<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      _columns.Add(new InsertColumnMeta() { columnDescriptor = NodeVisitor.EvaluateExpressionToColumn(field) });
      return this;
    }

    public InsertQuery<TModel> UseField<TValue>(Expression<Func<TModel, TValue>> field, SelectSubquery<TValue> subquery)
    {
      _columns.Add(new InsertColumnMeta() { columnDescriptor = NodeVisitor.EvaluateExpressionToColumn(field), subquery = subquery });
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

    public InsertQuery<TModel> OnConflictDoUpdate(Action<FieldListBuilder<TModel>> fields, Action<UpdateStatementsBuilder<TModel>> builder)
    {
      _onInsertConflict = OnInsertConflict.DoUpdate;
      _onInsertConflictUpdate = builder;
      _onInsertConflictUpdateFields = fields;
      return this;
    }

    public RawQuery GetRawQuery()
    {
      if (IsEmpty)
        return RawQuery.Create("SELECT 0");

      RawQuery rq = new RawQuery();

      var columns = _columns.Count == 0 ? AllColumnsWithoutAutoIncrement : _columns;

      rq.Append("INSERT INTO ");

      rq.AppendTableName(Table.Name, Table.Schema);

      rq.Append("(");
      rq.AppendColumnNames(columns.Select(x => x.columnDescriptor.Name));
      if (_idColumn != null) {
        if (columns.Count > 0)
          rq.Append(",");
        rq.AppendColumnName(_idColumn.Name);
      }

      rq.Append(")");

      rq.Append(" VALUES ");

      var first = true;
      foreach (var obj in _objects) {
        if (!first)
          rq.Append(",");
        rq.Append("(");

        for (int i = 0; i < columns.Count; i++) {
          var column = columns[i];

          if (i > 0)
            rq.Append(",");

          if (column.subquery == null) {
            object val = ModelsRegistry.GetModelValueByColumn(obj, column.columnDescriptor);
            var npgValue = PgTypesConverter.ConvertToPgValue(column.columnDescriptor.Type, val);
            rq.Append(npgValue);
          }
          else {
            rq.AppendSurround(column.subquery.GetRawQuery());
          }
        }

        if (_idColumn != null) {
          if (columns.Count > 0)
            rq.Append(",");

          rq.Append(ExpressionBuilders.CurrSeqValueOfTable(_idRefColumn).RawQuery);
        }

        rq.Append(")");
        first = false;
      }

      if (_onInsertConflict == OnInsertConflict.DoNothing) {
        rq.Append(" ON CONFLICT DO NOTHING ");
      }

      if (_onInsertConflict == OnInsertConflict.DoUpdate) {
        rq.Append(" ON CONFLICT (");

        var fields = new FieldListBuilder<TModel>();
        _onInsertConflictUpdateFields(fields);
        first = true;
        foreach (var fieldExpression in fields.Fields) {
          if (!first)
            rq.Append(", ");

          var column = NodeVisitor.EvaluateExpressionToColumn(fieldExpression);
          rq.AppendColumnName(column.Name);

          first = false;
        }

        rq.Append(") DO UPDATE SET ");

        var updateStatementsBuilder = new UpdateStatementsBuilder<TModel>();
        _onInsertConflictUpdate(updateStatementsBuilder);

        first = true;
        foreach (var (column, typedExpression) in updateStatementsBuilder.UpdateParts) {
          if (!first)
            rq.Append(", ");

          rq.AppendColumnName(column.Name)
            .Append(" = ")
            .Append(typedExpression.RawQuery);

          first = false;
        }
      }

      if (TableModel.PrimaryKey != null) {
        rq.Append(" RETURNING ");
        rq.AppendColumnName(TableModel.PrimaryKey.Name);
      }

      rq.Append(";");

      if (_outputVariable != null) {
        rq.Append(" SELECT ");
        rq.Append(ExpressionBuilders.SetConfigText(_outputVariable, ExpressionBuilders.LastVal(), true).RawQuery);
      }

      rq.SkipExplicitColumnTableNames();
      return rq;
    }

    public async Task<InsertQueryResult> ReadResultAsync(NpgsqlDataReader reader)
    {
      InsertQueryResult res;

      if (await reader.ReadAsync())
        res = new InsertQueryResult(true, lastInsertId: reader.GetInt32(0));
      else
        res = new InsertQueryResult(false, lastInsertId: null);

      await reader.NextResultAsync();

      return res;
    }
  }
}