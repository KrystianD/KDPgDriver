using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using KDLib;
using KDPgDriver.Results;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public interface ISelectFromBuilder
  {
    bool IsSingleValue { get; }
    List<ResultColumnDef> GetColumns();
    RawQuery GetRawQuery(string defaultSchema = null);
  }

  public class SelectFromBuilder : ISelectFromBuilder
  {
    private readonly List<ResultColumnDef> _columns = new List<ResultColumnDef>();
    private readonly RawQuery _selectPart = new RawQuery();

    private List<KdPgTableDescriptor> _tables = new List<KdPgTableDescriptor>();

    public bool IsSingleValue { get; private set; }
    public List<ResultColumnDef> GetColumns() => _columns;

    public static SelectFromBuilder FromExpression<TModel, TNewModel>(Expression<Func<TModel, TNewModel>> prBody)
    {
      var b = new SelectFromBuilder();
      b.AddTable(Helper.GetTable<TModel>());
      TypedExpression exp;

      switch (prBody.Body) {
        case NewExpression newExpression:
        {
          var members = newExpression.Members.Cast<PropertyInfo>();
          var args = newExpression.Arguments;

          foreach (var (member, argExpression) in members.Zip(args)) {
            exp = NodeVisitor.EvaluateToTypedExpression(argExpression);
            b.AddSelectPart(exp.RawQuery, member, exp.Type);
          }

          break;
        }

        default:
          exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);

          b.AddSelectPart(exp.RawQuery, null, exp.Type);
          b.IsSingleValue = true;
          break;
      }

      return b;
    }

    public static SelectFromBuilder FromFieldListBuilder<TModel>(FieldListBuilder<TModel> builder)
    {
      var b = new SelectFromBuilder();
      b.AddTable(Helper.GetTable<TModel>());

      foreach (var fieldExpression in builder.Fields) {
        var column = NodeVisitor.EvaluateExpressionToColumn(fieldExpression);
        b.AddSelectPart(RawQuery.CreateColumnName(column.Name), column.PropertyInfo, column.Type);
      }

      return b;
    }

    public static SelectFromBuilder AllColumns<TModel>()
    {
      var b = new SelectFromBuilder();
      b.AddTable(Helper.GetTable<TModel>());

      foreach (var column in Helper.GetTable(typeof(TModel)).Columns)
        b.AddSelectPart(RawQuery.CreateColumnName(column.Name), column.PropertyInfo, column.Type);
      return b;
    }

    public RawQuery GetRawQuery(string defaultSchema)
    {
      RawQuery rq = new RawQuery();
      rq.Append("SELECT ");

      bool firstColumn = true;
      foreach (var col in _columns) {
        var exp = col.RawQuery;
        var type = col.Type;

        if (!firstColumn)
          rq.Append(",");
        rq.AppendWithCast(exp.RenderSimple(), type.PostgresFetchType == type.PostgresType ? null : type.PostgresFetchType);
        firstColumn = false;
      }

      rq.Append(" FROM ");

      bool firstTable = true;
      foreach (var table in _tables) {
        if (!firstTable)
          rq.Append(",");
        rq.AppendTableName(table.Name, table.Schema ?? defaultSchema);
        firstTable = false;
      }

      return rq;
    }

    // helpers
    private void AddTable(KdPgTableDescriptor table)
    {
      _tables.Add(table);
    }

    private void AddSelectPart(RawQuery exp, PropertyInfo member, KDPgValueType type)
    {
      _columns.Add(new ResultColumnDef() {
          RawQuery = exp,
          // SchemaName = schema,
          // TableName = tableName,
          EndModelProperty = member,
          Type = type,
      });
    }
  }
}