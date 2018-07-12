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
    RawQuery GetRawQuery();
  }

  public class SelectFromBuilder : ISelectFromBuilder
  {
    private readonly List<ResultColumnDef> _columns = new List<ResultColumnDef>();
    private readonly RawQuery _selectPart = new RawQuery();

    public bool IsSingleValue { get; private set; }
    public List<ResultColumnDef> GetColumns() => _columns;
    public RawQuery GetRawQuery() => _selectPart;

    public static SelectFromBuilder FromExpression<TModel, TNewModel>(Expression<Func<TModel, TNewModel>> prBody)
    {
      var b = new SelectFromBuilder();
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
      foreach (var fieldExpression in builder.Fields) {
        var column = NodeVisitor.EvaluateExpressionToColumn(fieldExpression);
        b.AddSelectPart(RawQuery.CreateColumnName(column.Name), column.PropertyInfo, column.Type);
      }

      return b;
    }

    public static SelectFromBuilder AllColumns<TModel>()
    {
      var b = new SelectFromBuilder();
      foreach (var column in Helper.GetTable(typeof(TModel)).Columns)
        b.AddSelectPart(RawQuery.CreateColumnName(column.Name), column.PropertyInfo, column.Type);
      return b;
    }

    // helpers
    private void AddSelectPart(RawQuery exp, PropertyInfo member, KDPgValueType type)
    {
      if (!_selectPart.IsEmpty)
        _selectPart.Append(",");
      _selectPart.AppendWithCast(exp.RenderSimple(), type.PostgresFetchType == type.PostgresType ? null : type.PostgresFetchType);

      _columns.Add(new ResultColumnDef() {
          EndModelProperty = member,
          Type = type,
      });
    }
  }
}