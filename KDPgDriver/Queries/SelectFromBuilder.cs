using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using KDLib;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class SelectFromBuilder<TOut>
  {
    private readonly List<ResultColumnDef> _columns = new List<ResultColumnDef>();
    private readonly RawQuery _selectPart = new RawQuery();
    
    public bool IsSingleValue { get; private set; }

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

    public List<ResultColumnDef> GetColumns()
    {
      return _columns;
    }
    
    public RawQuery GetRawQuery()
    {
      return _selectPart;
    }
    
    public static SelectFromBuilder<TOut> FromExpression<TModel>(Expression<Func<TModel, TOut>> prBody)
    {
      var b = new SelectFromBuilder<TOut>();
      TypedExpression exp;

      switch (prBody.Body) {
        case NewExpression newExpression:
        {
          var members = newExpression.Members.Cast<PropertyInfo>();
          var args = newExpression.Arguments;

          foreach (var (member, argExpression) in members.Zip(args)) {
            exp = NodeVisitor.Visit(argExpression);
            b.AddSelectPart(exp.RawQuery, member, exp.Type);
          }

          break;
        }

        default:
          exp = NodeVisitor.Visit(prBody.Body);

          b.AddSelectPart(exp.RawQuery, null, exp.Type);
          b.IsSingleValue = true;

          break;
      }

      return b;
    }

    public static SelectFromBuilder<TOut> FromFieldListBuilder(FieldListBuilder<TOut> builder)
    {
      var b = new SelectFromBuilder<TOut>();
      foreach (var fieldExpression in builder.Fields) {
        var member = NodeVisitor.EvaluateToPropertyInfo(fieldExpression);
        var column = Helper.GetColumn(member);

        b.AddSelectPart(RawQuery.CreateColumnName(column.Name), member, column.Type);
      }
      return b;
    }

    public static SelectFromBuilder<TOut> AllColumns()
    {
      var b = new SelectFromBuilder<TOut>();
      foreach (var column in Helper.GetTable(typeof(TOut)).Columns)
        b.AddSelectPart(RawQuery.CreateColumnName(column.Name), column.PropertyInfo, column.Type);
      return b;
    }

  }
}