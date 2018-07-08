using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using KDLib;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public interface ISelectQuery
  {
    RawQuery GetQuery(Driver driver);
  }

  public class SelectQuery<TOut> : ISelectQuery
  {
    public IQueryBuilder Builder { get; }

    private List<ResultColumnDef> columns = new List<ResultColumnDef>();
    private StringBuilder selectPart = new StringBuilder();
    public bool isSingleValue = false;

    public SelectQuery(IQueryBuilder queryBuilder)
    {
      Builder = queryBuilder;
    }

    public IList<ResultColumnDef> GetColumns()
    {
      return columns.Count == 0
          ? Helper.GetTable(typeof(TOut)).Columns.Select(x => new ResultColumnDef()
          {
              PropertyInfo = x.PropertyInfo,
              KdPgColumnType = x.Type,
          }).ToList()
          : columns;
    }

    public RawQuery GetQuery(Driver driver)
    {
      string selectStr = selectPart.ToString();
      if (selectStr.Length == 0) {
        selectStr = Helper.GetTable(typeof(TOut)).Columns.Select(x => Helper.Quote(x.Name)).JoinString(",");
      }

      string schema = Builder.SchemaName ?? driver.Schema;

      RawQuery rq = new RawQuery();
      rq.Append("SELECT ", selectStr, " FROM ", Helper.QuoteTable(Builder.TableName, schema));

      RawQuery wherePart = Builder.GetWherePart();
      if (!wherePart.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(wherePart);
      }

      return rq;
    }

    private void AddSelectPart(RawQuery exp, PropertyInfo member, KDPgValueType type)
    {
      if (selectPart.Length > 0)
        selectPart.Append(", ");
      selectPart.Append(exp.RenderSimple());

      columns.Add(new ResultColumnDef()
      {
          PropertyInfo = member,
          KdPgColumnType = type,
      });
    }

    public void ProcessSingleField<TModel>(Expression<Func<TModel, TOut>> prBody)
    {
      TypedExpression exp;

      switch (prBody.Body) {
        case NewExpression newExpression:
        {
          var members = newExpression.Members.Cast<PropertyInfo>();
          var args = newExpression.Arguments;

          foreach (var (member, argExpression) in members.Zip(args)) {
            exp = NodeVisitor.Visit(argExpression);
            AddSelectPart(exp.RawQuery, member, exp.Type);
          }

          break;
        }

        default:
          exp = NodeVisitor.Visit(prBody.Body);

          AddSelectPart(exp.RawQuery, null, exp.Type);
          isSingleValue = true;

          break;
      }
    }

    public void ProcessListOfFields<TModel>(FieldListBuilder<TModel> builder)
    {
      foreach (var fieldExpression in builder.Fields) {
        var member = NodeVisitor.EvaluateToPropertyInfo(fieldExpression);
        var column = Helper.GetColumn(member);

        AddSelectPart(RawQuery.CreateColumnName(column.Name), member, column.Type);
      }
    }
  }
}