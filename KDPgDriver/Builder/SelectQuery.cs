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

    private void VisitForSelectNewType(NewExpression e)
    {
      var args = e.Arguments;
      var members = e.Members;

      foreach (var (member, arg) in members.Zip(args)) {
        var exp = NodeVisitor.Visit(Evaluator.PartialEval(arg));

        if (selectPart.Length > 0)
          selectPart.Append(", ");
        selectPart.Append(exp.RawQuery.RenderSimple());

        columns.Add(new ResultColumnDef()
        {
            PropertyInfo = (PropertyInfo) member,
            KdPgColumnType = exp.Type,
        });
      }
    }

    public void Process(Expression prBody)
    {
      switch (prBody.NodeType) {
        case ExpressionType.MemberAccess:
          var member = (PropertyInfo) ((MemberExpression) prBody).Member;
          string columnName = Helper.GetColumn(member).Name;

          if (selectPart.Length > 0)
            selectPart.Append(", ");
          selectPart.Append(columnName);

          columns.Add(new ResultColumnDef()
          {
              PropertyInfo = member,
              KdPgColumnType = Helper.GetColumnDataType(member).Type,
          });

          isSingleValue = true;

          break;
        case ExpressionType.New:
          VisitForSelectNewType((NewExpression) prBody);
          break;
        default:
          throw new Exception($"invalid node: {prBody.NodeType}");
      }
    }
  }
}