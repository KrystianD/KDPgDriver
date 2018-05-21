using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using KDLib;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class SelectQuery<TOut>
  {
    public readonly ParametersContainer Parameters;

    public IQueryBuilder Builder { get; }

    private List<PropertyInfo> columns = new List<PropertyInfo>();
    private StringBuilder selectPart = new StringBuilder();

    public SelectQuery(IQueryBuilder queryBuilder, ParametersContainer parameters)
    {
      Parameters = parameters;
      Builder = queryBuilder;
    }

    public IList<PropertyInfo> GetColumns()
    {
      return columns.Count == 0 ? Helper.GetModelColumns(typeof(TOut)) : columns;
    }

    public string GetQuery(Driver driver)
    {
      string selectStr = selectPart.ToString();
      if (selectStr.Length == 0) {
        selectStr = Helper.GetModelColumnNames(typeof(TOut)).JoinString(",");
      }

      string q = $"SELECT {selectStr} FROM \"{driver.Schema}\".\"{Builder.TableName}\"";
      string wherePart = Builder.GetWherePart();
      if (wherePart.Length > 0)
        q += $" WHERE {wherePart}";

      return q;
    }

    private string BuildSelectExpression(Expression e)
    {
      return NodeVisitor.Visit(e, Parameters).Expression;
    }

    private void VisitForSelectNewType(NewExpression e)
    {
      var args = e.Arguments;
      var members = e.Members;

      foreach (var (member, arg) in members.Zip(args)) {
        var exp = BuildSelectExpression(Evaluator.PartialEval(arg));

        if (selectPart.Length > 0)
          selectPart.Append(", ");
        selectPart.Append(exp);
        columns.Add((PropertyInfo) member);
      }
    }

    public void Process(Expression prBody)
    {
      switch (prBody.NodeType) {
        // case ExpressionType.Lambda:
        // break;
        case ExpressionType.New:
          VisitForSelectNewType((NewExpression) prBody);
          break;
        default:
          throw new Exception($"invalid node: {prBody.NodeType}");
      }
    }
  }
}