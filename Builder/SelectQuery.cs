using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using KDLib;

namespace KDPgDriver.Builder
{
  public class SelectQuery<TOut>
  {
    public readonly ParametersContainer Parameters;
    
    public IBaseQueryBuilder Builder { get; }

    private StringBuilder selectPart = new StringBuilder();

    public SelectQuery(IBaseQueryBuilder baseQueryBuilder, ParametersContainer parameters)
    {
      Parameters = parameters;
      Builder = baseQueryBuilder;
    }

    public string GetQuery()
    {
      string selectStr = selectPart.ToString();
      if (selectStr.Length == 0) {
        selectStr = Helper.GetModelColumnNames(typeof(TOut)).JoinString(",");
      }

      string q = $"SELECT {selectStr} FROM \"{Builder.Driver.Schema}\".\"{Builder.TableName}\"";
      string wherePart = Builder.GetWherePart();
      if (wherePart.Length > 0)
        q += $" WHERE {wherePart}";

      return q;
    }

    private string BuildSelectExpression(Expression e)
    {
      return NodeVisitor.Visit2(e, Parameters).Expression;
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