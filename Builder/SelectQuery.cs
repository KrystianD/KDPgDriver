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
  public class SelectQuery<TOut>
  {
    public readonly ParametersContainer Parameters;

    public IQueryBuilder Builder { get; }

    private List<ResultColumnDef> columns = new List<ResultColumnDef>();
    private StringBuilder selectPart = new StringBuilder();
    public bool isSingleValue = false;

    public SelectQuery(IQueryBuilder queryBuilder, ParametersContainer parameters)
    {
      Parameters = parameters;
      Builder = queryBuilder;
    }

    public IList<ResultColumnDef> GetColumns()
    {
      return columns.Count == 0 ? Helper.GetModelColumns(typeof(TOut)).Select(x=>new ResultColumnDef()
      {
          PropertyInfo = x,
          KdPgColumnType = Helper.GetColumnDataType(x),
      }) .ToList(): columns;
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

    private void VisitForSelectNewType(NewExpression e)
    {
      var args = e.Arguments;
      var members = e.Members;

      foreach (var (member, arg) in members.Zip(args)) {
        var exp = NodeVisitor.Visit(Evaluator.PartialEval(arg), Parameters);

        if (selectPart.Length > 0)
          selectPart.Append(", ");
        selectPart.Append(exp.Expression);
        
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
          var member = ((MemberExpression) prBody).Member;
          string columnName = Helper.GetColumnName(member);
          
          if (selectPart.Length > 0)
            selectPart.Append(", ");
          selectPart.Append(columnName);
          
          columns.Add(new ResultColumnDef()
          {
              PropertyInfo = (PropertyInfo) member,
              KdPgColumnType =Helper.GetColumnDataType((PropertyInfo) member),
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