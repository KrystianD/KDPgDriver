using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using KDLib;
using KDPgDriver.Fluent;
using KDPgDriver.Results;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public interface ISelectFromBuilder
  {
    RawQuery GetRawQuery(string defaultSchema = null);

    IResultProcessor GetResultProcessor();
  }

  public interface IResultProcessor
  {
    object ParseResult(object[] values);
    int FieldsCount { get; }
  }

  public class SelectFromBuilder : ISelectFromBuilder
  {
    private class ResultColumnDef
    {
      public KDPgValueType Type;
      public RawQuery RawQuery;
    }

    private readonly List<ResultColumnDef> _columns = new List<ResultColumnDef>();
    private readonly List<RawQuery.TableNamePlaceholder> _tablePlaceholders = new List<RawQuery.TableNamePlaceholder>();

    private List<TypedExpression> LeftJoinsExpressions { get; set; }
    private IResultProcessor ResultProcessor { get; set; }

    public static SelectFromBuilder FromCombinedExpression<TCombinedModel, TNewModel>(TablesList tablesList, Expression<Func<TCombinedModel, TNewModel>> prBody)
    {
      var builder = new SelectFromBuilder();
      builder.LeftJoinsExpressions = tablesList.JoinExpressions;

      var options = new NodeVisitor.EvaluationOptions();
      foreach (var table in tablesList.Tables) {
        builder.AddTable(table);
        options.ParameterToTableAlias.Add(table.Name, table);
      }

      var tableToPlaceholder = tablesList.Tables.ToDictionary(x => x.Name);

      TypedExpression exp;
      switch (prBody.Body) {
        /* For:
         * .Select(x => new {
                            M1 = x.M1,
                            M2_name = x.M2.Name1,
                            M3_calc = x.M2.Id * 2,
                        })
         */
        case NewExpression newExpression:
        {
          IEnumerable<Expression> args = newExpression.Arguments;

          var resultProcessor = new AnonymousTypeResultProcessor<TNewModel>();
          builder.ResultProcessor = resultProcessor;

          foreach (var argExpression in args) {
            // Member is Table (like M1 = x.M1)
            if (argExpression is MemberExpression memberExpression && Helper.IsTable(memberExpression.Type)) {
              var tablePlaceholder = tableToPlaceholder[memberExpression.Member.Name];
              var table = tablePlaceholder.Table;

              var tableTestRawQuery = new RawQuery();
              tableTestRawQuery.AppendTable(tablePlaceholder);
              tableTestRawQuery.Append(" IS NULL");
              builder.AddSelectPart(tableTestRawQuery, KDPgValueTypeInstances.Boolean);
              
              foreach (var column in table.Columns) {
                var rq = new RawQuery();
                rq.AppendColumn(column, tablePlaceholder);
                builder.AddSelectPart(rq, column.Type);
              }

              resultProcessor.AddModelEntry(table);
            }
            // Member is Member-expression (like M2_name = x.M2.Name1, M3_calc = x.M2.Id * 2)
            else {
              exp = NodeVisitor.EvaluateToTypedExpression(argExpression, (string) null, options);
              builder.AddSelectPart(exp.RawQuery, exp.Type);
              resultProcessor.AddMemberEntry(exp.Type);
            }
          }

          break;
        }

        /* For:
         * .Select(x => x.M1)
         * .Select(x => x.M2.Name1)
         */
        case MemberExpression memberExpression:
        {
          // .Select(x => x.M1)
          if (Helper.IsTable(typeof(TNewModel))) {
            var resultProcessor = new CombinedModelResultProcessor<TNewModel>();
            builder.ResultProcessor = resultProcessor;

            var tablePlaceholder = tableToPlaceholder[memberExpression.Member.Name];
            var table = tablePlaceholder.Table;

            var tableTestRawQuery = new RawQuery();
            tableTestRawQuery.AppendTable(tablePlaceholder);
            tableTestRawQuery.Append(" IS NULL");
            builder.AddSelectPart(tableTestRawQuery, KDPgValueTypeInstances.Boolean);
            
            foreach (var column in table.Columns) {
              var rq = new RawQuery();
              rq.AppendColumn(column, tablePlaceholder);
              builder.AddSelectPart(rq, column.Type);
              // resultProcessor.UseColumn(column);
            }
          }
          // .Select(x => x.M2.Name1)
          else {
            // Select returns single value from combined model
            exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);
            builder.AddSelectPart(exp.RawQuery, exp.Type);
            builder.ResultProcessor = new SingleValueResultProcessor(exp.Type);
          }

          break;
        }

        /* For:
         * .Select(x => 0)
         */
        default:
          // Select return constant value
          exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);
          builder.AddSelectPart(exp.RawQuery, exp.Type);
          builder.ResultProcessor = new SingleValueResultProcessor(exp.Type);
          break;
      }

      return builder;
    }

    /* For:
     * .Select()
     */
    public static SelectFromBuilder AllColumnsFromCombined<TCombinedModel>(TablesList tablesList)
    {
      var builder = new SelectFromBuilder();
      builder.LeftJoinsExpressions = tablesList.JoinExpressions;

      var resultProcessor = new AnonymousTypeResultProcessor<TCombinedModel>();

      foreach (var tablePlaceholder in tablesList.Tables) {
        builder.AddTable(tablePlaceholder);
        
        var tableTestRawQuery = new RawQuery();
        tableTestRawQuery.AppendTable(tablePlaceholder);
        tableTestRawQuery.Append(" IS NULL");
        builder.AddSelectPart(tableTestRawQuery, KDPgValueTypeInstances.Boolean);

        foreach (var column in tablePlaceholder.Table.Columns) {
          var rq = new RawQuery();
          rq.AppendColumn(column, tablePlaceholder);
          builder.AddSelectPart(rq, column.Type);
        }

        resultProcessor.AddModelEntry(tablePlaceholder.Table);
      }

      builder.ResultProcessor = resultProcessor;

      return builder;
    }

    public static SelectFromBuilder FromExpression<TModel, TNewModel>(Expression<Func<TModel, TNewModel>> prBody)
    {
      var b = new SelectFromBuilder();
      b.AddTable(Helper.GetTable<TModel>());
      TypedExpression exp;

      switch (prBody.Body) {
        case NewExpression newExpression:
        {
          IEnumerable<PropertyInfo> members = newExpression.Members.Cast<PropertyInfo>();
          IEnumerable<Expression> args = newExpression.Arguments;

          var resultProcessor = new AnonymousTypeResultProcessor<TNewModel>();
          b.ResultProcessor = resultProcessor;

          foreach (var (member, argExpression) in members.Zip(args)) {
            exp = NodeVisitor.EvaluateToTypedExpression(argExpression);
            b.AddSelectPart(exp.RawQuery, exp.Type);
            resultProcessor.AddMemberEntry(exp.Type);
          }

          break;
        }

        default:
          exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);

          b.AddSelectPart(exp.RawQuery, exp.Type);
          b.ResultProcessor = new SingleValueResultProcessor(exp.Type);

          break;
      }

      return b;
    }

    public static SelectFromBuilder FromFieldListBuilder<TModel>(FieldListBuilder<TModel> builder)
    {
      var b = new SelectFromBuilder();
      b.AddTable(Helper.GetTable<TModel>());

      var resultProcessor = new ModelResultProcessor<TModel>();

      foreach (var fieldExpression in builder.Fields) {
        var column = NodeVisitor.EvaluateExpressionToColumn(fieldExpression);
        b.AddSelectPart(column.TypedExpression.RawQuery, column.Type);
        resultProcessor.UseColumn(column);
      }

      b.ResultProcessor = resultProcessor;

      return b;
    }

    public static SelectFromBuilder AllColumns<TModel>()
    {
      var b = new SelectFromBuilder();
      b.AddTable(Helper.GetTable<TModel>());

      foreach (var column in Helper.GetTable<TModel>().Columns)
        b.AddSelectPart(column.TypedExpression.RawQuery, column.Type);

      b.ResultProcessor = new ModelResultProcessor<TModel>();

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
        exp.MarkSimple();
        rq.AppendWithCast(exp, type.PostgresFetchType == type.PostgresTypeName ? null : type.PostgresFetchType);
        firstColumn = false;
      }

      rq.Append(" FROM ");

      int tableNum = 0;
      // bool firstTable = true;
      if (_tablePlaceholders.Count > 1) {
        var firstTable = _tablePlaceholders[0].Table;
        string alias = $"t{tableNum}";
        rq.AppendTableName(firstTable.Name, firstTable.Schema ?? defaultSchema, alias);
        rq.ApplyAlias(_tablePlaceholders[0].Name, alias);
        tableNum++;

        foreach (var tablePlaceholder in _tablePlaceholders.Skip(1)) {
          var table = tablePlaceholder.Table;

          rq.Append(" LEFT JOIN ");

          alias = $"t{tableNum}";
          rq.AppendTableName(table.Name, table.Schema ?? defaultSchema, alias);
          rq.ApplyAlias(tablePlaceholder.Name, alias);

          rq.Append(" ON ");
          rq.AppendSurround(LeftJoinsExpressions[tableNum].RawQuery);

          tableNum++;
        }
      }
      else {
        rq.AppendTableName(_tablePlaceholders[0].Name, _tablePlaceholders[0].Table.Schema ?? defaultSchema);
      }

      if (_tablePlaceholders.Count == 1)
        rq.SkipExplicitColumnTableNames();
      return rq;
    }

    public IResultProcessor GetResultProcessor()
    {
      return ResultProcessor;
    }

    // helpers
    private void AddTable(RawQuery.TableNamePlaceholder table)
    {
      _tablePlaceholders.Add(table);
    }

    private void AddTable(KdPgTableDescriptor table)
    {
      _tablePlaceholders.Add(new RawQuery.TableNamePlaceholder(table, table.Name));
    }

    private void AddSelectPart(RawQuery exp, KDPgValueType type)
    {
      _columns.Add(new ResultColumnDef() {
          RawQuery = exp,
          Type = type,
      });
    }
  }
}