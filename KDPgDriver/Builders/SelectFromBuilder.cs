using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using KDLib;
using KDPgDriver.Builders.ResultProcessors;
using KDPgDriver.Fluent;
using KDPgDriver.Traverser;
using KDPgDriver.Types;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public interface ISelectFromBuilder
  {
    RawQuery GetRawQuery();

    ISelectResultProcessor GetResultProcessor();
  }

  public interface ISelectResultProcessor
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
    private bool _distinct;

    private List<TypedExpression> LeftJoinsExpressions { get; set; }
    private ISelectResultProcessor SelectResultProcessor { get; set; }

    public static SelectFromBuilder FromCombinedExpression<TCombinedModel, TNewModel>(TablesList tablesList, Expression<Func<TCombinedModel, TNewModel>> prBody)
    {
      var builder = new SelectFromBuilder();
      builder.LeftJoinsExpressions = tablesList.JoinExpressions;

      var options = new EvaluationOptions();
      foreach (var tablePlaceholder in tablesList.Tables) {
        builder.AddTable(tablePlaceholder);
        // options.ParameterToTableAlias.Add(tablePlaceholder.Name, tablePlaceholder);
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

          var resultProcessor = new AnonymousTypeSelectResultProcessor<TNewModel>();
          builder.SelectResultProcessor = resultProcessor;

          foreach (var argExpression in args) {
            // Member is Table (like M1 = x.M1)
            if (argExpression is MemberExpression memberExpression && ModelsRegistry.IsTable(memberExpression.Type)) {
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
              exp = NodeVisitor.EvaluateToTypedExpression(argExpression, (string)null, options);
              builder.AddSelectPart(exp.RawQuery, exp.Type);
              resultProcessor.AddMemberEntry(exp.Type);
            }
          }

          break;
        }

        /* For:
         * .Select(x => new CustomDto {
             Id = x.M1.Id * 2,
             M1 = x.M1,
           })
         */
        case MemberInitExpression memberInitExpression:
        {
          var resultProcessor = new CustomDtoResultProcessor<TNewModel>();
          builder.SelectResultProcessor = resultProcessor;

          foreach (var entry in memberInitExpression.Bindings) {
            var memberAssignment = (MemberAssignment)entry;
            var argExpression = memberAssignment.Expression;
            var propertyInfo = (PropertyInfo)memberAssignment.Member;

            // Member is Table (like M1 = x)
            if (argExpression is MemberExpression memberExpression && ModelsRegistry.IsTable(memberExpression.Type)) {
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

              resultProcessor.AddModelEntry(propertyInfo, table, withNullIndicator: true);
            }
            // Member is Member-expression (like Id = x.Id * 2)
            else {
              exp = NodeVisitor.EvaluateToTypedExpression(argExpression, (string)null, options);
              builder.AddSelectPart(exp.RawQuery, exp.Type);
              resultProcessor.AddMemberEntry(propertyInfo, exp.Type);
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
          if (ModelsRegistry.IsTable(typeof(TNewModel))) {
            var resultProcessor = new CombinedModelSelectResultProcessor<TNewModel>();
            builder.SelectResultProcessor = resultProcessor;

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
            builder.SelectResultProcessor = new SingleValueSelectResultProcessor(exp.Type);
          }

          break;
        }

        /* For:
         * .Select(x => x.Val1)
         */
        default:
          // Select return constant value
          exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);
          builder.AddSelectPart(exp.RawQuery, exp.Type);
          builder.SelectResultProcessor = new SingleValueSelectResultProcessor(exp.Type);
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

      var resultProcessor = new AnonymousTypeSelectResultProcessor<TCombinedModel>();

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

      builder.SelectResultProcessor = resultProcessor;

      return builder;
    }

    // For simple selects
    public static SelectFromBuilder FromExpression<TModel, TNewModel>(Expression<Func<TModel, TNewModel>> prBody)
    {
      var builder = new SelectFromBuilder();
      builder.AddTable(ModelsRegistry.GetTable<TModel>());
      TypedExpression exp;

      switch (prBody.Body) {
        /* For:
         * .Select(x => new {
         *   Val1 = x.Val1
         * })
         */
        case NewExpression newExpression:
        {
          var resultProcessor = new AnonymousTypeSelectResultProcessor<TNewModel>();
          builder.SelectResultProcessor = resultProcessor;

          foreach (var arg in newExpression.Arguments) {
            exp = NodeVisitor.EvaluateToTypedExpression(arg);
            builder.AddSelectPart(exp.RawQuery, exp.Type);
            resultProcessor.AddMemberEntry(exp.Type);
          }

          break;
        }

        /* For:
         * .Select(x => new CustomDto {
             Id = x.Id * 2,
             M1 = x,
           })
         */
        case MemberInitExpression memberInitExpression:
        {
          var resultProcessor = new CustomDtoResultProcessor<TNewModel>();
          builder.SelectResultProcessor = resultProcessor;

          foreach (var entry in memberInitExpression.Bindings) {
            var memberAssignment = (MemberAssignment)entry;
            var argExpression = memberAssignment.Expression;
            var propertyInfo = (PropertyInfo)memberAssignment.Member;

            // Member is Table (like M1 = x)
            if (argExpression is ParameterExpression memberExpression && ModelsRegistry.IsTable(memberExpression.Type)) {
              var table = ModelsRegistry.GetTable(memberExpression.Type);

              foreach (var column in ModelsRegistry.GetTable<TModel>().Columns)
                builder.AddSelectPart(column.TypedExpression.RawQuery, column.Type);

              resultProcessor.AddModelEntry(propertyInfo, table, withNullIndicator: false);
            }
            // Member is Member-expression (like Id = x.Id * 2)
            else {
              var options = new EvaluationOptions();
              exp = NodeVisitor.EvaluateToTypedExpression(argExpression, (string)null, options);
              builder.AddSelectPart(exp.RawQuery, exp.Type);
              resultProcessor.AddMemberEntry(propertyInfo, exp.Type);
            }
          }

          break;
        }

        /* For:
         * .Select(x => x.Val1)
         */
        default:
          exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);

          builder.AddSelectPart(exp.RawQuery, exp.Type);
          builder.SelectResultProcessor = new SingleValueSelectResultProcessor(exp.Type);

          break;
      }

      return builder;
    }

    public static SelectFromBuilder FromFieldListBuilder<TModel>(FieldListBuilder<TModel> builder)
    {
      var b = new SelectFromBuilder();
      b.AddTable(ModelsRegistry.GetTable<TModel>());

      var resultProcessor = new ModelSelectResultProcessor<TModel>();

      foreach (var column in builder.Fields) {
        b.AddSelectPart(column.TypedExpression.RawQuery, column.Type);
        resultProcessor.UseColumn(column);
      }

      b.SelectResultProcessor = resultProcessor;

      return b;
    }

    public static SelectFromBuilder AllColumns<TModel>()
    {
      var b = new SelectFromBuilder();
      b.AddTable(ModelsRegistry.GetTable<TModel>());

      foreach (var column in ModelsRegistry.GetTable<TModel>().Columns)
        b.AddSelectPart(column.TypedExpression.RawQuery, column.Type);

      b.SelectResultProcessor = new ModelSelectResultProcessor<TModel>();

      return b;
    }

    public void Distinct()
    {
      _distinct = true;
    }

    public RawQuery GetRawQuery()
    {
      RawQuery rq = new RawQuery();
      rq.Append("SELECT ");

      if (_distinct)
        rq.Append("DISTINCT ");

      bool firstColumn = true;
      foreach (var col in _columns) {
        var exp = col.RawQuery;
        var type = col.Type;

        if (!firstColumn)
          rq.Append(",");
        rq.AppendWithCast(exp, type.PostgresFetchType == type.PostgresTypeName ? null : type.PostgresFetchType);
        firstColumn = false;
      }

      rq.Append(" FROM ");

      int tableNum = 0;
      // bool firstTable = true;
      if (_tablePlaceholders.Count > 1) {
        var firstTable = _tablePlaceholders[0].Table;
        string alias = $"t{tableNum}";
        rq.AppendTableName(firstTable.Name, firstTable.Schema, alias);
        rq.ApplyAlias(_tablePlaceholders[0].Name, alias);
        tableNum++;

        foreach (var tablePlaceholder in _tablePlaceholders.Skip(1)) {
          var table = tablePlaceholder.Table;

          rq.Append(" LEFT JOIN ");

          alias = $"t{tableNum}";
          rq.AppendTableName(table.Name, table.Schema, alias);
          rq.ApplyAlias(tablePlaceholder.Name, alias);

          rq.Append(" ON ");
          rq.AppendSurround(LeftJoinsExpressions[tableNum].RawQuery);

          tableNum++;
        }
      }
      else {
        rq.AppendTableName(_tablePlaceholders[0].Name, _tablePlaceholders[0].Table.Schema);
      }

      if (_tablePlaceholders.Count == 1)
        rq.SkipExplicitColumnTableNames();
      return rq;
    }

    public ISelectResultProcessor GetResultProcessor()
    {
      return SelectResultProcessor;
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