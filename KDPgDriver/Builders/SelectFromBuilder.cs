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

  public class SingleValueResultProcessor : IResultProcessor
  {
    private readonly KDPgValueType _type;

    public int FieldsCount => 1;

    public SingleValueResultProcessor(KDPgValueType type)
    {
      _type = type;
    }

    public object ParseResult(object[] values)
    {
      return Helper.ConvertFromNpgsql(_type, values[0]);
    }
  }

  // public class AnonymousTypeProcessor : IResultProcessor
  // {
  //   private readonly Type _anonymousType;
  //   private readonly List<KDPgValueType> _members = new List<KDPgValueType>();
  //
  //   public int FieldsCount => _members.Count;
  //
  //   public AnonymousTypeProcessor(Type anonymousType)
  //   {
  //     _anonymousType = anonymousType;
  //   }
  //
  //   public void AddMember(KDPgValueType expType)
  //   {
  //     _members.Add(expType);
  //   }
  //
  //   public object ParseResult(object[] rawFieldValues)
  //   {
  //     int fieldsCount = _members.Count;
  //
  //     object[] values = new object[fieldsCount];
  //     for (int i = 0; i < fieldsCount; i++) {
  //       object outputValue = Helper.ConvertFromNpgsql(_members[i], rawFieldValues[i]);
  //       values[i] = outputValue;
  //     }
  //
  //     return Activator.CreateInstance(_anonymousType, values);
  //   }
  // }

  public class ModelResultProcessor<TModel> : IResultProcessor
  {
    private static readonly KdPgTableDescriptor Table = Helper.GetTable<TModel>();

    public int FieldsCount => Table.Columns.Count;

    public object ParseResult(object[] values)
    {
      var obj = Activator.CreateInstance(Table.ModelType);

      for (var i = 0; i < Table.Columns.Count; i++) {
        var col = Table.Columns[i];
        var val = Helper.ConvertFromNpgsql(col.Type, values[i]);
        col.PropertyInfo.SetValue(obj, val);
      }

      return obj;
    }
  }

  public class AnonymousTypeResultProcessor<TModel> : IResultProcessor
  {
    public class Entry
    {
      public KdPgTableDescriptor MemberTable;

      public KDPgValueType MemberType;
    }

    private readonly List<Entry> Entries = new List<Entry>();

    private int fieldsCount = 0;

    public int FieldsCount => fieldsCount;

    public object ParseResult(object[] values)
    {
      object[] constructorParams = new object[Entries.Count];

      int columnIdx = 0;

      for (var i = 0; i < Entries.Count; i++) {
        var entry = Entries[i];

        if (entry.MemberTable != null) {
          var table = Entries[i].MemberTable;

          var modelObj = Activator.CreateInstance(table.ModelType);

          foreach (var column in table.Columns) {
            var val = Helper.ConvertFromNpgsql(column.Type, values[columnIdx++]);
            column.PropertyInfo.SetValue(modelObj, val);
          }

          constructorParams[i] = modelObj;
        }
        else if (entry.MemberType != null) {
          constructorParams[i] = Helper.ConvertFromNpgsql(entry.MemberType, values[columnIdx++]);
        }
        else
          throw new Exception();
      }

      return Activator.CreateInstance(typeof(TModel), constructorParams);
    }

    public void AddModelEntry(KdPgTableDescriptor table)
    {
      fieldsCount += table.Columns.Count;

      Entries.Add(new Entry {
          MemberTable = table,
      });
    }

    public void AddMemberEntry(KDPgValueType memberType)
    {
      fieldsCount += 1;

      Entries.Add(new Entry {
          MemberType = memberType,
      });
    }
  }

  public class SelectFromBuilder : ISelectFromBuilder
  {
    private class ResultColumnDef
    {
      public KDPgValueType Type;
      public RawQuery RawQuery;
    }

    private readonly List<ResultColumnDef> _columns = new List<ResultColumnDef>();
    private readonly List<KdPgTableDescriptor> _tables = new List<KdPgTableDescriptor>();

    private IResultProcessor ResultProcessor { get; set; }

    public static SelectFromBuilder FromCombinedExpression<TCombinedModel, TNewModel>(TablesList tablesList, Expression<Func<TCombinedModel, TNewModel>> prBody)
    {
      var b = new SelectFromBuilder();

      foreach (var table in tablesList.Tables)
        b.AddTable(table);

      TypedExpression exp;
      switch (prBody.Body) {
        case NewExpression newExpression:
        {
          IEnumerable<PropertyInfo> members = newExpression.Members.Cast<PropertyInfo>();
          IEnumerable<Expression> args = newExpression.Arguments;

          var resultProcessor = new AnonymousTypeResultProcessor<TNewModel>();
          b.ResultProcessor = resultProcessor;

          foreach (var (member, argExpression) in members.Zip(args)) {
            if (argExpression is MemberExpression memberExpression && Helper.IsTable(memberExpression.Type)) {
              var table = Helper.GetTable(memberExpression.Type);

              foreach (var column in table.Columns)
                b.AddSelectPart(column.TypedExpression.RawQuery, column.PropertyInfo, column.Type);

              resultProcessor.AddModelEntry(table);
            }
            else {
              exp = NodeVisitor.EvaluateToTypedExpression(argExpression);
              b.AddSelectPart(exp.RawQuery, member, exp.Type);
              resultProcessor.AddMemberEntry(exp.Type);
            }
          }

          break;
        }

        default:
          exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);

          b.AddSelectPart(exp.RawQuery, null, exp.Type);
          b.ResultProcessor = new SingleValueResultProcessor(exp.Type);

          break;
      }

      return b;
    }

    public static SelectFromBuilder AllColumnsFromCombined<TCombinedModel>(TablesList tablesList)
    {
      var b = new SelectFromBuilder();

      var pr = new AnonymousTypeResultProcessor<TCombinedModel>();

      foreach (var table in tablesList.Tables) {
        b.AddTable(table);

        pr.AddModelEntry(table);

        foreach (var column in table.Columns)
          b.AddSelectPart(column.TypedExpression.RawQuery, column.PropertyInfo, column.Type);
      }

      b.ResultProcessor = pr;

      return b;
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
            b.AddSelectPart(exp.RawQuery, member, exp.Type);
            resultProcessor.AddMemberEntry(exp.Type);
          }

          break;
        }

        default:
          exp = NodeVisitor.EvaluateToTypedExpression(prBody.Body);

          b.AddSelectPart(exp.RawQuery, null, exp.Type);
          b.ResultProcessor = new SingleValueResultProcessor(exp.Type);

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
        b.AddSelectPart(RawQuery.CreateColumn(column), column.PropertyInfo, column.Type);
      }

      b.ResultProcessor = new ModelResultProcessor<TModel>();

      return b;
    }

    public static SelectFromBuilder AllColumns<TModel>()
    {
      var b = new SelectFromBuilder();
      b.AddTable(Helper.GetTable<TModel>());

      foreach (var column in Helper.GetTable(typeof(TModel)).Columns)
        b.AddSelectPart(column.TypedExpression.RawQuery, column.PropertyInfo, column.Type);

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
        rq.AppendWithCast(exp, type.PostgresFetchType == type.PostgresType ? null : type.PostgresFetchType);
        firstColumn = false;
      }

      rq.Append(" FROM ");

      int i = 1;
      bool firstTable = true;
      foreach (var table in _tables) {
        if (!firstTable)
          rq.Append(",");
        firstTable = false;

        string alias = $"t{i++}";

        if (_tables.Count > 1) {
          rq.AppendTableName(table.Name, table.Schema ?? defaultSchema, alias);
          rq.ApplyAlias(table, alias);
        }
        else {
          rq.AppendTableName(table.Name, table.Schema ?? defaultSchema);
        }
      }

      return rq;
    }

    public IResultProcessor GetResultProcessor()
    {
      return ResultProcessor;
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
          Type = type,
      });
    }
  }
}