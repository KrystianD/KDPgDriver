using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace KDPgDriver
{
  public class Driver
  {
    private NpgsqlConnection _npgsqlConnection;

    public async Task ConnectAsync()
    {
      var connString = "Host=127.0.0.1;Database=test;Username=postgres";

      _npgsqlConnection = new NpgsqlConnection(connString);
      await _npgsqlConnection.OpenAsync();

      string fn = @"
CREATE OR REPLACE FUNCTION f_escape_regexp(text) RETURNS text AS
$func$
SELECT regexp_replace($1, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\1', 'g')
$func$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION f_escape_like(text) RETURNS text AS
$func$
SELECT replace(replace(replace($1
         , '\', '\\')  -- must come 1st
         , '%', '\%')
         , '_', '\_');
$func$
LANGUAGE sql IMMUTABLE;
";

      var t = _npgsqlConnection.CreateCommand();
      t.CommandText = fn;
      await t.ExecuteNonQueryAsync();
    }

    public async Task<Result<TOut>> QueryAsync<TOut>(UpdateQuery<TOut> builder) where TOut : class
    {
      string query = builder.GetQuery();
      var p = builder.GetParams();

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, _npgsqlConnection)) {
        for (int i = 0; i < p.Count; i++)
          cmd.Parameters.AddWithValue($"{i}", p[i]);

        await cmd.ExecuteNonQueryAsync();
      }

      return null;
    }

    public async Task<Result<TOut>> QueryAsync<TOut>(SelectQuery<TOut> builder) where TOut : class
    {
      string query = builder.GetQuery();
      var p = builder.GetParams();

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, _npgsqlConnection)) {
        for (int i = 0; i < p.Count; i++)
          cmd.Parameters.AddWithValue($"{i}", p[i]);

        using (var reader = await cmd.ExecuteReaderAsync()) {
          while (await reader.ReadAsync()) {
            for (int i = 0; i < reader.FieldCount; i++) {
              var t = reader.GetValue(i);

              if (t is Array a) {
                Console.Write("[");
                foreach (var item in a) {
                  Console.Write(item);
                  Console.Write(",");
                }

                Console.Write("]");
              }
              else {
                Console.Write(t);
              }

              Console.Write(",");
            }

            Console.WriteLine();
          }
        }
      }

      return new Result<TOut>();
      // return builder.
    }
  }

  public class TypedValue
  {
    public string Expression { get; }
    public Type Type { get; }

    public TypedValue(string expression, Type type)
    {
      Expression = expression;
      Type = type;
    }
  }

  public interface IBaseQueryBuilder
  {
    string TableName { get; }

    List<object> GetParams();
    string GetNextParam(object value);
    string GetWherePart();
    TypedValue Visit(Expression e);
  }


  public class BaseBaseQueryBuilder<TModel> : IBaseQueryBuilder
  {
    public string TableName { get; }

    private readonly List<object> _params = new List<object>();
    private readonly StringBuilder _wherePart = new StringBuilder();

    public string GetWherePart() => _wherePart.ToString();

    public BaseBaseQueryBuilder()
    {
      TableName = Helper.GetTableName(typeof(TModel));
    }

    public string GetNextParam(object value)
    {
      if (value is string s) {
        if (s.Length < 30) {
          return "'" + s.Replace("'", "''") + "'";
        }
      }

      var name = $"@{_params.Count}";
      _params.Add(value);
      return name;
    }

    public object GetConstant(Expression e)
    {
      switch (e) {
        case ConstantExpression me:
          return me.Value;
        default:
          throw new Exception($"invalid node: {(e == null ? "(null)" : e.NodeType.ToString())}");
      }
    }


    public TypedValue Visit(Expression e)
    {
      switch (e) {
        // case NewArrayExpression newArrayExpression:
        //   var itemType = newArrayExpression.Type.GetElementType();
        //   
        //   
        //   
        //   return null;

        case MemberExpression me:
          return new TypedValue(Helper.GetColumnName(me.Member), Helper.GetColumnType(me.Member));

        case ConstantExpression me:
          return new TypedValue(GetNextParam(me.Value), me.Type);

        case BinaryExpression be:
          TypedValue left, right;

          switch (be.NodeType) {
            case ExpressionType.Equal:
              left = Visit(be.Left);
              right = Visit(be.Right);

              return new TypedValue($"{left.Expression} = {right.Expression}", typeof(bool));

            case ExpressionType.Add:
              left = Visit(be.Left);
              right = Visit(be.Right);

              string op;
              if (left.Type == typeof(string) && right.Type == typeof(string))
                op = "||";
              else
                op = "+";

              return new TypedValue($"{left.Expression} {op} {right.Expression}", typeof(string));

            case ExpressionType.AndAlso:
              left = Visit(be.Left);
              right = Visit(be.Right);
              return new TypedValue($"({left.Expression}) AND ({right.Expression})", typeof(bool));

            case ExpressionType.OrElse:
              left = Visit(be.Left);
              right = Visit(be.Right);
              return new TypedValue($"({left.Expression}) OR ({right.Expression})", typeof(bool));

            default:
              throw new Exception($"unknown operator: {be.NodeType}");
          }

        case MethodCallExpression call:
          var callObject = call.Object != null ? Visit(call.Object) : null;
          string txt;

          if (call.Method.Name == "PgIn") {
            callObject = Visit(call.Arguments[0]);
            var value = GetConstant(call.Arguments[1]);

            StringBuilder sb = new StringBuilder();
            if (value is Array array) {
              foreach (var item in array) {
                sb.Append(GetNextParam(item));
                sb.Append(",");
              }

              sb.Remove(sb.Length - 1, 1);
            }
            else {
              throw new Exception($"invalid array: {value.GetType()}");
            }

            return new TypedValue($"({callObject.Expression}) IN ({sb})", typeof(string[]));
          }
          else if (call.Method.Name == "Substring") {
            string start = Visit(call.Arguments[0]).Expression;
            string length = Visit(call.Arguments[1]).Expression;
            return new TypedValue($"substring(({callObject.Expression}) from ({start}) for ({length}))", typeof(string));
          }
          else if (call.Method.Name == "StartsWith") {
            txt = Visit(call.Arguments[0]).Expression;
            return new TypedValue($"({callObject.Expression}) LIKE (f_escape_like({txt}) || '%')", typeof(string));
          }
          else if (call.Method.Name == "get_Item") {
            txt = Visit(call.Arguments[0]).Expression;

            return new TypedValue($"({callObject.Expression})->{txt}", typeof(object));
          }
          else if (call.Method.Name == "Contains") {
            if (callObject.Type.IsGenericType && callObject.Type.GetGenericTypeDefinition() == typeof(List<>)) {
              var value = Visit(call.Arguments[0]).Expression;
              return new TypedValue($"({value}) = ANY({callObject.Expression})", typeof(string));
            }
            else {
              throw new Exception($"Contains cannot be used on non-list");
            }
          }
          else { throw new Exception($"invalid method: {call.Method.Name}"); }

        default:
          throw new Exception($"invalid node: {(e == null ? "(null)" : e.NodeType.ToString())}");
      }
    }

    public BaseBaseQueryBuilder<TModel> Where(Expression<Func<TModel, bool>> exp)
    {
      var e = Evaluator.PartialEval(exp.Body);
      var whereSql = "(" + Visit(e).Expression + ")";

      if (_wherePart.Length > 0)
        _wherePart.Append(" AND ");
      _wherePart.Append(whereSql);

      return this;
    }

    public SelectQuery<TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      var us = new SelectQuery<TNewModel>(this);
      us.Process(pr.Body);
      return us;
    }

    public UpdateQuery<TModel> Update(Action<UpdateStatementsBuilder<TModel>> fn)
    {
      var uq = new UpdateQuery<TModel>(this);
      var us = new UpdateStatementsBuilder<TModel>(uq);
      fn(us);
      return uq;
    }

    public List<object> GetParams()
    {
      return _params;
    }
  }

  public class UpdateStatementsBuilder<TModel>
  {
    private readonly UpdateQuery<TModel> _updateQuery;


    public UpdateStatementsBuilder(UpdateQuery<TModel> updateQuery)
    {
      _updateQuery = updateQuery;
    }

    public UpdateStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
    {
      switch (field.Body) {
        case MemberExpression memberExpression:
          string colName = Helper.GetColumnName(memberExpression.Member);

          _updateQuery.updateParts.Add(colName, _updateQuery.Builder.GetNextParam(value));

          break;
        default:
          throw new Exception($"invalid node: {field.Body.NodeType}");
      }

      // var v = _fromBuilder.Visit(field);
      return this;
    }
  }

  public class SelectQuery<TOut>
  {
    public IBaseQueryBuilder Builder { get; }

    private StringBuilder selectPart = new StringBuilder();

    public SelectQuery(IBaseQueryBuilder baseQueryBuilder)
    {
      Builder = baseQueryBuilder;
    }

    public List<object> GetParams()
    {
      return Builder.GetParams();
    }

    public string GetQuery()
    {
      string q = $"SELECT {selectPart} FROM public.{Builder.TableName}";
      string wherePart = Builder.GetWherePart();
      if (wherePart.Length > 0)
        q += $" WHERE {wherePart}";

      return q;
    }

    private string BuildSelectExpression(Expression e)
    {
      return Builder.Visit(e).Expression;
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

  public class UpdateQuery<TOut>
  {
    public IBaseQueryBuilder Builder { get; }

    public Dictionary<string, string> updateParts = new Dictionary<string, string>();

    public UpdateQuery(IBaseQueryBuilder baseQueryBuilder)
    {
      Builder = baseQueryBuilder;
    }

    public List<object> GetParams()
    {
      return Builder.GetParams();
    }

    public string GetQuery()
    {
      string q = $"UPDATE public.{Builder.TableName}";

      string setPart = string.Join(", ", updateParts.Select(x => $"{x.Key} = {x.Value}"));
      q += $" SET {setPart}";

      string wherePart = Builder.GetWherePart();
      if (wherePart.Length > 0)
        q += $" WHERE {wherePart}";

      return q;
    }
  }

  public class Result<T> { }

  public static class Ext
  {
    public static IEnumerable<(TFirst, TSecond)> Zip<TFirst, TSecond>(this IEnumerable<TFirst> first, IEnumerable<TSecond> second)
    {
      return first.Zip(second, (a, b) => (a, b));
    }
  }
}