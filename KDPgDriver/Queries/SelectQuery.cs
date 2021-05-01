using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using KDPgDriver.Builders;
using KDPgDriver.Results;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver.Queries
{
  public interface ISelectQuery : IQuery { }

  // ReSharper disable UnusedTypeParameter
  public class SelectQuery<TModel, TOut> : ISelectQuery
  {
    private readonly IWhereBuilder _whereBuilder;

    private readonly ISelectFromBuilder _fromBuilder;
    private readonly IOrderBuilder _orderBuilder;
    private readonly LimitBuilder _limitBuilder;

    private readonly bool _existsQuery;

    public SelectQuery(IWhereBuilder whereBuilder,
                       ISelectFromBuilder fromBuilder,
                       IOrderBuilder orderBuilder,
                       LimitBuilder limitBuilder,
                       bool existsQuery)
    {
      _whereBuilder = whereBuilder;
      _fromBuilder = fromBuilder;
      _orderBuilder = orderBuilder;
      _limitBuilder = limitBuilder;
      _existsQuery = existsQuery;
    }

    public RawQuery GetRawQuery()
    {
      RawQuery rq = _fromBuilder.GetRawQuery();

      var whereRawQuery = _whereBuilder.GetRawQuery();
      if (!whereRawQuery.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(whereRawQuery);
      }

      if (_orderBuilder != null) {
        var r = _orderBuilder.GetRawQuery();
        if (!r.IsEmpty) {
          rq.Append(" ORDER BY ");
          rq.Append(r);
        }
      }

      if (_limitBuilder != null) {
        if (_limitBuilder.LimitValue.HasValue)
          rq.Append($" LIMIT {_limitBuilder.LimitValue}");
        if (_limitBuilder.OffsetValue.HasValue)
          rq.Append($" OFFSET {_limitBuilder.OffsetValue}");
      }

      if (_existsQuery) {
        var existsRq = new RawQuery();
        existsRq.AppendFuncInvocation("SELECT EXISTS", rq);
        rq = existsRq;
      }

      return rq;
    }

    internal async Task<SelectQueryResult<TOut>> ReadResultAsync(NpgsqlDataReader reader)
    {
      var proc = _fromBuilder.GetResultProcessor();

      Debug.Assert(proc.FieldsCount == reader.FieldCount, "proc.FieldsCount == reader.FieldCount");

      var objects = new List<TOut>();

      object[] values = new object[reader.FieldCount];

      while (await reader.ReadAsync()) {
        for (int i = 0; i < reader.FieldCount; i++)
          values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);

        objects.Add((TOut)proc.ParseResult(values));
      }

      await reader.NextResultAsync();

      return new SelectQueryResult<TOut>(objects);
    }
  }
}