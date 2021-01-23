using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface ISelectQuery : IQuery
  {
    IResultProcessor GetResultProcessor();
  }

  // ReSharper disable UnusedTypeParameter
  public class SelectQuery<TModel, TOut> : ISelectQuery
  // ReSharper restore UnusedTypeParameter
  {
    private readonly IWhereBuilder _whereBuilder;

    private readonly ISelectFromBuilder _fromBuilder;
    private readonly IOrderBuilder _orderBuilder;
    private readonly LimitBuilder _limitBuilder;

    private readonly bool _existsQuery;

    public IResultProcessor GetResultProcessor() => _fromBuilder.GetResultProcessor();

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
  }
}