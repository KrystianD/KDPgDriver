using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface ISelectQuery : IQuery
  {
    IResultProcessor GetResultProcessor();
  }

  public class SelectQuery<TModel, TOut> : ISelectQuery
  {
    private readonly IWhereBuilder _whereBuilder;

    private readonly ISelectFromBuilder _fromBuilder;
    private readonly IOrderBuilder _orderBuilder;
    private readonly LimitBuilder _limitBuilder;

    public IResultProcessor GetResultProcessor() => _fromBuilder.GetResultProcessor();

    public SelectQuery(IWhereBuilder whereBuilder,
                       ISelectFromBuilder fromBuilder,
                       IOrderBuilder orderBuilder,
                       LimitBuilder limitBuilder)
    {
      _whereBuilder = whereBuilder;
      _fromBuilder = fromBuilder;
      _orderBuilder = orderBuilder;
      _limitBuilder = limitBuilder;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      RawQuery rq = _fromBuilder.GetRawQuery(defaultSchema);

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

      return rq;
    }
  }
}