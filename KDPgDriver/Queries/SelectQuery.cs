using System.Collections.Generic;
using NLog.LayoutRenderers;

namespace KDPgDriver.Builders
{
  public interface ISelectQuery : IQuery { }

  public class SelectQuery<TOut> : ISelectQuery
  {
    private readonly IQueryBuilder _queryBuilder;

    private readonly SelectFromBuilder<TOut> _fromBuilder;
    private readonly IOrderBuilder _orderBuilder;
    private readonly LimitBuilder _limitBuilder;
    
    public bool IsSingleValue => _fromBuilder.IsSingleValue;

    public SelectQuery(IQueryBuilder queryBuilder,
                       SelectFromBuilder<TOut> fromBuilder,
                       IOrderBuilder orderBuilder,
                       LimitBuilder limitBuilder)
    {
      _queryBuilder = queryBuilder;
      _fromBuilder = fromBuilder;
      _orderBuilder = orderBuilder;
      _limitBuilder = limitBuilder;
    }

    public List<ResultColumnDef> GetColumns() => _fromBuilder.GetColumns();

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      string schema = _queryBuilder.SchemaName ?? defaultSchema;

      RawQuery rq = new RawQuery();
      rq.Append("SELECT ")
        .Append(_fromBuilder.GetRawQuery())
        .Append(" FROM ")
        .AppendTableName(_queryBuilder.TableName, schema);

      var whereRawQuery = _queryBuilder.GetWhereBuilder().GetRawQuery();
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