using System.Collections.Generic;
using KDPgDriver.Builders;
using KDPgDriver.Results;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface ISelectQuery : IQuery
  {
    List<ResultColumnDef> GetColumns();
    bool IsSingleValue { get; }
  }

  public class SelectQuery<TModel, TOut> : ISelectQuery
  {
    private readonly string TableName = Helper.GetTableName(typeof(TModel));
    private readonly string SchemaName = Helper.GetTableSchema(typeof(TModel));

    private readonly IQueryBuilder _queryBuilder;

    private readonly ISelectFromBuilder _fromBuilder;
    private readonly IOrderBuilder _orderBuilder;
    private readonly LimitBuilder _limitBuilder;

    public bool IsSingleValue => _fromBuilder.IsSingleValue;

    public SelectQuery(IQueryBuilder queryBuilder,
                       ISelectFromBuilder fromBuilder,
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
      string schema = SchemaName ?? defaultSchema;

      RawQuery rq = new RawQuery();
      rq.Append("SELECT ")
        .Append(_fromBuilder.GetRawQuery())
        .Append(" FROM ")
        .AppendTableName(TableName, schema);

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