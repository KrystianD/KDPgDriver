using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface IDeleteQuery : IQuery { }

  public class DeleteQuery<TModel> : IDeleteQuery
  {
    private readonly KdPgTableDescriptor Table = Helper.GetTable<TModel>();

    private readonly WhereBuilder<TModel> _whereBuilder;

    public DeleteQuery(WhereBuilder<TModel> whereBuilder)
    {
      _whereBuilder = whereBuilder;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      string schema = Table.Schema ?? defaultSchema;

      RawQuery rq = new RawQuery();
      rq.Append("DELETE FROM ")
        .AppendTableName(Table.Name, schema);

      var whereRawQuery = _whereBuilder.GetRawQuery();
      if (!whereRawQuery.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(whereRawQuery);
      }

      rq.SkipExplicitColumnTableNames();
      return rq;
    }
  }
}