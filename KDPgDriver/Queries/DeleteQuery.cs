using System.Threading.Tasks;
using KDPgDriver.Builders;
using KDPgDriver.Results;
using KDPgDriver.Types;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver.Queries
{
  public interface IDeleteQuery : IQuery
  {
    Task<DeleteQueryResult> ReadResultAsync(NpgsqlDataReader reader);
  }

  public class DeleteQuery<TModel> : IDeleteQuery
  {
    private readonly KdPgTableDescriptor Table = ModelsRegistry.GetTable<TModel>();

    private readonly WhereBuilder<TModel> _whereBuilder;

    public DeleteQuery(WhereBuilder<TModel> whereBuilder)
    {
      _whereBuilder = whereBuilder;
    }

    public RawQuery GetRawQuery()
    {
      RawQuery rq = new RawQuery();
      rq.Append("DELETE FROM ")
        .AppendTableName(Table.Name, Table.Schema);

      var whereRawQuery = _whereBuilder.GetRawQuery();
      if (!whereRawQuery.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(whereRawQuery);
      }

      return rq;
    }

    public Task<DeleteQueryResult> ReadResultAsync(NpgsqlDataReader reader) => Task.FromResult(new DeleteQueryResult());
  }
}