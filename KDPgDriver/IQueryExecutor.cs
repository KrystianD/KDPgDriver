using System.Threading.Tasks;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;
using KDPgDriver.Results;

namespace KDPgDriver {
  public interface IQueryExecutor
  {
    Task<InsertQueryResult> QueryAsync<TOut>(InsertQuery<TOut> insertQuery);
    Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> updateQuery);
    Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> selectQuery);
    Task<DeleteQueryResult> QueryAsync(DeleteQuery updateQuery);

    SelectQueryFluentBuilder1<TModel> From<TModel>();
  }
}