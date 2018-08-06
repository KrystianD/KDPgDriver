using System.Collections.Generic;
using System.Threading.Tasks;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;
using KDPgDriver.Results;

namespace KDPgDriver
{
  public interface IQueryExecutor
  {
    Task<InsertQueryResult> QueryAsync(IInsertQuery insertQuery);
    Task<UpdateQueryResult> QueryAsync(IUpdateQuery updateQuery);
    Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> selectQuery);
    Task<DeleteQueryResult> QueryAsync(IDeleteQuery updateQuery);

    SelectQueryFluentBuilder1Prep<TModel> From<TModel>();
    InsertQueryFluentBuilder1<TModel> Insert<TModel>();
    UpdateQueryFluentBuilder1<TModel> Update<TModel>();
    DeleteQueryFluentBuilder1<TModel> Delete<TModel>();

    InsertQueryFluentBuilder1<T> Insert<T>(T obj);
    InsertQueryFluentBuilder1<T> InsertMany<T>(IEnumerable<T> objects);
  }
}