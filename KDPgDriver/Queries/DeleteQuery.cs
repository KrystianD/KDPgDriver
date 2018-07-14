using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface IDeleteQuery : IQuery { }

  public class DeleteQuery<TModel> : IDeleteQuery
  {
    private readonly string TableName = Helper.GetTableName(typeof(TModel));
    private readonly string SchemaName = Helper.GetTableSchema(typeof(TModel));
    
    private readonly WhereBuilder<TModel>  _whereBuilder;

    public DeleteQuery(WhereBuilder<TModel> whereBuilder)
    {
      _whereBuilder = whereBuilder;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      string schema = SchemaName ?? defaultSchema;

      RawQuery rq = new RawQuery();
      rq.Append("DELETE FROM ")
        .AppendTableName(TableName, schema);

      var whereRawQuery = _whereBuilder.GetRawQuery();
      if (!whereRawQuery.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(whereRawQuery);
      }

      return rq;
    }
  }
}