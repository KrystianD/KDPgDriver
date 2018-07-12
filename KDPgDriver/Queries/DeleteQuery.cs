namespace KDPgDriver.Builders
{
  public interface IDeleteQuery : IQuery { }

  public class DeleteQuery : IDeleteQuery
  {
    private readonly IQueryBuilder _builder;

    public DeleteQuery(IQueryBuilder queryBuilder)
    {
      _builder = queryBuilder;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      string schema = _builder.SchemaName ?? defaultSchema;

      RawQuery rq = new RawQuery();
      rq.Append("DELETE FROM ")
        .AppendTableName(_builder.TableName, schema);

      var whereRawQuery = _builder.GetWhereBuilder().GetRawQuery();
      if (!whereRawQuery.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(whereRawQuery);
      }

      return rq;
    }
  }
}