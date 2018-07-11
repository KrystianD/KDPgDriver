namespace KDPgDriver.Builder
{
  public interface IUpdateQuery : IQuery { }

  public class UpdateQuery<TOut> : IUpdateQuery
  {
    private readonly UpdateStatementsBuilder<TOut> _updateStatementsBuilder;
    private readonly IQueryBuilder _builder;

    public UpdateQuery(IQueryBuilder queryBuilder, UpdateStatementsBuilder<TOut> updateStatementsBuilder)
    {
      _updateStatementsBuilder = updateStatementsBuilder;
      _builder = queryBuilder;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      string schema = _builder.SchemaName ?? defaultSchema;

      RawQuery rq = new RawQuery();
      rq.Append("UPDATE ")
        .AppendTableName(_builder.TableName, schema)
        .Append("\n");

      rq.Append("SET ");
      bool first = true;
      foreach (var (name, rawQuery) in _updateStatementsBuilder.UpdateParts) {
        if (!first)
          rq.Append(", ");

        rq.AppendColumnName(name)
          .Append(" = ")
          .Append(rawQuery);

        first = false;
      }
      
      rq.Append("\n");

      var whereRawQuery = _builder.GetWhereBuilder().GetRawQuery();
      if (!whereRawQuery.IsEmpty) {
        rq.Append("WHERE ")
          .Append(whereRawQuery);
      }

      return rq;
    }
  }
}