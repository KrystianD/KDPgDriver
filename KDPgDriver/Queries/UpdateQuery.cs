namespace KDPgDriver.Builders
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
      foreach (var (column, typedExpression) in _updateStatementsBuilder.UpdateParts) {
        if (!first)
          rq.Append(", ");

        rq.AppendColumnName(column.Name)
          .Append(" = ")
          .Append(typedExpression.RawQuery);

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