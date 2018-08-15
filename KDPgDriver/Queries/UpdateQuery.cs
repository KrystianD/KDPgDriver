using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface IUpdateQuery : IQuery { }

  public class UpdateQuery<TModel> : IUpdateQuery
  {
    private readonly string TableName = Helper.GetTableName(typeof(TModel));
    private readonly string SchemaName = Helper.GetTableSchema(typeof(TModel));

    private readonly UpdateStatementsBuilder<TModel> _updateStatementsBuilder;
    private readonly WhereBuilder<TModel> _whereBuilder;

    public UpdateQuery(WhereBuilder<TModel> whereBuilder, UpdateStatementsBuilder<TModel> updateStatementsBuilder)
    {
      _updateStatementsBuilder = updateStatementsBuilder;
      _whereBuilder = whereBuilder;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      string schema = SchemaName ?? defaultSchema;

      RawQuery rq = new RawQuery();
      rq.Append("UPDATE ")
        .AppendTableName(TableName, schema)
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

      var whereRawQuery = _whereBuilder.GetRawQuery();
      if (!whereRawQuery.IsEmpty) {
        rq.Append("WHERE ")
          .Append(whereRawQuery);
      }

      rq.SkipExplicitColumnTableNames();
      return rq;
    }
  }
}