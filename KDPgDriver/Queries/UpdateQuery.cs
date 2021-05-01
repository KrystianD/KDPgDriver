using System.Threading.Tasks;
using KDPgDriver.Builders;
using KDPgDriver.Results;
using KDPgDriver.Types;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver.Queries
{
  public interface IUpdateQuery : IQuery
  {
    Task<UpdateQueryResult> ReadResultAsync(NpgsqlDataReader reader);
  }

  public class UpdateQuery<TModel> : IUpdateQuery
  {
    private readonly KdPgTableDescriptor Table = ModelsRegistry.GetTable<TModel>();

    private readonly UpdateStatementsBuilder<TModel> _updateStatementsBuilder;
    private readonly WhereBuilder<TModel> _whereBuilder;

    public UpdateQuery(WhereBuilder<TModel> whereBuilder, UpdateStatementsBuilder<TModel> updateStatementsBuilder)
    {
      _updateStatementsBuilder = updateStatementsBuilder;
      _whereBuilder = whereBuilder;
    }

    public RawQuery GetRawQuery()
    {
      RawQuery rq = new RawQuery();
      rq.Append("UPDATE ")
        .AppendTableName(Table.Name, Table.Schema)
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

    public async Task<UpdateQueryResult> ReadResultAsync(NpgsqlDataReader reader)
    {
      await reader.NextResultAsync();
      return new UpdateQueryResult();
    }
  }
}