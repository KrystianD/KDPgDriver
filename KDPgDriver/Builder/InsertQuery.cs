using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using KDLib;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class InsertQueryInit<TModel> { }

  public interface IInsertQuery : IQuery { }

  public class InsertQuery<TModel> : IInsertQuery
  {
    private List<KdPgColumnDescriptor> columns = new List<KdPgColumnDescriptor>();
    private List<string> extractors = new List<string>();

    private List<TModel> objects = new List<TModel>();

    // private StringBuilder insertStr = new StringBuilder();
    private RawQuery insertPartQuery = new RawQuery();

    public ParametersContainer Parameters { get; } = new ParametersContainer();

    private KdPgTableDescriptor TableModel = Helper.GetTable(typeof(TModel));

    private bool preparation = true;

    public InsertQuery() { }

    public InsertQuery<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      PropertyInfo column = NodeVisitor.EvaluateToPropertyInfo(field);
      columns.Add(Helper.GetColumn(column));
      return this;
    }

    public InsertQuery<TModel> AddObject(TModel obj)
    {
      preparation = false;

      if (columns.Count == 0) {
        columns.AddRange(Helper.GetTable(typeof(TModel)).Columns
                               .Where(x => (x.Flags & KDPgColumnFlagsEnum.PrimaryKey) == 0));
      }

      objects.Add(obj);

      if (!insertPartQuery.IsEmpty)
        insertPartQuery.Append(",");
      insertPartQuery.Append("(");

      for (int i = 0; i < columns.Count; i++) {
        var column = columns[i];
        object val = Helper.GetModelValueByColumn(obj, column);

        var npgValue = Helper.ConvertToNpgsql(column, val);

        if (i > 0)
          insertPartQuery.Append(",");

        insertPartQuery.Append(val == null ? "NULL" : Parameters.GetNextParam(npgValue));
      }

      insertPartQuery.Append(")");

      return this;
    }

    public InsertQuery<TModel> AddMany(IEnumerable<TModel> objs)
    {
      foreach (var obj in objs)
        AddObject(obj);
      return this;
    }

    public RawQuery GetQuery(Driver driver)
    {
      RawQuery q = new RawQuery();

      q.Append("INSERT INTO ");

      q.AppendTableName(
          tableName: Helper.GetTableName(typeof(TModel)),
          schema: Helper.GetTableSchema(typeof(TModel)) ?? driver.Schema);

      q.Append("(").AppendColumnNames(columns.Select(x => x.Name)).Append(")");

      q.Append(" VALUES ");
      q.Append(insertPartQuery);

      if (TableModel.PrimaryKey != null) {
        q.Append(" RETURNING ");
        q.AppendColumnName(TableModel.PrimaryKey.Name);
      }

      return q;
    }
  }
}