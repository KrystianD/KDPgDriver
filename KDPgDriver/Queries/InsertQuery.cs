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

namespace KDPgDriver.Builders
{
  public class InsertQueryInit<TModel> { }

  public interface IInsertQuery : IQuery { }

  public class InsertQuery<TModel> : IInsertQuery
  {
    private static readonly KdPgTableDescriptor TableModel = Helper.GetTable(typeof(TModel));
    
    private readonly List<KdPgColumnDescriptor> _columns = new List<KdPgColumnDescriptor>();

    private readonly List<TModel> _objects = new List<TModel>();

    private readonly RawQuery _insertPartQuery = new RawQuery();

    public InsertQuery<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      PropertyInfo column = NodeVisitor.EvaluateToPropertyInfo(field);
      _columns.Add(Helper.GetColumn(column));
      return this;
    }

    public InsertQuery<TModel> AddObject(TModel obj)
    {
      if (_columns.Count == 0) {
        _columns.AddRange(Helper.GetTable(typeof(TModel)).Columns
                               .Where(x => (x.Flags & KDPgColumnFlagsEnum.PrimaryKey) == 0));
      }

      _objects.Add(obj);

      if (!_insertPartQuery.IsEmpty)
        _insertPartQuery.Append(",");
      _insertPartQuery.Append("(");

      for (int i = 0; i < _columns.Count; i++) {
        var column = _columns[i];
        object val = Helper.GetModelValueByColumn(obj, column);
        var npgValue = Helper.ConvertToNpgsql(column.Type, val);

        if (i > 0)
          _insertPartQuery.Append(",");

        _insertPartQuery.Append(npgValue);
      }

      _insertPartQuery.Append(")");

      return this;
    }

    public InsertQuery<TModel> AddMany(IEnumerable<TModel> objs)
    {
      foreach (var obj in objs)
        AddObject(obj);
      return this;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      RawQuery q = new RawQuery();

      q.Append("INSERT INTO ");

      q.AppendTableName(
          tableName: Helper.GetTableName(typeof(TModel)),
          schema: Helper.GetTableSchema(typeof(TModel)) ?? defaultSchema);

      q.Append("(").AppendColumnNames(_columns.Select(x => x.Name)).Append(")");

      q.Append(" VALUES ");
      q.Append(_insertPartQuery);

      if (TableModel.PrimaryKey != null) {
        q.Append(" RETURNING ");
        q.AppendColumnName(TableModel.PrimaryKey.Name);
      }

      return q;
    }
  }
}