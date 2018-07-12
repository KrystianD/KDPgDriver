using System;
using System.Linq.Expressions;
using System.Reflection;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  // public class InsertStatementsBuilder<TModel>
  // {
  //   private readonly UpdateQuery<TModel> _updateQuery;
  //
  //   public InsertStatementsBuilder(UpdateQuery<TModel> updateQuery)
  //   {
  //     _updateQuery = updateQuery;
  //   }
  //
  //   public InsertStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
  //   {
  //     switch (field.Body) {
  //       case MemberExpression memberExpression:
  //
  //         PropertyInfo columnPropertyInfo = (PropertyInfo) memberExpression.Member;
  //         string colName = Helper.GetColumn(columnPropertyInfo).Name;
  //         var npgValue = Helper.ConvertToNpgsql(columnPropertyInfo, value);
  //         _updateQuery.updateParts.Add(colName, RawQuery.Create(npgValue));
  //         break;
  //       default:
  //         throw new Exception($"invalid node: {field.Body.NodeType}");
  //     }
  //
  //     return this;
  //   }
  // }
}