using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using KDLib;
using KDPgDriver.Builders;
using KDPgDriver.Queries;
using KDPgDriver.Types;
using KDPgDriver.Utils;

namespace KDPgDriver.Traverser
{
  internal class EvaluationOptions
  {
    public readonly Dictionary<ParameterExpression, RawQuery.TableNamePlaceholder> ParameterToTableAlias = new Dictionary<ParameterExpression, RawQuery.TableNamePlaceholder>();

    public bool ExpandBooleans { get; set; } = false;
  }

  internal class PathInfo
  {
    public TypedExpression Expression;
    public KdPgColumnDescriptor Column { get; set; }
    public List<object> JsonPath { get; } = new List<object>();
    public ParameterExpression ParameterExp;
  }

  internal static class NodeVisitor
  {
    public static KdPgColumnDescriptor EvaluateFuncExpressionToColumn<TModel>(Expression<Func<TModel, object>> exp)
    {
      return EvaluateExpressionToColumn(exp.Body);
    }

    public static KdPgColumnDescriptor EvaluateFuncExpressionToColumn<TModel, T>(Expression<Func<TModel, T>> exp)
    {
      return EvaluateExpressionToColumn(exp.Body);
    }

    public static KdPgColumnDescriptor EvaluateExpressionToColumn(Expression exp)
    {
      PropertyInfo EvaluateToPropertyInfo(Expression exp2)
      {
        switch (exp2) {
          case LambdaExpression lambda:
            return EvaluateToPropertyInfo(lambda.Body);

          case MemberExpression me:
            if (me.Member.Name == "Value") // unwrap optional type
              return (PropertyInfo)((MemberExpression)me.Expression).Member;
            return (PropertyInfo)me.Member;

          case UnaryExpression un:
            return un.NodeType switch {
                ExpressionType.Convert => (PropertyInfo)((MemberExpression)un.Operand).Member,
                _ => throw new InvalidOperationException($"unknown operator: {un.NodeType}"),
            };

          default:
            throw new Exception($"invalid node: {exp.NodeType}");
        }
      }

      return ModelsRegistry.GetColumn(EvaluateToPropertyInfo(exp));
    }

    public static TypedExpression VisitFuncExpression<TModel>(Expression<Func<TModel, object>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel, T>(Expression<Func<TModel, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel1, TModel2, T>(Expression<Func<TModel1, TModel2, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel1, TModel2, TModel3, T>(Expression<Func<TModel1, TModel2, TModel3, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel1, TModel2, TModel3, TModel4, T>(Expression<Func<TModel1, TModel2, TModel3, TModel4, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression EvaluateToTypedExpression(Expression expression, string inputParameterName, EvaluationOptions options = null)
    {
      return EvaluateToTypedExpression(expression, inputParameterName == null ? null : new HashSet<string> { inputParameterName }, options);
    }

    public static TypedExpression EvaluateToTypedExpression(Expression expression, HashSet<string> inputParametersNames = null, EvaluationOptions options = null)
    {
      return new NodeVisitorInternal(options).EvaluateToTypedExpression(expression, inputParametersNames);
    }

    public static PathInfo VisitPath<TModel>(Expression<Func<TModel, object>> expression)
    {
      return new NodeVisitorInternal(null).VisitPath(expression);
    }

    public static PathInfo VisitPath<TModel>(Expression<Func<TModel, object>> expression, EvaluationOptions options)
    {
      return new NodeVisitorInternal(options).VisitPath(expression);
    }

    public static PathInfo VisitPath(Expression expression, EvaluationOptions options)
    {
      return new NodeVisitorInternal(options).VisitPath(expression);
    }
  }

  internal class NodeVisitorInternal
  {
    private readonly EvaluationOptions _options;

    public NodeVisitorInternal(EvaluationOptions options)
    {
      _options = options ?? new EvaluationOptions();
    }

    public TypedExpression EvaluateToTypedExpression(Expression expression, HashSet<string> inputParametersNames = null)
    {
      return VisitExpression(Evaluator.PartialEval(expression, inputParametersNames));
    }

    private TypedExpression VisitExpression(Expression exp, KDPgValueType expectedType = null)
    {
      switch (exp) {
        case MemberExpression me:
          var pi = VisitPath(me);
          if (_options.ExpandBooleans && me.Type == typeof(bool)) // for cases like (x => x.BoolValue)
            return ExpressionBuilders.Eq(pi.Expression, TypedExpression.FromValue(true));

          return pi.Expression;

        case ConstantExpression me:
          var pgValue = PgTypesConverter.ConvertObjectToPgValue(me.Value);
          pgValue = AdjustType(pgValue, expectedType);
          return new TypedExpression(RawQuery.Create(pgValue), pgValue.Type);

        case UnaryExpression un:
          var val = VisitExpression(un.Operand);

          switch (un.NodeType) {
            case ExpressionType.Convert:
              if (_options.ExpandBooleans && un.Operand is MemberExpression && un.Operand.Type == typeof(bool)) // for cases like (x => x.BoolValue)
                return ExpressionBuilders.Eq(val, TypedExpression.FromValue(true));

              if (un.Type == typeof(object)) // not important conversion (eg due to Func<Model,object> expression)
                return val;

              var targetType = un.Type;
              if (targetType.IsNullable()) targetType = targetType.GetNullableInnerType();

              var pgTargetType = PgTypesConverter.CreatePgValueTypeFromObjectType(targetType);
              if (pgTargetType == val.Type) return val;

              return ExpressionBuilders.Cast(val, pgTargetType);

            case ExpressionType.Not: return ExpressionBuilders.Not(val);
            case ExpressionType.ArrayLength: return ExpressionBuilders.ArrayLength(val);
            default: throw new Exception($"unknown operator: {un.NodeType}");
          }

        case BinaryExpression be:
          TypedExpression left = VisitExpression(be.Left);
          TypedExpression right = VisitExpression(be.Right, expectedType: left.Type);

          return be.NodeType switch {
              ExpressionType.Equal => ExpressionBuilders.Eq(left, right),
              ExpressionType.NotEqual => ExpressionBuilders.NotEq(left, right),
              ExpressionType.Add => ExpressionBuilders.Add(left, right),
              ExpressionType.Subtract => ExpressionBuilders.Subtract(left, right),
              ExpressionType.Multiply => ExpressionBuilders.Multiply(left, right),
              ExpressionType.Divide => ExpressionBuilders.Divide(left, right),
              ExpressionType.AndAlso => ExpressionBuilders.And(new[] { left, right }),
              ExpressionType.OrElse => ExpressionBuilders.Or(new[] { left, right }),
              ExpressionType.LessThan => ExpressionBuilders.LessThan(left, right),
              ExpressionType.LessThanOrEqual => ExpressionBuilders.LessThanEqual(left, right),
              ExpressionType.GreaterThan => ExpressionBuilders.GreaterThan(left, right),
              ExpressionType.GreaterThanOrEqual => ExpressionBuilders.GreaterThanEqual(left, right),
              _ => throw new Exception($"unknown operator: {be.NodeType}")
          };

        case MethodCallExpression call:
          var methodEntry = Database.FindMethod(call);

          if (methodEntry != null) {
            return methodEntry.Process(call, x => VisitExpression(x));
          }
          // List
          else if (call.Method.Name == "Contains") {
            TypedExpression callObject = VisitExpression(call.Object);
            TypedExpression arg1 = VisitExpression(call.Arguments[0]);
            return ExpressionBuilders.Contains(callObject, arg1);
          }
          // Native accessors
          else if (call.Method.Name == "get_Item") {
            TypedExpression callObject = VisitExpression(call.Object);
            TypedExpression arg1 = VisitExpression(call.Arguments[0]);

            RawQuery rq = new RawQuery();
            rq.AppendSurround(callObject.RawQuery);
            rq.Append("->");
            rq.Append(arg1.RawQuery);

            return new TypedExpression(rq, KDPgValueTypeInstances.Json);
          }
          // Extension methods
          else if (call.Method.Name == "PgIn") {
            TypedExpression extensionObject = VisitExpression(call.Arguments[0]);
            return GetConstant(call.Arguments[1]) switch {
                ISelectSubquery subquery => ExpressionBuilders.In(extensionObject, subquery.GetTypedExpression()),
                IEnumerable enumerable => ExpressionBuilders.In(extensionObject, enumerable),
                _ => throw new ArgumentException("Invalid value passed to PgIn method"),
            };
          }
          else if (call.Method.Name == "PgNotIn") {
            TypedExpression extensionObject = VisitExpression(call.Arguments[0]);
            return GetConstant(call.Arguments[1]) switch {
                ISelectSubquery subquery => ExpressionBuilders.NotIn(extensionObject, subquery.GetTypedExpression()),
                IEnumerable enumerable => ExpressionBuilders.NotIn(extensionObject, enumerable),
                _ => throw new ArgumentException("Invalid value passed to PgNotIn method"),
            };
          }
          else if (call.Method.Name == "PgContainsAny") {
            TypedExpression extensionObject = VisitExpression(call.Arguments[0]);
            TypedExpression arg1 = VisitExpression(call.Arguments[1]);
            return ExpressionBuilders.ContainsAny(extensionObject, arg1);
          }
          // PG funcs
          else {
            string methodName = call.Method.Name;

            Type funcType;
            if (call.Method.DeclaringType == typeof(Func))
              funcType = typeof(FuncInternal);
            else if (call.Method.DeclaringType == typeof(AggregateFunc))
              funcType = typeof(AggregateFuncInternal);
            else
              throw new Exception($"Unsupported method call ({call.Method})");

            if (methodName == "Raw") {
              var valueType = PgTypesConverter.CreatePgValueTypeFromObjectType(call.Method.ReturnType);
              var text = (string)((ConstantExpression)call.Arguments[0]).Value;
              return new TypedExpression(RawQuery.Create(text), valueType);
            }
            else {
              var internalMethod = funcType.GetMethod(methodName);
              if (internalMethod == null) throw new Exception($"invalid method: {call.Method.Name}");

              var passedArgsCount = call.Arguments.Count;
              var methodArgs = internalMethod.GetParameters();
              var methodArgsCount = methodArgs.Length;

              var args = call.Arguments.Zip(methodArgs)
                             .Select(x => {
                               var (value, parameter) = x;
                               return parameter.ParameterType == typeof(TypedExpression)
                                   ? VisitExpression(value)
                                   : GetConstant(value);
                             })
                             .Concat(Enumerable.Repeat(Type.Missing, Math.Max(0, methodArgsCount - passedArgsCount)))
                             .ToArray();

              return (TypedExpression)internalMethod.Invoke(null, args);
            }
          }

        default:
          throw new Exception($"Invalid node: {(exp?.NodeType.ToString() ?? "(null)")}");
      }
    }

    public PathInfo VisitPath<TModel>(Expression<Func<TModel, object>> exp)
    {
      return VisitPath(exp.Body);
    }

    public PathInfo VisitPath(Expression exp)
    {
      var pi = new PathInfo();
      KDPgValueType pathValueType = null;

      List<object> parts = new List<object>();

      // extract Body expression if Func expression was passed
      if (exp is LambdaExpression lm)
        exp = lm.Body;

      // remove Convert from last part
      if (exp is UnaryExpression un && un.NodeType == ExpressionType.Convert)
        exp = (MemberExpression)un.Operand;

      void Traverse(Expression innerExpression)
      {
        Expression parentExpression;
        switch (innerExpression) {
          case MemberExpression memberExpression:
            parentExpression = memberExpression.Expression;
            var member = (PropertyInfo)memberExpression.Member;

            Traverse(parentExpression);

            parts.Add(member);

            break;

          case MethodCallExpression callExpression:
            Debug.Assert(callExpression.Method.Name == "get_Item");
            parentExpression = callExpression.Object;
            var indexValue = GetConstant(callExpression.Arguments.First());

            Traverse(parentExpression);

            parts.Add(indexValue);

            break;

          case ParameterExpression parameterExpression: // lambda parameter
            pi.ParameterExp = parameterExpression;
            break;

          default:
            throw new Exception("invalid node");
        }
      }

      Traverse(exp);

      var rq = new RawQuery();

      string overrideTableName = null;
      for (var index = 0; index < parts.Count; index++) {
        var part = parts[index];
        var isLast = index == parts.Count - 1;
        
        switch (part) {
          case PropertyInfo member:

            // table
            if (ModelsRegistry.IsTable(member.PropertyType)) {
              if (parts.Count == 1) { // only process table if it is only part in the path (x => x.M1.Name)
                var table = ModelsRegistry.GetTable(member.PropertyType);

                rq.AppendTable(new RawQuery.TableNamePlaceholder(table, member.Name));

                pathValueType = null;
              }
              else {
                overrideTableName = member.Name;
              }
            }
            // column
            else if (ModelsRegistry.IsColumn(member)) {
              var column = ModelsRegistry.GetColumn(member);
              pi.Column = column;

              if (_options == null || _options.ParameterToTableAlias.Count == 0) {
                var tableVarName = overrideTableName ?? column.Table.Name;
                rq.AppendColumn(column, new RawQuery.TableNamePlaceholder(column.Table, tableVarName));
              }
              else {
                // var tableVarName = overrideTableName ?? pi.ParameterName;
                rq.AppendColumn(column, _options.ParameterToTableAlias[pi.ParameterExp]);
              }

              pathValueType = column.Type;
            }
            // json path
            else if (ModelsRegistry.IsJsonPropertyName(member)) {
              var fieldName = ModelsRegistry.GetJsonPropertyName(member);
              var fieldType = ModelsRegistry.GetJsonPropertyType(member);

              rq.Append(fieldType == KDPgValueTypeInstances.Json || !isLast ? "->" : "->>");
              rq.Append(EscapeUtils.EscapePostgresString(fieldName));
              pi.JsonPath.Add(fieldName);

              pathValueType = fieldType;
            }
            // property
            else if (member.MemberType == MemberTypes.Property) {
              if (member.Name == "Count" || member.Name == "Length") {
                var newRq = ExpressionBuilders.ArrayLength(new TypedExpression(rq, pathValueType));
                rq = newRq.RawQuery;
                pathValueType = newRq.Type;
              }
              else {
                var t = Database.FindProperty(member);

                if (t != null) {
                  var newRq = new TypedExpression(rq, pathValueType);
                  newRq = t.Processor(newRq);
                  rq = newRq.RawQuery;
                  pathValueType = newRq.Type;
                }
                else {
                  throw new Exception($"Unsupported function: {member.Name}");
                }
              }
            }
            else {
              throw new Exception("Unable to process path part");
            }

            break;

          case string jsonObjectProperty:
            rq.Append("->");
            rq.Append(EscapeUtils.EscapePostgresString(jsonObjectProperty));
            pi.JsonPath.Add(jsonObjectProperty);

            pathValueType = KDPgValueTypeInstances.Json;
            break;

          case int jsonArrayIndex:
            rq.Append("->");
            rq.Append(jsonArrayIndex.ToString());
            pi.JsonPath.Add(jsonArrayIndex);

            pathValueType = KDPgValueTypeInstances.Json;
            break;

          default:
            throw new Exception("invalid node");
        }
      }

      if (pi.JsonPath.Count > 0) {
        // cast to native type if known
        if (pathValueType != KDPgValueTypeInstances.Json && pathValueType.PostgresFetchType != "text") {
          RawQuery rq2 = new RawQuery();
          rq2.AppendSurround(rq);
          rq2.Append("::", pathValueType.PostgresFetchType);
          rq = rq2;
        }
      }


      pi.Expression = new TypedExpression(rq, pathValueType);

      return pi;
    }

    private static object GetConstant(Expression exp)
    {
      return exp switch {
          ConstantExpression constExp => constExp.Value,
          UnaryExpression unaryExp => unaryExp.NodeType switch {
              ExpressionType.Convert => ((ConstantExpression)unaryExp.Operand).Value,
              _ => throw new Exception($"Unknown operator: {unaryExp.NodeType}"),
          },
          _ => throw new Exception($"Invalid node: {(exp?.NodeType.ToString() ?? "(null)")}"),
      };
    }

    // Adjust type of PgValue instance for cases like comparing 'date' pg type type with C# DateTime instance or 'time' pg type with C# TimeSpan instance 
    private static PgValue AdjustType(PgValue pgValue, KDPgValueType expectedType)
    {
      if (expectedType == null)
        return pgValue;

      // 'time' and C# TimeSpan 
      if (expectedType == KDPgValueTypeInstances.Time && pgValue.Type == KDPgValueTypeInstances.Interval)
        return PgTypesConverter.ConvertToPgValue(expectedType, pgValue.Value);

      // 'date' and C# DateTime 
      if (expectedType == KDPgValueTypeInstances.Date && pgValue.Type == KDPgValueTypeInstances.DateTime)
        return PgTypesConverter.ConvertToPgValue(expectedType, pgValue.Value);

      return pgValue;
    }
  }
}