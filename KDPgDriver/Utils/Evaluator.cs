using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace KDPgDriver.Utils
{
  public static class Evaluator
  {
    public static Expression PartialEval(Expression expression, string inputParameterName = null)
    {
      var inpParams = inputParameterName == null ? null : new HashSet<string> { inputParameterName };
      return PartialEval(expression, inpParams);
    }

    public static Expression PartialEval(Expression expression, HashSet<string> inputParametersNames)
    {
      return new SubtreeEvaluator(new Nominator(inputParametersNames).Nominate(expression)).Eval(expression);
    }

    /// <summary>
    /// Evaluates & replaces sub-trees when first candidate is reached (top-down)
    /// </summary>
    private class SubtreeEvaluator : ExpressionVisitor
    {
      private readonly HashSet<Expression> _candidates;

      internal SubtreeEvaluator(HashSet<Expression> candidates)
      {
        _candidates = candidates;
      }

      internal Expression Eval(Expression exp)
      {
        return Visit(exp);
      }

      public override Expression Visit(Expression exp)
      {
        if (exp == null)
          return null;
        if (_candidates.Contains(exp))
          return Evaluate(exp);
        return base.Visit(exp);
      }

      private Expression Evaluate(Expression e)
      {
        if (e.NodeType == ExpressionType.Constant)
          return e;
        LambdaExpression lambda = Expression.Lambda(e);
        Delegate fn = lambda.Compile();
        return Expression.Constant(fn.DynamicInvoke(null), e.Type);
      }
    }

    /// <summary>
    /// Performs bottom-up analysis to determine which nodes can possibly
    /// be part of an evaluated sub-tree.
    /// </summary>
    private class Nominator : ExpressionVisitor
    {
      private readonly HashSet<string> _inputParameterName;

      HashSet<Expression> candidates;
      bool cannotBeEvaluated;

      public Nominator(HashSet<string> inputParameterName = null)
      {
        _inputParameterName = inputParameterName;
      }

      internal HashSet<Expression> Nominate(Expression expression)
      {
        candidates = new HashSet<Expression>();
        Visit(expression);
        return candidates;
      }

      private bool CanBeEvaluatedLocally(Expression expression)
      {
        if (expression is MethodCallExpression callExpression) {
          // Do not evaluate Func.* calls
          if (callExpression.Method.DeclaringType == typeof(Func))
            return false;
        }

        if (expression is ParameterExpression parameterExpression) {
          if (_inputParameterName == null) {
            return false;
          }
          else {
            return !_inputParameterName.Contains(parameterExpression.Name);
            // return parameterExpression.Name != _inputParameterName;
          }
        }
        else {
          return true;
        }

        // return expression.NodeType != ExpressionType.Parameter;
      }

      public override Expression Visit(Expression expression)
      {
        if (expression == null)
          return null;

        bool saveCannotBeEvaluated = cannotBeEvaluated;
        cannotBeEvaluated = false;
        base.Visit(expression);
        if (!cannotBeEvaluated) {
          if (CanBeEvaluatedLocally(expression)) {
            candidates.Add(expression);
          }
          else {
            cannotBeEvaluated = true;
          }
        }

        cannotBeEvaluated |= saveCannotBeEvaluated;
        return expression;
      }
    }
  }
}