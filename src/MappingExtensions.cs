using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.ProviderHelpers;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{
    public static class MappingExtensions
    {
        public static bool IsKeyColumnExists(this IEnumerable<ColumnMapping> mappings)
        {
            bool isPrimaryKeyColumnExists = GetKeyColumnMappings(mappings).Any();
            if (!isPrimaryKeyColumnExists)
                isPrimaryKeyColumnExists = mappings.Any(cm => cm.Active && ((SqlColumn)cm.DestinationColumn).IsPrimaryKey);
            return isPrimaryKeyColumnExists;
        }

        public static IEnumerable<ColumnMapping> GetKeyColumnMappings(this IEnumerable<ColumnMapping> mappings)
        {
            return mappings.Where(cm => cm != null && cm.DestinationColumn != null && cm.Active && cm.IsKey);
        }

        public static bool IsKeyColumn(this Column column, IEnumerable<ColumnMapping> mappings)
        {
            var keyColumnMappings = GetKeyColumnMappings(mappings);
            if (keyColumnMappings.Any())
            {
                return keyColumnMappings.Any(cm => cm.DestinationColumn == column);
            }
            return column.IsPrimaryKey;
        }

        public static string GetConditionalsSql(out List<SqlParameter> parameters, MappingConditionalCollection conditionals, bool skipVirtualColumns, bool useDestinationColumns)
        {
            string conditionalsSql = string.Empty;
            int conditionalCount = 0;
            parameters = new List<SqlParameter>();

            foreach (MappingConditional conditional in conditionals)
            {
                if (skipVirtualColumns && conditional.SourceColumn != null && conditional.Mapping != null && conditional.Mapping.DestinationTable != null
                    && conditional.Mapping.DestinationTable.Columns.Where(c => c.IsNew).Any(virtualColumn => string.Compare(virtualColumn.Name, conditional.SourceColumn.Name, true) == 0))
                {
                    continue;
                }
                string columnName = conditional.SourceColumn.Name;
                if (useDestinationColumns)
                {
                    ColumnMapping columnMapping = conditional.Mapping.GetColumnMappings().FirstOrDefault(cm => cm != null && cm.SourceColumn != null && string.Compare(cm.SourceColumn.Name, conditional.SourceColumn.Name, true) == 0);
                    if (columnMapping != null && columnMapping.DestinationColumn != null)
                    {
                        columnName = columnMapping.DestinationColumn.Name;
                    }
                    else
                    {
                        continue;
                    }
                }
                switch (conditional.ConditionalOperator)
                {
                    case ConditionalOperator.Contains:
                        if (!string.IsNullOrEmpty(conditional.Condition))
                        {
                            conditionalsSql = string.Format(
                                "{0}[{1}] LIKE '%{2}%' and ",
                                conditionalsSql,
                                columnName,
                                conditional.Condition
                            );
                        }
                        continue;
                    case ConditionalOperator.NotContains:
                        if (!string.IsNullOrEmpty(conditional.Condition))
                        {
                            conditionalsSql = string.Format(
                                "{0}[{1}] NOT LIKE '%{2}%' and ",
                                conditionalsSql,
                                columnName,
                                conditional.Condition
                            );
                        }
                        continue;

                    case ConditionalOperator.EqualTo:
                        if (conditional.IsNullStringCondition)
                        {
                            conditionalsSql = string.Format("{0}[{1}]] IS NULL and ", conditionalsSql, columnName);
                            continue;
                        }
                        else if (conditional.IsNullOrEmptyStringCondition)
                        {
                            conditionalsSql = string.Format("{0}[{1}] IS NULL OR [{2}] = '' and ", conditionalsSql, columnName, columnName);
                            continue;
                        }
                        else
                        {
                            conditionalsSql = string.Format(
                                "{0}[{1}] = @conditional{2} and ",
                                conditionalsSql, columnName, conditionalCount
                            );
                        }
                        break;
                    case ConditionalOperator.DifferentFrom:
                        if (conditional.IsNullStringCondition)
                        {
                            conditionalsSql = string.Format("{0}[{1}] IS NOT NULL and ", conditionalsSql, columnName);
                            continue;
                        }
                        else if (conditional.IsNullOrEmptyStringCondition)
                        {
                            conditionalsSql = string.Format("{0}[{1}] IS NOT NULL AND [{1}] <> '' and ", conditionalsSql, columnName);
                            continue;
                        }
                        else
                        {
                            conditionalsSql = string.Format("{0}[{1}] <> @conditional{2} and ", conditionalsSql, columnName, conditionalCount);
                        }
                        break;
                    case ConditionalOperator.In:
                        var conditionalValue = conditional.Condition;
                        if (!string.IsNullOrEmpty(conditionalValue))
                        {
                            if (conditional.SourceColumn.Type == typeof(string))
                            {
                                conditionalValue = string.Join(",", conditionalValue.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(val => $"'{val}'"));
                            }
                            conditionalsSql = string.Format(
                                "{0}[{1}] IN ({2}) and ",
                                conditionalsSql,
                                columnName,
                                conditionalValue
                            );
                        }
                        continue;
                    case ConditionalOperator.NotIn:
                        var notInConditionalValue = conditional.Condition;
                        if (!string.IsNullOrEmpty(notInConditionalValue))
                        {
                            if (conditional.SourceColumn.Type == typeof(string))
                            {
                                notInConditionalValue = string.Join(",", notInConditionalValue.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(val => $"'{val}'"));
                            }
                            conditionalsSql = string.Format(
                                "{0}[{1}] NOT IN ({2}) and ",
                                conditionalsSql,
                                columnName,
                                notInConditionalValue
                            );
                        }
                        continue;
                    case ConditionalOperator.StartsWith:
                        if (!string.IsNullOrEmpty(conditional.Condition))
                        {
                            conditionalsSql = string.Format(
                                "{0}[{1}] LIKE '{2}%' and ",
                                conditionalsSql,
                                columnName,
                                conditional.Condition
                            );
                        }
                        continue;
                    case ConditionalOperator.NotStartsWith:
                        if (!string.IsNullOrEmpty(conditional.Condition))
                        {
                            conditionalsSql = string.Format(
                                "{0}[{1}] NOT LIKE '{2}%' and ",
                                conditionalsSql,
                                columnName,
                                conditional.Condition
                            );
                        }
                        continue;
                    case ConditionalOperator.EndsWith:
                        if (!string.IsNullOrEmpty(conditional.Condition))
                        {
                            conditionalsSql = string.Format(
                                "{0}[{1}] LIKE '%{2}' and ",
                                conditionalsSql,
                                columnName,
                                conditional.Condition
                            );
                        }
                        continue;
                    case ConditionalOperator.NotEndsWith:
                        if (!string.IsNullOrEmpty(conditional.Condition))
                        {
                            conditionalsSql = string.Format(
                                "{0}[{1}] NOT LIKE '%{2}' and ",
                                conditionalsSql,
                                columnName,
                                conditional.Condition
                            );
                        }
                        continue;

                    default:
                        conditionalsSql = string.Format(
                            "{0}[{1}] = @conditional{2} and ",
                            conditionalsSql,
                            columnName,
                            conditionalCount
                        );
                        break;
                }

                if (conditional.SourceColumn.Type == typeof(DateTime))
                {
                    parameters.Add(new SqlParameter("@conditional" + conditionalCount, DateTime.Parse(conditional.Condition)));
                }
                else
                {
                    parameters.Add(new SqlParameter("@conditional" + conditionalCount, conditional.Condition));
                }

                conditionalCount = conditionalCount + 1;
            }

            return conditionalsSql;
        }
    }
}
