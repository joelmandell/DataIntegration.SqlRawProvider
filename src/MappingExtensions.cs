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

                conditionalsSql = GetConditionalSql(conditionalsSql, columnName, conditional, conditionalCount);

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

        public static string GetConditionalSql(string conditionalsSql, string columnName, MappingConditional mappingConditional, int conditionalCount)
        {
            switch (mappingConditional.ConditionalOperator)
            {
                case ConditionalOperator.Contains:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] LIKE '%{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.NotContains:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT LIKE '%{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.EqualTo:
                    if (mappingConditional.IsNullStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}]] IS NULL and ", conditionalsSql, columnName);
                        break;
                    }
                    else if (mappingConditional.IsNullOrEmptyStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}] IS NULL OR [{2}] = '' and ", conditionalsSql, columnName, columnName);
                        break;
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
                    if (mappingConditional.IsNullStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}] IS NOT NULL and ", conditionalsSql, columnName);
                        break;
                    }
                    else if (mappingConditional.IsNullOrEmptyStringCondition)
                    {
                        conditionalsSql = string.Format("{0}[{1}] IS NOT NULL AND [{1}] <> '' and ", conditionalsSql, columnName);
                        break;
                    }
                    else
                    {
                        conditionalsSql = string.Format("{0}[{1}] <> @conditional{2} and ", conditionalsSql, columnName, conditionalCount);
                    }
                    break;

                case ConditionalOperator.In:
                    var conditionalValue = mappingConditional.Condition;
                    if (!string.IsNullOrEmpty(conditionalValue))
                    {
                        if (mappingConditional.SourceColumn.Type == typeof(string))
                        {
                            conditionalValue = string.Join(",", conditionalValue.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(val => $"'{val.Trim()}'"));
                        }
                        conditionalsSql = string.Format(
                            "{0}[{1}] IN ({2}) and ",
                            conditionalsSql,
                            columnName,
                            conditionalValue
                        );
                    }
                    break;

                case ConditionalOperator.NotIn:
                    var notInConditionalValue = mappingConditional.Condition;
                    if (!string.IsNullOrEmpty(notInConditionalValue))
                    {
                        if (mappingConditional.SourceColumn.Type == typeof(string))
                        {
                            notInConditionalValue = string.Join(",", notInConditionalValue.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(val => $"'{val.Trim()}'"));
                        }
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT IN ({2}) and ",
                            conditionalsSql,
                            columnName,
                            notInConditionalValue
                        );
                    }
                    break;

                case ConditionalOperator.StartsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] LIKE '{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.NotStartsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT LIKE '{2}%' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.EndsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] LIKE '%{2}' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                case ConditionalOperator.NotEndsWith:
                    if (!string.IsNullOrEmpty(mappingConditional.Condition))
                    {
                        conditionalsSql = string.Format(
                            "{0}[{1}] NOT LIKE '%{2}' and ",
                            conditionalsSql,
                            columnName,
                            mappingConditional.Condition
                        );
                    }
                    break;

                default:
                    conditionalsSql = string.Format(
                        "{0}[{1}] = @conditional{2} and ",
                        conditionalsSql,
                        columnName,
                        conditionalCount
                    );
                    break;
            }

            return conditionalsSql;
        }

    }
}
