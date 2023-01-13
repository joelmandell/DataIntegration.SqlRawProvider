using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.ProviderHelpers;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.SqlProvider
{    
    public static class MappingExtensions
    {
        // Obsoleted. Use DataIntegration ColumnMapping.HasScriptWithValue instead
        public static bool HasScriptWithValue(this ColumnMapping columnMapping)
        {
            return columnMapping.ScriptType == ScriptType.Constant || columnMapping.HasNewGuidScript();
        }

        // Obsoleted. Use DataIntegration ScriptType.NewGuid instead
        public static bool HasNewGuidScript(this ColumnMapping columnMapping)
        {
            return Enum.IsDefined(typeof(ScriptType), "NewGuid") && (int)columnMapping.ScriptType == 4;
        }

        // Obsoleted. Use DataIntegration ColumnMapping.GetScriptValue instead
        public static string GetScriptValue(this ColumnMapping columnMapping)
        {
            if (columnMapping.ScriptType == ScriptType.Constant)
                return columnMapping.ScriptValue;
            if (columnMapping.HasNewGuidScript())
                return Guid.NewGuid().ToString();
            return null;
        }

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
    }
}
