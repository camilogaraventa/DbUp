﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using DbUp.ScriptProviders;

namespace DbUp.Journal
{
    /// <summary>
    /// An implementation of the <see cref="IJournal"/> interface which tracks version numbers for a 
    /// SQL Server database using a table called dbo.SchemaVersions.
    /// </summary>
    public sealed class TableJournal : IJournal
    {
        private readonly string tableName;
        private readonly string schemaTableName;
        private readonly string dbConnectionString;
        private readonly ILog   log;
        private readonly string schema;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableJournal"/> class.
        /// </summary>
        /// <param name="targetDbConnectionString">The connection to the target database.</param>
        /// <example>
        /// var journal = new TableJournal("Server=server;Database=database;Trusted_Connection=True");
        /// </example>
        public TableJournal(string targetDbConnectionString)
            : this(targetDbConnectionString, "dbo", "SchemaVersions", new ConsoleLog())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableJournal"/> class.
        /// </summary>
        /// <param name="targetDbConnectionString">The connection to the target database.</param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <example>
        /// var journal = new TableJournal("Server=server;Database=database;Trusted_Connection=True", "dbo");
        /// </example>
        public TableJournal(string targetDbConnectionString, string schema)
            : this(targetDbConnectionString, schema, "SchemaVersions", new ConsoleLog())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableJournal"/> class.
        /// </summary>
        /// <param name="targetDbConnectionString">The connection to the target database.</param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <param name="table">The table name.</param>
        /// <param name="logger">The log.</param>
        /// <example>
        /// var journal = new TableJournal("Server=server;Database=database;Trusted_Connection=True", "dbo", "MyVersionTable");
        /// </example>
        public TableJournal(string targetDbConnectionString, string schema, string table, ILog logger)
        {
            dbConnectionString = targetDbConnectionString;
            tableName = table;
            this.schema = schema;
            schemaTableName = schema + "." + tableName;
            log = logger;
        }

        /// <summary>
        /// Recalls the version number of the database.
        /// </summary>
        /// <returns>All executed scripts.</returns>
        public string[] GetExecutedScripts()
        {
            log.WriteInformation("Fetching list of already executed scripts.");
            var exists = DoesTableExist(dbConnectionString);
            if (!exists)
            {
                log.WriteInformation(string.Format("The {0} table could not be found. The database is assumed to be at version 0.", schemaTableName));
                return new string[0];
            }

            var scripts = new List<string>();
            using (var connection = new SqlConnection(dbConnectionString))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = string.Format("select [ScriptName] from {0} order by [ScriptName]", schemaTableName);
                command.CommandType = CommandType.Text;
                connection.Open();

                using(var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                        scripts.Add((string) reader[0]);
                }
            }
            return scripts.ToArray();
        }

        /// <summary>
        /// Records a database upgrade for a database specified in a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        public void StoreExecutedScript(SqlScript script)
        {
            var exists = DoesTableExist(dbConnectionString);
            if (!exists)
            {
                log.WriteInformation(string.Format("Creating the {0} table", schemaTableName));

                using (var connection = new SqlConnection(dbConnectionString))
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = string.Format(
@"create table {0} (
	[Id] int identity(1,1) not null constraint PK_SchemaVersions_Id primary key nonclustered ,
	[ScriptName] nvarchar(255) not null,
	[Applied] datetime not null
)", schemaTableName);

                    command.CommandType = CommandType.Text;
                    connection.Open();

                    command.ExecuteNonQuery();
                }

                log.WriteInformation(string.Format("The {0} table has been created", schemaTableName));
            }


            using (var connection = new SqlConnection(dbConnectionString))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = string.Format("insert into {0} (ScriptName, Applied) values (@scriptName, (getutcdate()))", schemaTableName);
                command.Parameters.AddWithValue("scriptName", script.Name);

                command.CommandType = CommandType.Text;
                connection.Open();

                command.ExecuteNonQuery();
            }
        }

        private bool DoesTableExist(string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = string.Format(
@"select count(*)
from sys.objects 
inner join sys.schemas on objects.schema_id = schemas.schema_id
where type='U' and objects.name = '{0}' and schemas.name = '{1}'", tableName, schema);
                    command.CommandType = CommandType.Text;
                    connection.Open();

                    int result;
                    int.TryParse(command.ExecuteScalar().ToString(), out result);

                    return result != 0;
                }
            }

        }
    }
}