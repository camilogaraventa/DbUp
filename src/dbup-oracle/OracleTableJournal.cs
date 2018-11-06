using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;
using System;
using System.Data;
using System.Globalization;

namespace DbUp.Oracle
{
    public class OracleTableJournal : TableJournal
    {
        #region Members

        private static readonly CultureInfo english = new CultureInfo("en-US", false);
        private Boolean journalExists;

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new Oracle table journal.
        /// </summary>
        /// <param name="connectionManager">The Oracle connection manager.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="table">The name of the journal table.</param>
        public OracleTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, String schema, String table)
            : base(connectionManager, logger, new OracleObjectParser(), schema, table)
        {
        }

        #endregion

        #region Properties

        public static CultureInfo English => english;

        #endregion

        #region Methods

        public override void EnsureTableExistsAndIsLatestVersion(Func<IDbCommand> dbCommandFactory)
        {
            if (!this.journalExists && !this.DoesTableExist(dbCommandFactory))
            {
                this.Log().WriteInformation(string.Format("Creating the {0} table", this.FqSchemaTableName));

                // We will never change the schema of the initial table create.
                using (var command = this.GetCreateTableSequence(dbCommandFactory))
                {
                    command.ExecuteNonQuery();
                }

                // We will never change the schema of the initial table create.
                using (var command = this.GetCreateTableCommand(dbCommandFactory))
                {
                    command.ExecuteNonQuery();
                }

                // We will never change the schema of the initial table create.
                using (var command = this.GetCreateTableTrigger(dbCommandFactory))
                {
                    command.ExecuteNonQuery();
                }

                this.Log().WriteInformation(string.Format("The {0} table has been created", this.FqSchemaTableName));

                this.OnTableCreated(dbCommandFactory);
            }

            this.journalExists = true;
        }

        protected virtual String CreateSchemaTableSequenceSql()
        {
            var fqSchemaTableName = this.UnquotedSchemaTableName;
            return $@" CREATE SEQUENCE {fqSchemaTableName}_sequence";
        }

        protected override String CreateSchemaTableSql(String quotedPrimaryKeyName)
        {
            var fqSchemaTableName = this.UnquotedSchemaTableName;
            return
                $@" CREATE TABLE {fqSchemaTableName}
                (
                    schemaversionid NUMBER(10),
                    scriptname VARCHAR2(255) NOT NULL,
                    applied TIMESTAMP NOT NULL,
                    CONSTRAINT PK_{ fqSchemaTableName } PRIMARY KEY (schemaversionid)
                )";
        }

        protected virtual String CreateSchemaTableTriggerSql()
        {
            var fqSchemaTableName = this.UnquotedSchemaTableName;
            return $@" CREATE OR REPLACE TRIGGER {fqSchemaTableName}_on_insert
                    BEFORE INSERT ON {fqSchemaTableName}
                    FOR EACH ROW
                    BEGIN
                        SELECT {fqSchemaTableName}_sequence.nextval
                        INTO :new.schemaversionid
                        FROM dual;
                    END;
                ";
        }

        protected override String DoesTableExistSql()
        {
            var unquotedSchemaTableName = this.UnquotedSchemaTableName.ToUpper(English);
            return $"select 1 from user_tables where table_name = '{unquotedSchemaTableName}'";
        }

        protected IDbCommand GetCreateTableSequence(Func<IDbCommand> dbCommandFactory)
        {
            var command = dbCommandFactory();
            command.CommandText = this.CreateSchemaTableSequenceSql();
            command.CommandType = CommandType.Text;
            return command;
        }

        protected IDbCommand GetCreateTableTrigger(Func<IDbCommand> dbCommandFactory)
        {
            var command = dbCommandFactory();
            command.CommandText = this.CreateSchemaTableTriggerSql();
            command.CommandType = CommandType.Text;
            return command;
        }

        protected override String GetInsertJournalEntrySql(String scriptName, String applied)
        {
            var unquotedSchemaTableName = this.UnquotedSchemaTableName.ToUpper(English);
            return $"insert into {unquotedSchemaTableName} (ScriptName, Applied) values (:" + scriptName.Replace("@", "") + ",:" + applied.Replace("@", "") + ")";
        }

        protected override String GetJournalEntriesSql()
        {
            var unquotedSchemaTableName = this.UnquotedSchemaTableName.ToUpper(English);
            return $"select scriptname from {unquotedSchemaTableName} order by scriptname";
        }

        #endregion
    }
}