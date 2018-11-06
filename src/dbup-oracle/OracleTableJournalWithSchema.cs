using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using System;

namespace DbUp.Oracle
{
    /// <summary>
    /// Oracle Table Journal that allows defining schema for schemaversions table
    /// </summary>
    public class OracleTableJournalWithSchema : OracleTableJournal
    {
        #region Constructors

        /// <summary>
        /// Creates a new <see cref="OracleTableJournalWithSchema"/>
        /// </summary>
        /// <param name="connectionManager">The Oracle connection manager.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="table">The name of the journal table.</param>
        public OracleTableJournalWithSchema(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, String schema, String table) : base(connectionManager, logger, schema, table)
        {
            if (string.IsNullOrWhiteSpace(schema))
            {
                throw new ArgumentNullException(nameof(schema));
            }
        }

        #endregion

        #region Properties

        private String FqSchemaTableNameToUpper
        {
            get
            {
                return this.FqSchemaTableName.ToUpper(English);
            }
        }

        private String QuotedSchemaToUpper
        {
            get
            {
                return this.QuoteIdentifier(this.SchemaTableSchema).ToUpper(English);
            }
        }

        private String SchemaToUpper
        {
            get
            {
                return this.SchemaTableSchema.ToUpper(English);
            }
        }

        private String UnquotedSchemaTableNameToUpper
        {
            get
            {
                return this.UnquotedSchemaTableName.ToUpper(English);
            }
        }

        #endregion

        #region Methods

        public override String GetInsertJournalEntryScript(String scriptName)
        {
            return $"insert into {this.FqSchemaTableNameToUpper} (ScriptName, Applied) values ('" + scriptName.Replace("@", "") + "', sysdate)";
        }

        protected override String CreateSchemaTableSequenceSql()
        {
            return $@" CREATE SEQUENCE {this.QuotedSchemaToUpper}.{this.UnquotedSchemaTableNameToUpper}_sequence";
        }

        protected override String CreateSchemaTableSql(String quotedPrimaryKeyName)
        {
            return $@" CREATE TABLE {this.FqSchemaTableNameToUpper}
                    (
                        schemaversionid NUMBER(10),
                        scriptname VARCHAR2(255) NOT NULL,
                        applied TIMESTAMP NOT NULL,
                        CONSTRAINT PK_{this.UnquotedSchemaTableNameToUpper} PRIMARY KEY (schemaversionid)
                    )";
        }

        protected override String CreateSchemaTableTriggerSql()
        {
            return $@" CREATE OR REPLACE TRIGGER {$"{this.QuotedSchemaToUpper}.{this.UnquotedSchemaTableNameToUpper}"}_on_insert
                    BEFORE INSERT ON {this.FqSchemaTableNameToUpper}
                    FOR EACH ROW
                    BEGIN
                        SELECT {this.UnquotedSchemaTableNameToUpper}_sequence.nextval
                        INTO :new.schemaversionid
                        FROM dual;
                    END;
                ";
        }

        protected override String DoesTableExistSql()
        {
            return $"select 1 from all_tables where table_name = '{this.UnquotedSchemaTableNameToUpper}' and owner = '{this.SchemaToUpper}'";
        }

        protected override String GetInsertJournalEntrySql(String scriptName, String applied)
        {
            return $"insert into {this.FqSchemaTableNameToUpper} (ScriptName, Applied) values (:" + scriptName.Replace("@", "") + ",:" + applied.Replace("@", "") + ")";
        }

        protected override String GetJournalEntriesSql()
        {
            return $"select scriptname from {this.FqSchemaTableNameToUpper} order by scriptname";
        }

        #endregion
    }
}