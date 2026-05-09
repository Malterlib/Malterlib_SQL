// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/PostgresClient>
#include <Mib/SQL/PostgresDatabase>
#include <Mib/Concurrency/AsyncDestroy>

#include "Test_Malterlib_SQL_DatabaseBackendShared.h"
#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"
#include "Test_Malterlib_SQL_PostgresDatabase_Parity.h"
#include "Test_Malterlib_SQL_PostgresShared.h"

using namespace NMib;
using namespace NMib::NContainer;
using namespace NMib::NConcurrency;
using namespace NMib::NSQL;
using namespace NMib::NSQL::NTest::NPostgres;
using namespace NMib::NStr;

namespace
{
	TCFuture<void> fg_ResetPostgresBackendTestSchema(TCActor<CPostgresClientActor> _Client)
	{
		NContainer::TCVector<CStr> TableNames;
		TableNames.f_InsertLast("people");
		TableNames.f_InsertLast("renamed_check_test");
		TableNames.f_InsertLast("changed_check_test");
		TableNames.f_InsertLast("value_types");
		TableNames.f_InsertLast("nullable_types");
		TableNames.f_InsertLast("default_values");
		TableNames.f_InsertLast("conversion_failures");
		TableNames.f_InsertLast("users");
		TableNames.f_InsertLast("sessions");
		TableNames.f_InsertLast("user_roles");
		TableNames.f_InsertLast("legacy_sessions");
		TableNames.f_InsertLast("sessions_renamed");
		TableNames.f_InsertLast("legacy_constrained_sessions");
		TableNames.f_InsertLast("constrained_sessions_renamed");
		TableNames.f_InsertLast("required_column_test");
		TableNames.f_InsertLast("rebuild_test");
		TableNames.f_InsertLast("__mib_rebuild_old_rebuild_test");
		TableNames.f_InsertLast("narrow_keys");
		TableNames.f_InsertLast("key_only");
		TableNames.f_InsertLast("backfill_test");
		TableNames.f_InsertLast("index_collision");
		TableNames.f_InsertLast("index_change");
		TableNames.f_InsertLast("rebuild_keep_child");
		TableNames.f_InsertLast("rebuild_keep_table_with_a_long_name_near_the_identifier_limit");
		TableNames.f_InsertLast("rebuild_keep_parent");
		TableNames.f_InsertLast("__mib_rebuild_old_rebuild_keep_parent");
		TableNames.f_InsertLast("rebuild_keep");
		TableNames.f_InsertLast("__mib_rebuild_old_rebuild_keep");
		TableNames.f_InsertLast("rebuild_rename_all");
		TableNames.f_InsertLast("__mib_rebuild_old_rebuild_rename_all");
		TableNames.f_InsertLast("rebuild_fk_item");
		TableNames.f_InsertLast("rebuild_fk_category");
		TableNames.f_InsertLast("unique_add_migration");
		TableNames.f_InsertLast("gen_col_test");
		TableNames.f_InsertLast("gen_col_upsert");
		TableNames.f_InsertLast("gen_col_migrate");
		TableNames.f_InsertLast("__mib_rebuild_old_gen_col_migrate");
		TableNames.f_InsertLast("adopt_constraint");
		TableNames.f_InsertLast("add_default_expr");
		TableNames.f_InsertLast("__mib_rebuild_old_add_default_expr");
		TableNames.f_InsertLast("drop_constraint_test");
		TableNames.f_InsertLast("drop_add_test");
		TableNames.f_InsertLast("__mib_rebuild_old_drop_add_test");
		TableNames.f_InsertLast("adopt_existing");
		TableNames.f_InsertLast("same_shape_a");
		TableNames.f_InsertLast("same_shape_b");
		TableNames.f_InsertLast("fk_child");
		TableNames.f_InsertLast("fk_parent");
		TableNames.f_InsertLast("self_ref");
		TableNames.f_InsertLast("indexed_people");
		TableNames.f_InsertLast("schema_migrations");

		for (CStr const &TableName : TableNames)
			co_await _Client(&CPostgresClientActor::f_Execute, "DROP TABLE IF EXISTS \"{}\" CASCADE"_f << TableName);

		co_return {};
	}

	TCFuture<void> fg_RunPostgresDatabaseIntegrationScenario(bool _bTLS)
	{
		DMibTestPath(_bTLS ? "runs typed backend tests with TLS" : "runs typed backend tests without TLS");
		co_await fg_WithPostgresTestServer
			(
				{
					.m_DirectoryName = _bTLS ? "PostgresDatabase_TLS" : "PostgresDatabase_NoTLS"
					, .m_bTLS = _bTLS
					, .m_MissingExecutableWarning = "Warning: Failed to find postgres executables, disabling database backend tests\n"
				}
				, g_ActorFunctor / [](CPostgresConnectionSettings Settings) -> TCFuture<void>
				{
					auto CaptureScope = co_await g_CaptureExceptions;

					TCActor<CPostgresClientActor> Client = fg_CreatePostgresClient();
					auto CleanupClient = co_await fg_AsyncDestroy(Client);
					co_await Client(&CPostgresClientActor::f_Connect, CPostgresConnectionSettings(Settings)).f_Timeout(gc_Timeout, "Timed out connecting PostgreSQL reset client");

					auto fCreateBackend = [Settings](ICSqlSchemaVersions const *_pSchemaVersions)
						{
							return fg_CreateDatabaseBackendPostgres
								(
									_pSchemaVersions
									, { .m_ConnectionSettings = CPostgresConnectionSettings(Settings) }
								)
							;
						}
					;

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Type capabilities") -> TCFuture<void>
					{
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions);
						CSqlDatabaseBackendCapabilities Capabilities = co_await Backend(&ICSqlDatabaseBackendActor::f_Capabilities);

						DMibExpect(Capabilities.m_bUUID, ==, true);
						DMibExpect(Capabilities.m_bDate, ==, true);
						DMibExpect(Capabilities.m_bTimeOfDay, ==, true);
						DMibExpect(Capabilities.m_bTimestamp, ==, true);
						DMibExpect(Capabilities.m_bTimestampTz, ==, true);
						DMibExpect(Capabilities.m_bInterval, ==, true);
						DMibExpect(Capabilities.m_bJSON, ==, true);
						DMibExpect(Capabilities.m_bJSONB, ==, true);
						DMibExpect(Capabilities.m_bArrays, ==, true);
						DMibExpect(Capabilities.m_bUnrecognizedBackend, ==, true);
						DMibExpect(Capabilities.m_bNumberedPlaceholders, ==, true);

						DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_UUID), ==, true);
						DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_Json), ==, true);
						DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_Array_Integer32), ==, true);
						DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_Array_UUID), ==, true);
						DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_UnrecognizedBackend), ==, true);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Basic typed database") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabase(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Transaction finish rejects queued operations") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto Transaction = co_await Database.f_BeginTransaction();
						auto BlockingQuery = Transaction.f_QueryRaw(NStr::CStr("SELECT pg_sleep(0.2)"));
						auto Commit = Transaction.f_Commit();
						auto InsertAfterCommit = co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>
							(
								NStr::CStr("postgres-queued-after-commit@example.com")
							)
							.f_Wrap()
						;

						DMibExpect(bool(InsertAfterCommit), ==, false);

						co_await fg_Move(BlockingQuery);
						co_await fg_Move(Commit);

						DMibExpect
							(
								co_await Database.template f_Exists<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("postgres-queued-after-commit@example.com"))
								, ==
								, false
							)
						;

						co_return {};
					};

					DMibTestCategory("Raw SQL failing in a read transaction keeps its error") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto Transaction = co_await Database.f_BeginReadTransaction();
						auto Result = co_await Transaction.f_QueryRaw(NStr::CStr("SELECT 1 / 0")).f_Wrap();
						DMibExpect(bool(Result), ==, false);
						if (!Result)
							DMibExpect(Result.f_GetExceptionStr().f_Find("division by zero"), >=, 0);

						co_await Transaction.f_Rollback();

						co_return {};
					};

					DMibTestCategory("Aliased selects on different connections are not conflated") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						// gc_SelectAliasReuseFirst and gc_SelectAliasReuseSecond alias the same expression into different members of
						// one result struct, so they share SQL and content QueryID but need distinct row mappings. Warm them on
						// DIFFERENT pooled connections - the first read transaction is held open while the second begins. The shared
						// backend cache hands the second connection the first description's entry by QueryID, so the warm step must
						// record THIS description's row mapping, or the second select decodes its value into the first's member.
						auto Row = NMib::NSQL::NTest::NDatabaseBackend::fg_TestValueTypesRow("alias-reuse-xconn@example.com");
						Row.m_Int32 = 7;
						Row.m_Int16 = 35;
						co_await Database.template f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertValueTypes>(fg_TempCopy(Row));

						int64 Expected = int64(Row.m_Int32) + int64(Row.m_Int16);

						auto FirstTransaction = co_await Database.f_BeginReadTransaction();
						auto FirstRow = co_await FirstTransaction.template f_QueryOne<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectAliasReuseFirst>
							(
								NStr::CStr("alias-reuse-xconn@example.com")
							)
						;
						DMibExpect(FirstRow.m_First, ==, Expected);
						DMibExpect(FirstRow.m_Second, ==, int64(-1));

						auto SecondTransaction = co_await Database.f_BeginReadTransaction();
						auto SecondRow = co_await SecondTransaction.template f_QueryOne<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectAliasReuseSecond>
							(
								NStr::CStr("alias-reuse-xconn@example.com")
							)
						;
						DMibExpect(SecondRow.m_Second, ==, Expected);
						DMibExpect(SecondRow.m_First, ==, int64(-1));

						// Run the second select again on the same connection. The first run prepared and cached this statement, so
						// this run resolves the per-pointer mapping the warm step recorded - which must be THIS description's, not
						// the first description's that fp_EnsurePreparedSelect returned by QueryID.
						auto SecondRowAgain = co_await SecondTransaction.template f_QueryOne<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectAliasReuseSecond>
							(
								NStr::CStr("alias-reuse-xconn@example.com")
							)
						;
						DMibExpect(SecondRowAgain.m_Second, ==, Expected);
						DMibExpect(SecondRowAgain.m_First, ==, int64(-1));

						co_await FirstTransaction.f_Commit();
						co_await SecondTransaction.f_Commit();

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Constrained rename migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRenameConstrainedMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Additive migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rename migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRenameMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRebuildMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild restores the table privileges exactly") -> TCFuture<void>
					{
						{
							CSqlDatabaseClient Version1Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_RebuildVersion1SchemaVersions));
							co_await Version1Database.f_Open();
							co_await Version1Database.f_ApplySchema();

							co_await Version1Database.f_ExecuteRaw(CStr("REVOKE ALL ON rebuild_test FROM PUBLIC"));
							co_await Version1Database.f_ExecuteRaw(CStr("GRANT SELECT ON rebuild_test TO PUBLIC"));
							co_await Version1Database.f_ExecuteRaw(CStr("GRANT UPDATE (token) ON rebuild_test TO PUBLIC"));
							co_await Version1Database.f_ExecuteRaw(CStr("ALTER DEFAULT PRIVILEGES GRANT INSERT ON TABLES TO PUBLIC"));
						}

						CSqlDatabaseClient Version2Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_RebuildSchemaVersions));
						co_await Version2Database.f_Open();
						auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
						co_await Version2Database.f_ExecuteRaw(CStr("ALTER DEFAULT PRIVILEGES REVOKE INSERT ON TABLES FROM PUBLIC"));
						DMibAssertTrue(bool(ApplyResult));

						auto Privileges = co_await Version2Database.f_QueryRaw
							(
								CStr
								(
									"SELECT has_table_privilege('public', 'rebuild_test', 'SELECT')::text AS select_value, "
									"has_table_privilege('public', 'rebuild_test', 'INSERT')::text AS insert_value, "
									"has_column_privilege('public', 'rebuild_test', 'token', 'UPDATE')::text AS update_value"
								)
							)
						;
						DMibExpect(Privileges.m_Rows[0].m_Values[0].f_GetAsType<CStr>(), ==, CStr("true"));
						DMibExpect(Privileges.m_Rows[0].m_Values[1].f_GetAsType<CStr>(), ==, CStr("false"));
						DMibExpect(Privileges.m_Rows[0].m_Values[2].f_GetAsType<CStr>(), ==, CStr("true"));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild preserves a user table sharing the scratch name") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRebuildScratchNameCollision(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild adds a foreign key to a table created in the same version") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRebuildAddsForeignKeyToNewTable(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Migration adds a unique column") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAddUniqueColumnMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Generated column") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseGeneratedColumn(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Generated column insert paths") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseGeneratedColumnInsertPaths(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Generated column rebuild migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseGeneratedColumnRebuildMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Narrow autoincrement key") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseNarrowAutoIncrementKey(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild keeps table guarantees") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRebuildKeepsTableGuarantees(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Adopt existing table adds missing constraint") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAdoptExistingTableConstraint(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Adopt existing table enforces a column constraint") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAdoptExistingColumnConstraint(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Renamed column keeps its CHECK") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRenameColumnWithCheck(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("CHECK changed under the same name is rejected") -> TCFuture<void>
					{
						{
							CSqlDatabaseClient Version1Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_ChangedCheckV1SchemaVersions));
							co_await Version1Database.f_Open();
							co_await Version1Database.f_ApplySchema();
						}

						CSqlDatabaseClient Version2Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_ChangedCheckSchemaVersions));
						co_await Version2Database.f_Open();

						auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
						DMibExpect(bool(ApplyResult), ==, false);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Index name used by a constraint index is rejected") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_IndexCollisionSchemaVersions));
						co_await Database.f_Open();

						auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
						DMibExpect(bool(ApplyResult), ==, false);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Raw DEALLOCATE ALL keeps ORM statements usable") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						for (umint i = 0; i < 3; ++i)
							co_await Database.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-deallocate-before@example.com"));

						co_await Database.f_ExecuteRaw(NStr::CStr("DEALLOCATE ALL"));

						for (umint i = 0; i < 3; ++i)
							co_await Database.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-deallocate-after@example.com"));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Raw COPY fails without stalling the connection pool") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto CopyIn = co_await Database.f_QueryRawStream(NStr::CStr("COPY people (email) FROM STDIN")).f_Wrap();
						DMibExpect(bool(CopyIn), ==, false);

						auto CopyInExecute = co_await Database.f_ExecuteRaw(NStr::CStr("COPY people (email) FROM STDIN")).f_Wrap();
						DMibExpect(bool(CopyInExecute), ==, false);

						auto CopyOut = co_await Database.f_QueryRaw(NStr::CStr("COPY (SELECT 1) TO STDOUT")).f_Wrap();
						DMibExpect(bool(CopyOut), ==, false);

						auto After = co_await Database.f_QueryRaw(NStr::CStr("SELECT 1::int8 AS value"));
						DMibExpect(After.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

						co_return {};
					};

					DMibTestCategory("Raw COMMIT AND CHAIN ends the transaction but ROLLBACK TO SAVEPOINT does not") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						{
							auto Transaction = co_await Database.f_BeginTransaction();
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-chain-savepoint-before@example.com"));
							co_await Transaction.f_ExecuteRaw(NStr::CStr("SAVEPOINT postgres_chain_savepoint"));
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-chain-savepoint-undone@example.com"));
							co_await Transaction.f_ExecuteRaw(NStr::CStr("ROLLBACK TO SAVEPOINT postgres_chain_savepoint"));
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-chain-savepoint-after@example.com"));
							co_await Transaction.f_Commit();
						}

						auto SavepointCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM people WHERE email LIKE 'postgres-chain-savepoint-%'"));
						DMibExpect(SavepointCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

						// The marker must not be undone by rolling back to a savepoint created before the first raw SQL
						{
							auto Transaction = co_await Database.f_BeginTransaction();
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-typed-savepoint-before@example.com"));
							NStr::CStr Savepoint = co_await Transaction.f_CreateSavepoint();
							co_await Transaction.f_ExecuteRaw("ROLLBACK TO SAVEPOINT {}"_f << Savepoint);
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-typed-savepoint-after@example.com"));
							co_await Transaction.f_Commit();
						}

						auto TypedSavepointCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM people WHERE email LIKE 'postgres-typed-savepoint-%'"));
						DMibExpect(TypedSavepointCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

						// The first raw statement can import a snapshot, which must precede any query
						{
							CSqlTransactionSettings Settings;
							Settings.m_Isolation = ESqlTransactionIsolation::mc_RepeatableRead;

							auto Exporting = co_await Database.f_BeginReadTransaction(Settings);
							auto Snapshot = co_await Exporting.f_QueryRaw(NStr::CStr("SELECT pg_export_snapshot()"));
							NStr::CStr SnapshotID = Snapshot.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>();

							auto Importing = co_await Database.f_BeginReadTransaction(Settings);
							auto ImportResult = co_await Importing.f_QueryRaw("SET TRANSACTION SNAPSHOT '{}'"_f << SnapshotID).f_Wrap();
							DMibExpect(bool(ImportResult), ==, true);

							co_await Importing.f_Commit();
							co_await Exporting.f_Commit();
						}

						{
							auto Transaction = co_await Database.f_BeginTransaction();
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-chain-committed@example.com"));
							co_await Transaction.f_ExecuteRaw(NStr::CStr("COMMIT AND CHAIN"));

							auto AfterChain = co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-chain-after@example.com"))
								.f_Wrap()
							;
							DMibExpect(bool(AfterChain), ==, false);
						}

						auto ChainCount = co_await Database.f_QueryRaw
							(
								NStr::CStr("SELECT count(*) AS value FROM people WHERE email LIKE 'postgres-chain-%' AND email NOT LIKE 'postgres-chain-savepoint-%'")
							)
						;
						DMibExpect(ChainCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

						co_return {};
					};

					DMibTestCategory("Raw DEALLOCATE ALL in a transaction keeps ORM statements usable") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto Transaction = co_await Database.f_BeginTransaction();
						co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-transaction-deallocate-before@example.com"));

						co_await Transaction.f_ExecuteRaw(NStr::CStr("DEALLOCATE ALL"));

						co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-transaction-deallocate-after@example.com"));

						co_await Transaction.f_ExecuteRaw(NStr::CStr("DEALLOCATE ALL"));

						co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-transaction-deallocate-again@example.com"));

						co_await Transaction.f_ExecuteRaw(NStr::CStr("PREPARE postgres_transaction_dummy AS SELECT 1"));
						co_await Transaction.f_ExecuteRaw(NStr::CStr("DEALLOCATE postgres_transaction_dummy"));

						co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-transaction-deallocate-named@example.com"));
						co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-transaction-deallocate-named2@example.com"));
						co_await Transaction.f_Commit();

						auto Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM people WHERE email LIKE 'postgres-transaction-deallocate-%'"));
						DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(5));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Concurrent schema application") -> TCFuture<void>
					{
						CSqlDatabaseClient FirstDatabase(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						CSqlDatabaseClient SecondDatabase(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await FirstDatabase.f_Open();
						co_await SecondDatabase.f_Open();

						auto FirstApply = FirstDatabase.f_ApplySchema();
						auto SecondApply = SecondDatabase.f_ApplySchema();
						auto FirstResult = co_await fg_Move(FirstApply).f_Wrap();
						auto SecondResult = co_await fg_Move(SecondApply).f_Wrap();
						DMibExpect(bool(FirstResult), ==, true);
						DMibExpect(bool(SecondResult), ==, true);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Index definition change") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseIndexDefinitionChange(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Backfill of an added column") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseBackfillOfAddedColumn(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Add column with expression default") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAddColumnWithExpressionDefault(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Drop column recreates a reused constraint name") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseDropColumnReusedConstraint(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Drop column alongside an added column") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseDropColumnWithAddedColumn(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Same-shaped tables keep separate prepared statements") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseSameShapedTablePreparedCache(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Existing untracked table is adopted") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAdoptUntrackedExistingTable(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Raw stream without row description reuses the connection") -> TCFuture<void>
					{
						// With a single-connection pool the backend process id is stable as long as the connection is
						// reused. A raw stream over a statement with no row description (an UPDATE) drains cleanly to
						// ReadyForQuery, so it must return the connection to the pool instead of tearing it down and
						// reconnecting - which would change the backend process id.
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, { .m_ConnectionSettings = CPostgresConnectionSettings(Settings), .m_ConnectionPoolSize = 1 }
							)
						;
						CSqlDatabaseClient Database(Backend);
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto FirstPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(FirstPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 FirstPid = FirstPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						CSqlRawOperation StreamOperation;
						StreamOperation.m_Sql = "UPDATE people SET email = email WHERE false";
						auto StreamResult = co_await Database.f_QueryRawStream(fg_Move(StreamOperation)).f_Wrap();
						DMibExpect(bool(StreamResult), ==, false);

						auto SecondPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(SecondPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 SecondPid = SecondPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						DMibExpect(SecondPid, ==, FirstPid);

						co_return {};
					};

					DMibTestCategory("Raw stream BEGIN does not pool an open-transaction connection") -> TCFuture<void>
					{
						// A raw BEGIN routed through the stream API drains to ReadyForQuery with no row description but leaves the
						// session inside a transaction. With a single-connection pool the backend pid stays stable while a connection
						// is reused; pooling the open-transaction connection (the bug) keeps the pid, while the fix tears it down and
						// reconnects, changing the pid. A changed pid proves the open transaction was not returned to the pool.
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, { .m_ConnectionSettings = CPostgresConnectionSettings(Settings), .m_ConnectionPoolSize = 1 }
							)
						;
						CSqlDatabaseClient Database(Backend);
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto FirstPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(FirstPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 FirstPid = FirstPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						CSqlRawOperation StreamOperation;
						StreamOperation.m_Sql = "BEGIN";
						auto StreamResult = co_await Database.f_QueryRawStream(fg_Move(StreamOperation)).f_Wrap();
						DMibExpect(bool(StreamResult), ==, false);

						auto SecondPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(SecondPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 SecondPid = SecondPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						DMibExpect(SecondPid, !=, FirstPid);

						co_return {};
					};

					DMibTestCategory("Raw transaction-control does not pool an open-transaction connection") -> TCFuture<void>
					{
						// A raw BEGIN leaves the session in a transaction. With a single-connection pool the backend pid is stable
						// while a connection is reused. Pooling the open-transaction connection (the bug) would hand it to the next
						// query unchanged, keeping the pid the same; the fix tears the session down and reconnects, changing the pid.
						// A changed pid proves the in-transaction connection was not returned to the pool for unrelated work.
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, { .m_ConnectionSettings = CPostgresConnectionSettings(Settings), .m_ConnectionPoolSize = 1 }
							)
						;
						CSqlDatabaseClient Database(Backend);
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto FirstPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(FirstPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 FirstPid = FirstPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						co_await Database.f_ExecuteRaw(NStr::CStr("BEGIN"));

						auto SecondPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(SecondPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 SecondPid = SecondPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						DMibExpect(SecondPid, !=, FirstPid);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Referenced table rebuild migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseForeignKeyRebuildMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Forward foreign-key reference schema") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseForeignKeyOrderingSchema(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Self-referential foreign-key rebuild migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseSelfReferentialForeignKeyRebuildMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Drop column migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseDropColumnMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Transform migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseTransformMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Unknown version validation") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseUnknownVersionValidation(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Checksum validation") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseChecksumValidation(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Required column validation") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRequiredColumnValidation(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Index migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseIndexMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Abandoned select stream") -> TCFuture<void>
					{
						CSqlDatabaseClient Database
							(
								fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions)
							)
						;
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						for (uint64 i = 0; i < 129; ++i)
						{
							NMib::NSQL::NTest::NDatabaseBackend::CPersonRow Person;
							Person.m_Email = "abandoned-stream@example.com";
							co_await Database.f_Insert(NMib::NSQL::NTest::NDatabaseBackend::gc_PersonTable, fg_Move(Person));
						}

						{
							auto Rows = Database.f_Query<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("abandoned-stream@example.com"));
							auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator();
							DMibExpect(bool(iBatch), ==, true);
						}

						NMib::NSQL::NTest::NDatabaseBackend::CPersonRow Person;
						Person.m_Email = "after-abandoned-stream@example.com";
						co_await Database.f_Insert(NMib::NSQL::NTest::NDatabaseBackend::gc_PersonTable, fg_Move(Person));

						auto Rows = Database.f_Query<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("after-abandoned-stream@example.com"));
						umint nRows = 0;
						for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
						{
							for (auto const &pRow : *iBatch)
							{
								++nRows;
								DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("after-abandoned-stream@example.com"));
							}
						}

						DMibExpect(nRows, ==, umint(1));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("PostgreSQL parity") -> TCFuture<void>
					{
						CSqlDatabaseClient Database
							(
								fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions)
							)
						;
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						co_await NMib::NSQL::NTest::NPostgresDatabase::fg_RunPostgresParityTests(&Database);

						co_return {};
					};

					co_await fg_Move(Client).f_Destroy();
					CleanupClient.f_Clear();

					co_return {};
				}
			)
		;

		co_return {};
	}
}

struct CPostgresDatabase_Tests : public NMib::NTest::CTest
{
	void f_DoTests()
	{
		DMibTestSuite("Postgres rejects zero-sized connection pool") -> TCFuture<void>
		{
			CPostgresDatabaseBackendSettings Settings;
			Settings.m_ConnectionPoolSize = 0;

			CSqlDatabaseClient Database(fg_CreateDatabaseBackendPostgres(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings)));
			auto OpenResult = co_await Database.f_Open().f_Wrap();

			DMibExpectException
				(
					OpenResult.f_Access()
					, DMibErrorDatabaseInstance("PostgreSQL connection pool size must be at least 1")
				)
			;

			co_return {};
		};

		DMibTestSuite("Postgres database backend integration no TLS") -> TCFuture<void>
		{
			co_await fg_RunPostgresDatabaseIntegrationScenario(false);

			co_return {};
		};

		DMibTestSuite("Postgres database backend integration TLS") -> TCFuture<void>
		{
			co_await fg_RunPostgresDatabaseIntegrationScenario(true);

			co_return {};
		};
	}
};

DMibTestRegister(CPostgresDatabase_Tests, Malterlib::SQL);
