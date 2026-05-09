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
	struct CFkRetargetParentRow
	{
		int64 m_ID = 0;
	};

	struct CFkRetargetChildRow
	{
		int64 m_ID = 0;
		int64 m_ParentID = 0;
	};

	constexpr auto gc_FkRetargetATable = fg_SqlTable<CFkRetargetParentRow>
		(
			gc_Str<"fk_retarget_a">
			, fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &CFkRetargetParentRow::m_ID, ESqlColumnFlag::mc_PrimaryKey))
		)
	;

	constexpr auto gc_FkRetargetBTable = fg_SqlTable<CFkRetargetParentRow>
		(
			gc_Str<"fk_retarget_b">
			, fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &CFkRetargetParentRow::m_ID, ESqlColumnFlag::mc_PrimaryKey))
		)
	;

	// The child's foreign key keeps its name when it moves from fk_retarget_a to fk_retarget_b
	constexpr auto gc_FkRetargetChildV1Table = fg_SqlTable<CFkRetargetChildRow>
		(
			gc_Str<"fk_retarget_child">
			, fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &CFkRetargetChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey), fg_SqlColumn(gc_Str<"parent_id">, &CFkRetargetChildRow::m_ParentID))
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlForeignKey<&CFkRetargetChildRow::m_ParentID>(gc_Str<"fk_retarget_child_parent_fk">, fg_SqlReferences(gc_Str<"fk_retarget_a">, gc_Str<"id">)))
		)
	;

	constexpr auto gc_FkRetargetChildV2Table = fg_SqlTable<CFkRetargetChildRow>
		(
			gc_Str<"fk_retarget_child">
			, fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &CFkRetargetChildRow::m_ID, ESqlColumnFlag::mc_PrimaryKey), fg_SqlColumn(gc_Str<"parent_id">, &CFkRetargetChildRow::m_ParentID))
			, fg_SqlIndexes()
			, fg_SqlConstraints(fg_SqlForeignKey<&CFkRetargetChildRow::m_ParentID>(gc_Str<"fk_retarget_child_parent_fk">, fg_SqlReferences(gc_Str<"fk_retarget_b">, gc_Str<"id">)))
		)
	;

	constexpr auto gc_FkRetargetV1SchemaVersion = fg_SqlSchemaVersion
		(
			gc_Str<"fk_retarget_0001">
			, fg_SqlDatabase(gc_Str<"fk_retarget_database">, gc_FkRetargetATable, gc_FkRetargetBTable, gc_FkRetargetChildV1Table)
		)
	;
	constexpr auto gc_FkRetargetV2SchemaVersion = fg_SqlSchemaVersion
		(
			gc_Str<"fk_retarget_0002">
			, fg_SqlDatabase(gc_Str<"fk_retarget_database">, gc_FkRetargetATable, gc_FkRetargetBTable, gc_FkRetargetChildV2Table)
		)
	;
	constexpr auto gc_FkRetargetV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_FkRetargetV1SchemaVersion));
	constexpr auto gc_FkRetargetSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_FkRetargetV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_FkRetargetV2SchemaVersion, fg_SqlRebuildTable(gc_Str<"fk_retarget_a">))
		)
	;

	struct CReturningNarrowRow
	{
		int64 m_ID = 0;
		CStr m_Code;
		int8 m_Small = 0;
		CSqlDate m_Day;
		NTime::CTime m_At;
		TCSqlArray<CSqlDate> m_Days;
	};

	constexpr auto gc_ReturningNarrowTable = fg_SqlTable<CReturningNarrowRow>
		(
			gc_Str<"returning_narrow">
			, fg_SqlColumns
			(
				fg_SqlColumn(gc_Str<"id">, &CReturningNarrowRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(gc_Str<"code">, &CReturningNarrowRow::m_Code)
				, fg_SqlColumn(gc_Str<"small">, &CReturningNarrowRow::m_Small)
				, fg_SqlColumn(gc_Str<"day">, &CReturningNarrowRow::m_Day)
				, fg_SqlColumn(gc_Str<"at">, &CReturningNarrowRow::m_At)
				, fg_SqlColumn(gc_Str<"days">, &CReturningNarrowRow::m_Days)
			)
		)
	;
	constexpr auto gc_ReturningNarrowSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration
			(
				fg_SqlSchemaVersion(gc_Str<"returning_narrow_0001">, fg_SqlDatabase<ESqlDialect::mc_Postgres>(gc_Str<"returning_narrow_database">, gc_ReturningNarrowTable))
			)
		)
	;
	constexpr auto gc_DeleteReturningNarrowByCode = fg_SqlPreparedDelete(gc_ReturningNarrowTable).f_Where(fg_SqlParamEq<&CReturningNarrowRow::m_Code>());
	constexpr auto gc_UpdateReturningNarrowCodeByCode = fg_SqlPreparedUpdate(gc_ReturningNarrowTable)
		.f_Where(fg_SqlParamEq<&CReturningNarrowRow::m_Code>())
		.f_Set<&CReturningNarrowRow::m_Code>()
	;

	struct CUUIDValueRow
	{
		int64 m_ID = 0;
		NStorage::TCOptional<NCryptography::CUniversallyUniqueIdentifier> m_UUID;
	};

	constexpr auto gc_UUIDValueTable = fg_SqlTable<CUUIDValueRow>
		(
			gc_Str<"uuid_values">
			, fg_SqlColumns(fg_SqlColumn(gc_Str<"id">, &CUUIDValueRow::m_ID, ESqlColumnFlag::mc_PrimaryKey), fg_SqlColumn(gc_Str<"uuid">, &CUUIDValueRow::m_UUID))
		)
	;
	constexpr auto gc_UUIDValueSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(fg_SqlSchemaVersion(gc_Str<"uuid_values_0001">, fg_SqlDatabase<ESqlDialect::mc_Postgres>(gc_Str<"uuid_values_database">, gc_UUIDValueTable)))
		)
	;
	constexpr auto gc_SelectUUIDValueMinMax = fg_SqlPreparedSelect(gc_UUIDValueTable)
		.f_Where(CSqlAllRowsPredicate{})
		.f_Select(fg_SqlMin<&CUUIDValueRow::m_UUID>(), fg_SqlMax<&CUUIDValueRow::m_UUID>())
	;

	TCFuture<umint> fg_CountRawStreamRows(CSqlRawStream _Stream)
	{
		umint nRows = 0;
		for (auto iBatch = co_await fg_Move(_Stream.m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			nRows += (*iBatch).f_GetLen();

		co_return nRows;
	}

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
		TableNames.f_InsertLast("adopt_later_marker");
		TableNames.f_InsertLast("adopt_parent_codes");
		TableNames.f_InsertLast("adopt_fk_child");
		TableNames.f_InsertLast("adopt_fk_parent");
		TableNames.f_InsertLast("excluded");
		TableNames.f_InsertLast("adopt_nullable");
		TableNames.f_InsertLast("adopt_nullable_parent");
		TableNames.f_InsertLast("adopt_partitioned");
		TableNames.f_InsertLast("same_shape_a");
		TableNames.f_InsertLast("same_shape_b");
		TableNames.f_InsertLast("fk_partitioned_child");
		TableNames.f_InsertLast("fk_child");
		TableNames.f_InsertLast("fk_parent");
		TableNames.f_InsertLast("self_ref");
		TableNames.f_InsertLast("indexed_people");
		TableNames.f_InsertLast("drop_later");
		TableNames.f_InsertLast("__mib_rebuild_old_drop_later");
		TableNames.f_InsertLast("fk_retarget_child");
		TableNames.f_InsertLast("fk_retarget_a");
		TableNames.f_InsertLast("__mib_rebuild_old_fk_retarget_a");
		TableNames.f_InsertLast("fk_retarget_b");
		TableNames.f_InsertLast("returning_narrow");
		TableNames.f_InsertLast("checked_values");
		TableNames.f_InsertLast("uuid_values");
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
				, g_ActorFunctor / [](CPostgresConnectionSettings _Settings) -> TCFuture<void>
				{
					auto CaptureScope = co_await g_CaptureExceptions;

					TCActor<CPostgresClientActor> Client = fg_CreatePostgresClient();
					auto CleanupClient = co_await fg_AsyncDestroy(Client);
					co_await Client(&CPostgresClientActor::f_Connect, CPostgresConnectionSettings(_Settings)).f_Timeout(gc_Timeout, "Timed out connecting PostgreSQL reset client");

					auto fCreateBackend = [_Settings](ICSqlSchemaVersions const *_pSchemaVersions)
						{
							return fg_CreateDatabaseBackendPostgres(_pSchemaVersions, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings)});
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
						auto InsertAfterCommit = co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-queued-after-commit@example.com"))
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

						// Keep the first read transaction open so identical SQL with a different alias warms another pooled connection
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
					DMibTestCategory("Pooled statements are prepared again after a schema change") -> TCFuture<void>
					{
						{
							CSqlDatabaseClient Version1Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_Version1SchemaVersions));
							co_await Version1Database.f_Open();
							co_await Version1Database.f_ApplySchema();
						}

						CSqlDatabaseClient LatestDatabase
							(
								fg_CreateDatabaseBackendPostgres
								(
									&NMib::NSQL::NTest::NMigrations::gc_SchemaVersions
									, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings), .m_ConnectionPoolSize = 1}
								)
							)
						;
						co_await LatestDatabase.f_Open();

						auto Before = co_await LatestDatabase.f_QueryRaw(CStr("SELECT * FROM users"));
						DMibExpect(Before.m_Columns.f_GetLen(), ==, umint(3));

						co_await LatestDatabase.f_ApplySchema();

						auto After = co_await LatestDatabase.f_QueryRaw(CStr("SELECT * FROM users")).f_Wrap();
						DMibAssertTrue(bool(After));
						DMibExpect(After->m_Columns.f_GetLen(), ==, umint(4));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Declared table named like a materialized view fails the apply") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&gc_ReturningNarrowSchemaVersions));
						co_await Database.f_Open();

						co_await Database.f_ExecuteRaw(CStr("CREATE MATERIALIZED VIEW returning_narrow AS SELECT 1::int8 AS id"));
						auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
						co_await Database.f_ExecuteRaw(CStr("DROP MATERIALIZED VIEW returning_narrow"));

						DMibAssertFalse(bool(ApplyResult));
						DMibExpectTrue(ApplyResult.f_GetExceptionStr().f_Find("already exists") >= 0);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("MIN and MAX of a UUID member") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&gc_UUIDValueSchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto Empty = co_await Database.f_QueryOne<gc_SelectUUIDValueMinMax>();
						DMibExpect(bool(fg_Get<0>(Empty)), ==, false);
						DMibExpect(bool(fg_Get<1>(Empty)), ==, false);

						co_await Database.f_ExecuteRaw
							(
								CStr
								(
									"INSERT INTO uuid_values (id, uuid) VALUES (1, '7fffffff-0000-0000-0000-000000000000'), "
									"(2, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), (3, '00000000-0000-0000-0000-0000000000ff')"
								)
							)
						;

						auto MinMax = co_await Database.f_QueryOne<gc_SelectUUIDValueMinMax>();
						DMibAssertTrue(bool(fg_Get<0>(MinMax)));
						DMibExpect(*fg_Get<0>(MinMax), ==, NCryptography::CUniversallyUniqueIdentifier(0x00000000u, 0x0000, 0x0000, 0x0000, constant_uint64(0x0000000000ff)));
						DMibAssertTrue(bool(fg_Get<1>(MinMax)));
						DMibExpect(*fg_Get<1>(MinMax), ==, NCryptography::CUniversallyUniqueIdentifier(0xA0EEBC99u, 0x9C0B, 0x4EF8, 0xBB6D, constant_uint64(0x6BB9BD380A11)));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Mutation returning a value whose mapped conversion throws fails the operation") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_CheckedSchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						co_await Database.f_ExecuteRaw(CStr("INSERT INTO checked_values (id, version, value) VALUES (1, 1, -5)"));

						CStr Error = co_await NMib::NSQL::NTest::NDatabaseBackend::fg_CallOnOtherActor
							(
								[Database]() mutable -> TCFuture<CStr>
								{
									auto Result = co_await Database.template f_UpdateReturning
										<
											&NMib::NSQL::NTest::NDatabaseBackend::CCheckedRow::m_Value
											, NMib::NSQL::NTest::NDatabaseBackend::gc_UpdateCheckedVersionByID
										>
										(int64(2), NMib::NSQL::NTest::NDatabaseBackend::CCheckedValue{.m_Value = 1}).f_Wrap()
									;
									co_return Result ? CStr() : Result.f_GetExceptionStr();
								}
							)
						;
						DMibExpect(Error, ==, CStr("Stored checked value is out of range"));

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

						for (umint iIteration = 0; iIteration < 3; ++iIteration)
							co_await Database.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-deallocate-before@example.com"));

						co_await Database.f_ExecuteRaw(NStr::CStr("DEALLOCATE ALL"));

						for (umint iIteration = 0; iIteration < 3; ++iIteration)
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

						// A savepoint created after RESET ALL, which clears the marker, must not make ROLLBACK TO SAVEPOINT look chained
						{
							auto Transaction = co_await Database.f_BeginTransaction();
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-reset-savepoint-before@example.com"));
							co_await Transaction.f_ExecuteRaw(NStr::CStr("RESET ALL"));
							co_await Transaction.f_ExecuteRaw(NStr::CStr("SAVEPOINT postgres_reset_savepoint"));
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-reset-savepoint-undone@example.com"));
							co_await Transaction.f_ExecuteRaw(NStr::CStr("ROLLBACK TO SAVEPOINT postgres_reset_savepoint"));
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-reset-savepoint-after@example.com"));
							co_await Transaction.f_Commit();
						}

						auto ResetSavepointCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM people WHERE email LIKE 'postgres-reset-savepoint-%'"));
						DMibExpect(ResetSavepointCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

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

						// Raw SQL that takes no snapshot, such as SET LOCAL, leaves the import possible
						{
							DMibTestPath("Snapshot import after SET LOCAL");

							CSqlTransactionSettings Settings;
							Settings.m_Isolation = ESqlTransactionIsolation::mc_RepeatableRead;

							auto Exporting = co_await Database.f_BeginReadTransaction(Settings);
							auto Snapshot = co_await Exporting.f_QueryRaw(NStr::CStr("SELECT pg_export_snapshot()"));
							NStr::CStr SnapshotID = Snapshot.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>();

							auto Importing = co_await Database.f_BeginReadTransaction(Settings);
							co_await Importing.f_QueryRaw(NStr::CStr("SET LOCAL lock_timeout = '10s'"));
							auto ImportResult = co_await Importing.f_QueryRaw("SET TRANSACTION SNAPSHOT '{}'"_f << SnapshotID).f_Wrap();
							DMibExpect(bool(ImportResult), ==, true);

							co_await Importing.f_Commit();
							co_await Exporting.f_Commit();
						}

						{
							auto Transaction = co_await Database.f_BeginTransaction();
							co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-chain-committed@example.com"));
							co_await Transaction.f_ExecuteRaw(NStr::CStr("COMMIT AND CHAIN"));

							auto AfterChain = co_await Transaction.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(NStr::CStr("postgres-chain-after@example.com")).f_Wrap();
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
					DMibTestCategory("Upsert into a table named excluded") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseUpsertIntoTableNamedExcluded(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Existing untracked table is adopted") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAdoptUntrackedExistingTable(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Adoption rejects undeclared constraints") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAdoptRejectsUndeclaredConstraints(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Adoption keeps foreign key behavior") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseAdoptKeepsForeignKeyBehavior(fCreateBackend, true);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("View named like a declared table is rejected") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRejectsViewNamedLikeTable(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Adoption keeps NULL uniqueness, collations and foreign key targets") -> TCFuture<void>
					{
						struct CAdoptionCase
						{
							ch8 const *m_pName;
							ch8 const *m_pCreateSql;
							ch8 const *m_pExtraSql;        // Run after the table is created, or nullptr
							ch8 const *m_pExpectedInError; // A part of the error naming the constraint, or nullptr when the adoption succeeds
						};

						static constexpr CAdoptionCase c_Cases[] =
							{
								{
									"Declared shape"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY, code text, parent_code text NOT NULL REFERENCES adopt_nullable_parent (code))"
									, nullptr
									, nullptr
								}
								,
								{
									"Unique constraint with NULLS NOT DISTINCT"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY, code text, parent_code text NOT NULL REFERENCES adopt_nullable_parent (code), "
									"CONSTRAINT adopt_nullable_code_key UNIQUE NULLS NOT DISTINCT (code))"
									, nullptr
									, "NULLS NOT DISTINCT"
								}
								,
								{
									"Same-named unique index"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY, code text, parent_code text NOT NULL REFERENCES adopt_nullable_parent (code))"
									, "CREATE UNIQUE INDEX adopt_nullable_code ON adopt_nullable (code)"
									, nullptr
								}
								,
								{
									"Same-named unique index with NULLS NOT DISTINCT"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY, code text, parent_code text NOT NULL REFERENCES adopt_nullable_parent (code))"
									, "CREATE UNIQUE INDEX adopt_nullable_code ON adopt_nullable (code) NULLS NOT DISTINCT"
									, "NULLS NOT DISTINCT"
								}
								,
								{
									"Deferrable primary key"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY DEFERRABLE, code text, parent_code text NOT NULL REFERENCES adopt_nullable_parent (code))"
									, nullptr
									, "DEFERRABLE"
								}
								,
								{
									"Deferred unique constraint replaced by a declared unique index"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY, code text, parent_code text NOT NULL REFERENCES adopt_nullable_parent (code), "
									"CONSTRAINT adopt_nullable_code_key UNIQUE (code) DEFERRABLE INITIALLY DEFERRED)"
									, nullptr
									, "DEFERRABLE"
								}
								,
								{
									"Unique key in a nondeterministic collation"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY, code text COLLATE adopt_case_insensitive UNIQUE, "
									"parent_code text NOT NULL REFERENCES adopt_nullable_parent (code))"
									, nullptr
									, "UNIQUE (code)"
								}
								,
								{
									"Foreign key to a same-named table in another schema"
									, "CREATE TABLE adopt_nullable (id bigint PRIMARY KEY, code text, parent_code text NOT NULL REFERENCES adopt_other.adopt_nullable_parent (code))"
									, nullptr
									, "adopt_other"
								}
							}
						;

						for (auto const &Case : c_Cases)
						{
							DMibTestPath(NStr::CStr(Case.m_pName));

							{
								CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_AdoptNullableSchemaVersions));
								co_await Database.f_Open();
								co_await Database.f_ApplySchema();

								co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable"));
								co_await Database.f_ExecuteRaw
									(
										NStr::CStr("CREATE COLLATION IF NOT EXISTS adopt_case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
									)
								;
								co_await Database.f_ExecuteRaw(NStr::CStr("CREATE SCHEMA IF NOT EXISTS adopt_other"));
								co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE IF NOT EXISTS adopt_other.adopt_nullable_parent (code text PRIMARY KEY)"));
								co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_other.adopt_nullable_parent (code) VALUES ('seed') ON CONFLICT DO NOTHING"));
								co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_nullable_parent (code) VALUES ('seed') ON CONFLICT DO NOTHING"));
								co_await Database.f_ExecuteRaw(NStr::CStr(Case.m_pCreateSql));
								if (Case.m_pExtraSql)
									co_await Database.f_ExecuteRaw(NStr::CStr(Case.m_pExtraSql));
								co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO adopt_nullable (id, code, parent_code) VALUES (1, NULL, 'seed')"));
								co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
							}

							CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_AdoptNullableSchemaVersions));
							co_await Database.f_Open();

							auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
							if (Case.m_pExpectedInError)
							{
								DMibExpect(bool(ApplyResult), ==, false);
								if (!ApplyResult)
								{
									NStr::CStr Error = ApplyResult.f_GetExceptionStr();
									DMibExpect(Error.f_Find("does not declare") >= aint(0), ==, true);
									DMibExpect(Error.f_Find(Case.m_pExpectedInError) >= aint(0), ==, true);
								}
							}
							else
								DMibExpect(bool(ApplyResult), ==, true);

							auto Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_nullable"));
							DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

							co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE adopt_nullable"));
							co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM schema_migrations"));
						}

						co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("DROP SCHEMA IF EXISTS adopt_other CASCADE"));
						co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("DROP COLLATION IF EXISTS adopt_case_insensitive"));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Adoption refuses to rebuild a partition") -> TCFuture<void>
					{
						co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("CREATE TABLE adopt_partitioned (id bigint NOT NULL, value text NOT NULL) PARTITION BY RANGE (id)"));
						co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("CREATE TABLE adopt_existing PARTITION OF adopt_partitioned FOR VALUES FROM (0) TO (100)"));
						co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("INSERT INTO adopt_partitioned (id, value) VALUES (1, 'seed')"));

						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_AdoptSchemaVersions));
						co_await Database.f_Open();

						auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
						DMibExpect(bool(ApplyResult), ==, false);
						if (!ApplyResult)
							DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("a partition") >= aint(0), ==, true);

						auto Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_partitioned"));
						DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Pooled connection the server closed is connected again at checkout") -> TCFuture<void>
					{
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings), .m_ConnectionPoolSize = 1}
							)
						;
						CSqlDatabaseClient Database(fg_TempCopy(Backend));
						co_await Database.f_Open();

						auto FirstPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						int64 FirstPid = FirstPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						co_await Client(&CPostgresClientActor::f_Execute, "SELECT pg_terminate_backend({})"_f << FirstPid);

						bool bSessionEnded = false;
						for (umint iAttempt = 0; iAttempt < 200 && !bSessionEnded; ++iAttempt)
						{
							auto Sessions = co_await Client(&CPostgresClientActor::f_Execute, "SELECT count(*)::int8 AS value FROM pg_stat_activity WHERE pid = {}"_f << FirstPid);
							bSessionEnded = Sessions.m_Rows[0].m_Values[0].f_GetAsType<int64>() == 0;
							if (!bSessionEnded)
								co_await fg_Timeout(0.05);
						}
						DMibExpectTrue(bSessionEnded);

						// The pooled client sees the close shortly after the server ends the session
						co_await fg_Timeout(0.5);

						auto SecondPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value")).f_Wrap();
						DMibExpect(bool(SecondPidResult), ==, true);
						if (SecondPidResult)
							DMibExpect(SecondPidResult->m_Rows[0].m_Values[0].f_GetAsType<int64>(), !=, FirstPid);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Raw execute ignores returned rows it can't decode") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						NStr::CStr const Sql("SELECT 'infinity'::timestamptz AS value");
						DMibExpect(co_await Database.f_ExecuteRaw(Sql), ==, umint(0));

						auto Transaction = co_await Database.f_BeginTransaction();
						DMibExpect(co_await Transaction.f_ExecuteRaw(Sql), ==, umint(0));
						co_await Transaction.f_Rollback();

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Raw stream without row description reuses the connection") -> TCFuture<void>
					{
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings), .m_ConnectionPoolSize = 1}
							)
						;
						CSqlDatabaseClient Database(fg_TempCopy(Backend));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto FirstPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(FirstPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 FirstPid = FirstPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						CSqlRawOperation StreamOperation;
						StreamOperation.m_Sql = "UPDATE people SET email = email WHERE false";
						auto StreamResult = co_await Database.f_QueryRawStream(fg_Move(StreamOperation)).f_Wrap();
						DMibAssert(bool(StreamResult), ==, true);
						DMibExpect(StreamResult->m_Columns.f_GetLen(), ==, umint(0));
						DMibExpect(co_await fg_CountRawStreamRows(fg_Move(*StreamResult)), ==, umint(0));

						auto SecondPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(SecondPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 SecondPid = SecondPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						DMibExpect(SecondPid, ==, FirstPid);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Prepared select whose result type changed recovers") -> TCFuture<void>
					{
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings), .m_ConnectionPoolSize = 1}
							)
						;
						CSqlDatabaseClient Database(fg_TempCopy(Backend));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						NStr::CStr Email("stale-prepared@example.com");
						co_await Database.f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertPerson>(fg_TempCopy(Email));

						// The first transaction prepares the select on the one connection, which the second then runs
						for (umint iTransaction = 0; iTransaction < 2; ++iTransaction)
						{
							DMibTestPath("Transaction {}"_f << iTransaction);
							auto Transaction = co_await Database.f_BeginReadTransaction();
							auto Row = co_await Transaction.template f_QueryOne<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPersonByEmail>(fg_TempCopy(Email));
							DMibExpect(Row.m_Email, ==, Email);
							co_await Transaction.f_Commit();
						}

						co_await Database.f_ExecuteRaw(NStr::CStr("ALTER TABLE people ALTER COLUMN email TYPE varchar(320)"));

						{
							auto Transaction = co_await Database.f_BeginReadTransaction();
							auto StaleResult = co_await Transaction.template f_QueryOne<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPersonByEmail>(fg_TempCopy(Email)).f_Wrap();
							DMibExpect(bool(StaleResult) || StaleResult.f_GetExceptionStr().f_Find("cached plan") >= aint(0), ==, true);
							co_await Transaction.f_Rollback();
						}

						auto Transaction = co_await Database.f_BeginReadTransaction();
						auto Row = co_await Transaction.template f_QueryOne<NMib::NSQL::NTest::NDatabaseBackend::gc_SelectPersonByEmail>(fg_TempCopy(Email));
						DMibExpect(Row.m_Email, ==, Email);
						co_await Transaction.f_Commit();

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Raw stream BEGIN does not pool an open-transaction connection") -> TCFuture<void>
					{
						// BEGIN opens a transaction without sending a RowDescription
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings), .m_ConnectionPoolSize = 1}
							)
						;
						CSqlDatabaseClient Database(fg_TempCopy(Backend));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						auto FirstPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(FirstPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 FirstPid = FirstPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						CSqlRawOperation StreamOperation;
						StreamOperation.m_Sql = "BEGIN";
						auto StreamResult = co_await Database.f_QueryRawStream(fg_Move(StreamOperation)).f_Wrap();
						DMibExpect(bool(StreamResult), ==, true);

						auto SecondPidResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT pg_backend_pid()::int8 AS value"));
						DMibExpect(SecondPidResult.m_Rows.f_GetLen(), ==, umint(1));
						int64 SecondPid = SecondPidResult.m_Rows[0].m_Values[0].f_GetAsType<int64>();

						DMibExpect(SecondPid, !=, FirstPid);

						co_return {};
					};

					DMibTestCategory("Raw transaction-control does not pool an open-transaction connection") -> TCFuture<void>
					{
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings), .m_ConnectionPoolSize = 1}
							)
						;
						CSqlDatabaseClient Database(fg_TempCopy(Backend));
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

					DMibTestCategory("Raw PREPARE does not leave its statement on a pooled connection") -> TCFuture<void>
					{
						NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendPostgres
							(
								&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions
								, {.m_ConnectionSettings = CPostgresConnectionSettings(_Settings), .m_ConnectionPoolSize = 1}
							)
						;
						CSqlDatabaseClient Database(fg_TempCopy(Backend));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						co_await Database.f_ExecuteRaw(NStr::CStr("PREPARE raw_pooled_prepare_probe AS SELECT 1"));

						auto SecondPrepareResult = co_await Database.f_ExecuteRaw(NStr::CStr("PREPARE raw_pooled_prepare_probe AS SELECT 1")).f_Wrap();
						DMibExpect(bool(SecondPrepareResult), ==, true);

						auto TransactionResult = co_await Database.f_WithTransaction
							(
								[](CSqlTransaction _Transaction) -> TCFuture<void>
								{
									co_await _Transaction.f_ExecuteRaw(NStr::CStr("PREPARE raw_pooled_transaction_probe AS SELECT 1"));
									co_return {};
								}
							)
							.f_Wrap()
						;
						DMibExpect(bool(TransactionResult), ==, true);

						auto SecondTransactionPrepareResult = co_await Database.f_ExecuteRaw(NStr::CStr("PREPARE raw_pooled_transaction_probe AS SELECT 1")).f_Wrap();
						DMibExpect(bool(SecondTransactionPrepareResult), ==, true);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Referenced table rebuild migration") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseForeignKeyRebuildMigration(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Upgrade recreates a dropped table with its foreign keys") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRecreatedTableForeignKeys(fCreateBackend, NStr::CStr("DROP TABLE fk_parent CASCADE"));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Dropping a referenced table is rejected") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseDropReferencedTable(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild of a table that a partitioned table references") -> TCFuture<void>
					{
						{
							CSqlDatabaseClient Version1Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_FkVersion1SchemaVersions));
							co_await Version1Database.f_Open();
							co_await Version1Database.f_ApplySchema();

							co_await Version1Database.template f_Insert<NMib::NSQL::NTest::NDatabaseBackend::gc_InsertFkParent>(NStr::CStr("parent"));
							co_await Version1Database.f_ExecuteRaw
								(
									NStr::CStr("CREATE TABLE fk_partitioned_child (id bigint NOT NULL, parent_id bigint NOT NULL REFERENCES fk_parent (id)) PARTITION BY RANGE (id)")
								)
							;
							co_await Version1Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE fk_partitioned_child_low PARTITION OF fk_partitioned_child FOR VALUES FROM (0) TO (100)"));
							co_await Version1Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE fk_partitioned_child_high PARTITION OF fk_partitioned_child FOR VALUES FROM (100) TO (200)"));
							co_await Version1Database.f_ExecuteRaw(NStr::CStr("INSERT INTO fk_partitioned_child (id, parent_id) VALUES (1, 1), (150, 1)"));
						}

						CSqlDatabaseClient Version2Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_FkParentRebuildSchemaVersions));
						co_await Version2Database.f_Open();
						co_await Version2Database.f_ApplySchema();

						auto Count = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM fk_partitioned_child"));
						DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

						auto OrphanResult = co_await Version2Database.f_ExecuteRaw(NStr::CStr("INSERT INTO fk_partitioned_child (id, parent_id) VALUES (151, 999999)")).f_Wrap();
						DMibExpect(bool(OrphanResult), ==, false);

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
					DMibTestCategory("Drop column migration keeps undeclared columns") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseDropColumnKeepsUndeclaredColumn(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild keeps undeclared columns") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRebuildKeepsUndeclaredColumn(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Drop column keeps a column a later operation drops") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseDropColumnKeepsLaterDroppedColumn(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild keeps a column a later operation drops") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseRebuildKeepsLaterDroppedColumn(fCreateBackend);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Rebuild of a parent leaves a retargeted child foreign key to the additive pass") -> TCFuture<void>
					{
						{
							CSqlDatabaseClient Version1Database(fCreateBackend(&gc_FkRetargetV1SchemaVersions));
							co_await Version1Database.f_Open();
							co_await Version1Database.f_ApplySchema();

							co_await Version1Database.f_ExecuteRaw(CStr("INSERT INTO fk_retarget_a (id) VALUES (1), (2)"));
							co_await Version1Database.f_ExecuteRaw(CStr("INSERT INTO fk_retarget_b (id) VALUES (2)"));
							co_await Version1Database.f_ExecuteRaw(CStr("INSERT INTO fk_retarget_child (id, parent_id) VALUES (1, 2)"));
						}

						CSqlDatabaseClient Version2Database(fCreateBackend(&gc_FkRetargetSchemaVersions));
						co_await Version2Database.f_Open();
						co_await Version2Database.f_ApplySchema();

						CSqlRawResult Parent = co_await Version2Database.f_QueryRaw
							(
								CStr("SELECT confrelid::regclass::text FROM pg_constraint WHERE conname = 'fk_retarget_child_parent_fk' AND conrelid = 'fk_retarget_child'::regclass")
							)
						;
						DMibAssert(Parent.m_Rows.f_GetLen(), ==, umint(1));
						DMibExpect(Parent.m_Rows[0].m_Values[0].f_GetAsType<CStr>(), ==, CStr("fk_retarget_b"));

						auto OrphanResult = co_await Version2Database.f_ExecuteRaw(CStr("INSERT INTO fk_retarget_child (id, parent_id) VALUES (2, 1)")).f_Wrap();
						DMibExpect(bool(OrphanResult), ==, false);

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Returned value outside its field's range or infinite fails the mutation") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&gc_ReturningNarrowSchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						// SMALLINT holds the value, but the int8 field cannot
						co_await Database.f_ExecuteRaw
							(
								CStr
								(
									"INSERT INTO returning_narrow (code, small, day, at, days) VALUES "
									"('wide', 200, '2020-01-01', '2020-01-01 00:00:00+00', '{2020-01-01}'), "
									"('narrow', 5, '2020-01-02', '2020-01-02 00:00:00+00', '{2020-01-02}'), "
									"('infinite_day', 1, 'infinity', '2020-01-03 00:00:00+00', '{2020-01-03}'), "
									"('infinite_at', 1, '2020-01-04', '-infinity', '{2020-01-04}'), "
									"('infinite_days', 1, '2020-01-05', '2020-01-05 00:00:00+00', '{2020-01-05,infinity}')"
								)
							)
						;

						{
							DMibTestPath("Standalone");

							auto DeleteResult = co_await Database.template f_DeleteReturning<&CReturningNarrowRow::m_Small, gc_DeleteReturningNarrowByCode>(CStr("wide")).f_Wrap();
							DMibExpect(bool(DeleteResult), ==, false);

							auto UpdateResult = co_await Database.template f_UpdateReturning<&CReturningNarrowRow::m_Small, gc_UpdateReturningNarrowCodeByCode>
								(
									CStr("wide-updated")
									, CStr("wide")
								)
								.f_Wrap()
							;
							DMibExpect(bool(UpdateResult), ==, false);

							CSqlRawResult Codes = co_await Database.f_QueryRaw(CStr("SELECT code FROM returning_narrow WHERE small = 200"));
							DMibAssert(Codes.m_Rows.f_GetLen(), ==, umint(1));
							DMibExpect(Codes.m_Rows[0].m_Values[0].f_GetAsType<CStr>(), ==, CStr("wide"));
						}

						{
							DMibTestPath("In a transaction");

							auto Transaction = co_await Database.f_BeginTransaction();
							auto DeleteResult = co_await Transaction.template f_DeleteReturning<&CReturningNarrowRow::m_Small, gc_DeleteReturningNarrowByCode>(CStr("wide")).f_Wrap();
							DMibExpect(bool(DeleteResult), ==, false);
							co_await Transaction.f_Rollback();

							CSqlRawResult Count = co_await Database.f_QueryRaw(CStr("SELECT count(*) FROM returning_narrow WHERE code = 'wide'"));
							DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));
						}

						{
							DMibTestPath("Infinite");

							auto DayResult = co_await Database.template f_DeleteReturning<&CReturningNarrowRow::m_Day, gc_DeleteReturningNarrowByCode>(CStr("infinite_day")).f_Wrap();
							DMibExpect(bool(DayResult), ==, false);

							auto AtResult = co_await Database.template f_DeleteReturning<&CReturningNarrowRow::m_At, gc_DeleteReturningNarrowByCode>(CStr("infinite_at")).f_Wrap();
							DMibExpect(bool(AtResult), ==, false);

							auto DaysResult = co_await Database.template f_DeleteReturning<&CReturningNarrowRow::m_Days, gc_DeleteReturningNarrowByCode>(CStr("infinite_days")).f_Wrap();
							DMibExpect(bool(DaysResult), ==, false);

							CSqlRawResult Count = co_await Database.f_QueryRaw(CStr("SELECT count(*) FROM returning_narrow WHERE code LIKE 'infinite%'"));
							DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(3));
						}

						CSqlDate Day = co_await Database.template f_UpdateReturning<&CReturningNarrowRow::m_Day, gc_UpdateReturningNarrowCodeByCode>(CStr("narrow"), CStr("narrow"));
						DMibExpect(Day.m_Time, ==, NTime::CTimeConvert::fs_CreateTime(2020, 1, 2));

						NTime::CTime At = co_await Database.template f_UpdateReturning<&CReturningNarrowRow::m_At, gc_UpdateReturningNarrowCodeByCode>(CStr("narrow"), CStr("narrow"));
						DMibExpect(At, ==, NTime::CTimeConvert::fs_CreateTime(2020, 1, 2));

						TCSqlArray<CSqlDate> Days = co_await Database.template f_UpdateReturning<&CReturningNarrowRow::m_Days, gc_UpdateReturningNarrowCodeByCode>
							(
								CStr("narrow")
								, CStr("narrow")
							)
						;
						DMibAssert(Days.m_Values.f_GetLen(), ==, umint(1));
						DMibExpect((*Days.m_Values[0]).m_Time, ==, NTime::CTimeConvert::fs_CreateTime(2020, 1, 2));

						int8 Small = co_await Database.template f_DeleteReturning<&CReturningNarrowRow::m_Small, gc_DeleteReturningNarrowByCode>(CStr("narrow"));
						DMibExpect(Small, ==, int8(5));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Gap in the applied history") -> TCFuture<void>
					{
						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseHistoryGapValidation(fCreateBackend);

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

					DMibTestCategory("Schema history with columns that are not text fails the apply") -> TCFuture<void>
					{
						static constexpr ch8 const *c_CreateSql[] =
							{
								"CREATE TABLE schema_migrations (id BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, "
								"applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, applied_by_version TEXT NOT NULL)"
								, "CREATE TABLE schema_migrations (id TEXT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT, "
								"applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, applied_by_version TEXT NOT NULL)"
							}
						;
						static constexpr ch8 const *c_InsertSql[] =
							{
								"INSERT INTO schema_migrations (id, name, checksum, applied_by_version) VALUES (1, 'numeric', 'checksum', 'test')"
								, "INSERT INTO schema_migrations (id, name, checksum, applied_by_version) VALUES ('null-checksum', 'null', NULL, 'test')"
							}
						;

						for (umint iCase = 0; iCase < 2; ++iCase)
						{
							DMibTestPath(iCase == 0 ? "Numeric id" : "NULL checksum");

							co_await fg_ResetPostgresBackendTestSchema(Client);
							co_await Client(&CPostgresClientActor::f_Execute, CStr(c_CreateSql[iCase]));
							co_await Client(&CPostgresClientActor::f_Execute, CStr(c_InsertSql[iCase]));

							CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
							co_await Database.f_Open();
							auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
							DMibAssertFalse(bool(ApplyResult));
							DMibExpectTrue(ApplyResult.f_GetExceptionStr().f_Find("schema_migrations has an id or checksum that is not text") >= 0);
						}

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
					DMibTestCategory("Index migration in a schema whose name needs quoting") -> TCFuture<void>
					{
						// The role's search_path applies to the backend's new connections, while the reset client keeps public
						co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("CREATE SCHEMA \"Mib Mixed.Case\""));
						auto CleanupSchema = co_await fg_AsyncDestroy
							(
								[&]() -> TCFuture<void>
								{
									TCActor<CPostgresClientActor> ResetClient = Client;
									co_await ResetClient(&CPostgresClientActor::f_Execute, NStr::CStr("ALTER ROLE CURRENT_USER RESET search_path"));
									co_await ResetClient(&CPostgresClientActor::f_Execute, NStr::CStr("DROP SCHEMA IF EXISTS \"Mib Mixed.Case\" CASCADE"));
									co_return {};
								}
							)
						;
						co_await Client(&CPostgresClientActor::f_Execute, NStr::CStr("ALTER ROLE CURRENT_USER SET search_path TO \"Mib Mixed.Case\""));

						co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseIndexMigration(fCreateBackend);

						auto Indexes = co_await Client
							(
								&CPostgresClientActor::f_Execute
								, NStr::CStr("SELECT tablename::text FROM pg_indexes WHERE schemaname = 'Mib Mixed.Case' AND indexname = 'idx_indexed_people_email'")
							)
						;
						DMibExpect(Indexes.m_Rows.f_GetLen(), ==, umint(1));

						co_return {};
					};

					co_await fg_ResetPostgresBackendTestSchema(Client);
					DMibTestCategory("Abandoned select stream") -> TCFuture<void>
					{
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						for (uint64 iIteration = 0; iIteration < 129; ++iIteration)
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
						CSqlDatabaseClient Database(fCreateBackend(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ApplySchema();

						co_await NMib::NSQL::NTest::NPostgresDatabase::fg_RunPostgresParityTests(fg_Move(Database));

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
	void f_DoTests();
};

void CPostgresDatabase_Tests::f_DoTests()
{
	DMibTestSuite("Postgres rejects zero-sized connection pool") -> TCFuture<void>
	{
		CPostgresDatabaseBackendSettings Settings;
		Settings.m_ConnectionPoolSize = 0;

		CSqlDatabaseClient Database(fg_CreateDatabaseBackendPostgres(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings)));
		auto OpenResult = co_await Database.f_Open().f_Wrap();

		DMibExpectException(OpenResult.f_Access(), DMibErrorDatabaseInstance("PostgreSQL connection pool size must be at least 1"));

		co_return {};
	};

	DMibTestSuite("Postgres rejects zero select rows per batch") -> TCFuture<void>
	{
		CPostgresDatabaseBackendSettings Settings;
		Settings.m_nSelectRowsPerBatch = 0;

		CSqlDatabaseClient Database(fg_CreateDatabaseBackendPostgres(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings)));
		auto OpenResult = co_await Database.f_Open().f_Wrap();

		DMibExpectException(OpenResult.f_Access(), DMibErrorDatabaseInstance("PostgreSQL select rows per batch must be at least 1"));

		co_return {};
	};

	DMibTestSuite("Postgres rejects zero pipeline length") -> TCFuture<void>
	{
		CPostgresDatabaseBackendSettings Settings;
		Settings.m_nPipelineLength = 0;

		CSqlDatabaseClient Database(fg_CreateDatabaseBackendPostgres(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings)));
		auto OpenResult = co_await Database.f_Open().f_Wrap();

		DMibExpectException(OpenResult.f_Access(), DMibErrorDatabaseInstance("PostgreSQL pipeline length must be at least 1"));

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

DMibTestRegister(CPostgresDatabase_Tests, Malterlib::SQL);
