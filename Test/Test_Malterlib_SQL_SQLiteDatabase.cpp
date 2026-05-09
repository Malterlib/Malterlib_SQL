// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/SQLiteDatabase>
#include <Mib/File/File>

#include "Test_Malterlib_SQL_DatabaseBackendShared.h"
#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

#include "../Source/SQLiteDatabase/Malterlib_SQL_SQLiteDatabase_Internal.h"

using namespace NMib;
using namespace NMib::NConcurrency;
using namespace NMib::NSQL;
using namespace NMib::NStr;

namespace NMib::NSQL::NTest::NSQLiteDatabase
{
	void fg_DeleteSqliteDatabaseFiles(CStr const &_DatabasePath)
	{
		fg_TestAddCleanupPath(_DatabasePath);
		fg_TestAddCleanupPath(_DatabasePath + "-wal");
		fg_TestAddCleanupPath(_DatabasePath + "-shm");

		if (NFile::CFile::fs_FileExists(_DatabasePath))
			NFile::CFile::fs_DeleteFile(_DatabasePath);

		if (NFile::CFile::fs_FileExists(_DatabasePath + "-wal"))
			NFile::CFile::fs_DeleteFile(_DatabasePath + "-wal");

		if (NFile::CFile::fs_FileExists(_DatabasePath + "-shm"))
			NFile::CFile::fs_DeleteFile(_DatabasePath + "-shm");
	}

	auto fg_CreateSqliteBackendFactory(CStr _Path)
	{
		return [Path = fg_Move(_Path)](ICSqlSchemaVersions const *_pSchemaVersions)
			{
				CSQLiteDatabaseBackendSettings Settings;
				Settings.m_Path = Path;

				return fg_CreateDatabaseBackendSQLite(_pSchemaVersions, fg_Move(Settings));
			}
		;
	}

	struct CSQLiteDatabase_Tests : public NMib::NTest::CTest
	{
		void f_DoTests()
		{
			DMibTestSuite("Typed SQLite database in memory") -> TCFuture<void>
			{
				co_await NDatabaseBackend::fg_TestSqlDatabase(fg_CreateSqliteBackendFactory(":memory:"));

				co_return {};
			};

			DMibTestSuite("Typed SQLite database file") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabase.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabase(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite typed insert into autoincrement-only table") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// A table whose only column is an autoincrement primary key has nothing to insert; the typed
				// default insert must emit DEFAULT VALUES (not the invalid "() VALUES ()") so each row is created
				// with a generated id.
				co_await Database.f_ExecuteRaw("CREATE TABLE id_only_default (id INTEGER PRIMARY KEY AUTOINCREMENT)");

				co_await Database.f_Insert(NDatabaseBackend::gc_IdOnlyTable, NDatabaseBackend::CIdOnlyRow{});
				co_await Database.f_Insert(NDatabaseBackend::gc_IdOnlyTable, NDatabaseBackend::CIdOnlyRow{});

				CSqlRawResult Result = co_await Database.f_QueryRaw("SELECT COUNT(*) AS row_count, MIN(id) AS min_id, MAX(id) AS max_id FROM id_only_default");
				DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));
				if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values.f_GetLen() == 3)
				{
					DMibExpect(Result.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));
					DMibExpect(Result.m_Rows[0].m_Values[1].f_GetAsType<int64>(), ==, int64(1));
					DMibExpect(Result.m_Rows[0].m_Values[2].f_GetAsType<int64>(), ==, int64(2));
				}

				co_return {};
			};

			DMibTestSuite("SQLite empty path uses a single shared connection") -> TCFuture<void>
			{
				CSQLiteDatabaseBackendSettings Settings;
				Settings.m_Path = "";
				Settings.m_nReadConnections = 4;

				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendSQLite(&NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings));
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// An empty path opens an anonymous temporary database that is private to each connection. Were it
				// treated as a multi-connection database, the read pool would open separate empty databases and
				// never observe this row, so the read must run on the same single connection as the write.
				co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(NStr::CStr("empty-path@example.com"));

				umint nCount = co_await Database.f_Count<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("empty-path@example.com"));
				DMibExpect(nCount, ==, umint(1));

				co_return {};
			};

			DMibTestSuite("SQLite unique column flag is enforced") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// The generated DDL for a column flagged mc_Unique must carry a UNIQUE constraint the database
				// enforces; a second row reusing the same code must be rejected.
				CStr CreateSql = co_await NMib::NSQL::NPrivate::fg_SqliteCreateTable(NDatabaseBackend::gc_UniqueColumnTable.f_Describe(), false);
				co_await Database.f_ExecuteRaw(fg_Move(CreateSql));

				NDatabaseBackend::CUniqueColumnRow Row;
				Row.m_Code = "duplicate-code";
				co_await Database.f_Insert(NDatabaseBackend::gc_UniqueColumnTable, fg_TempCopy(Row));

				auto DuplicateResult = co_await Database.f_Insert(NDatabaseBackend::gc_UniqueColumnTable, fg_TempCopy(Row)).f_Wrap();
				DMibExpect(bool(DuplicateResult), ==, false);

				co_return {};
			};

			DMibTestSuite("SQLite raw query routes a returning mutation to the write connection") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRawReturning.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				// A file-based database uses a read pool whose connections are opened query_only=ON. INSERT ...
				// RETURNING returns rows while mutating, so f_QueryRaw must run it on the write connection rather
				// than rejecting it on a read-pool connection.
				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				CSqlRawResult Result = co_await Database.f_QueryRaw(NStr::CStr("INSERT INTO people (email) VALUES ('raw-returning@example.com') RETURNING id"));
				DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));

				umint nCount = co_await Database.f_Count<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("raw-returning@example.com"));
				DMibExpect(nCount, ==, umint(1));

				co_return {};
			};

			DMibTestSuite("SQLite raw stream bounds null-column type inference") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRawStreamNullInference.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE null_stream (id INTEGER PRIMARY KEY, late_value INTEGER)"));
				// late_value + 0 is an expression, so the streamed column has no declared type and its value type must be
				// inferred from the rows. Every probed row is NULL until a single trailing non-NULL value that sits far
				// beyond the inference cap. A bounded inference pass must give up and report the column as Null rather
				// than stepping to that trailing row, which would buffer the entire result before the stream returns.
				co_await Database.f_ExecuteRaw
					(
						NStr::CStr("WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 200) INSERT INTO null_stream (id, late_value) SELECT n, NULL FROM seq")
					)
				;
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO null_stream (id, late_value) VALUES (201, 42)"));

				CSqlRawOperation Operation;
				Operation.m_Sql = "SELECT late_value + 0 AS value FROM null_stream ORDER BY id";
				Operation.m_nRowsPerBatch = 16;

				auto Stream = co_await Database.f_QueryRawStream(fg_Move(Operation));
				DMibExpect(Stream.m_Columns.f_GetLen(), ==, umint(1));
				DMibExpect(Stream.m_Columns[0].m_ValueType, ==, ESqlValueType::mc_Null);

				umint nRows = 0;
				umint nNonNull = 0;
				int64 LastValue = 0;
				for (auto iBatch = co_await fg_Move(Stream.m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
					for (auto const &Row : *iBatch)
					{
						++nRows;
						if (Row.m_Values[0].f_GetTypeID() != ESqlValueType::mc_Null)
						{
							++nNonNull;
							LastValue = Row.m_Values[0].f_GetAsType<int64>();
						}
					}
				}

				DMibExpect(nRows, ==, umint(201));
				DMibExpect(nNonNull, ==, umint(1));
				DMibExpect(LastValue, ==, int64(42));

				co_return {};
			};

			DMibTestSuite("SQLite raw bind parameter count is enforced") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteBindParameterCount.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// A placeholder with no supplied value must be rejected, not silently bound to NULL.
				auto MissingValueResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT ? AS value")).f_Wrap();
				DMibExpect(bool(MissingValueResult), ==, false);

				NContainer::TCVector<CSqlValue> OneValue;
				OneValue.f_InsertLast(NStr::CStr("only-one@example.com"));
				auto TooFewResult = co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO people (email) VALUES (?), (?)"), fg_Move(OneValue)).f_Wrap();
				DMibExpect(bool(TooFewResult), ==, false);

				co_return {};
			};

			DMibTestSuite("SQLite raw empty statement is rejected") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteEmptyStatement.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// sqlite3_prepare_v2 compiles empty, whitespace-only, and comment-only SQL to a null statement. The raw
				// paths must reject that at prepare time. Without the fix the null statement flows on and the call fails
				// only later (a step on a null statement reports SQLITE_MISUSE), so assert the precise prepare-time error
				// rather than just a failed result - the message distinguishes the clean rejection from the misuse path.
				auto EmptyQuery = co_await Database.f_QueryRaw(NStr::CStr("")).f_Wrap();
				DMibExpect(bool(EmptyQuery), ==, false);
				DMibExpect(EmptyQuery.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

				auto WhitespaceQuery = co_await Database.f_QueryRaw(NStr::CStr("   \n\t")).f_Wrap();
				DMibExpect(bool(WhitespaceQuery), ==, false);
				DMibExpect(WhitespaceQuery.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

				auto CommentQuery = co_await Database.f_QueryRaw(NStr::CStr("-- just a comment")).f_Wrap();
				DMibExpect(bool(CommentQuery), ==, false);
				DMibExpect(CommentQuery.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

				auto EmptyExecute = co_await Database.f_ExecuteRaw(NStr::CStr("")).f_Wrap();
				DMibExpect(bool(EmptyExecute), ==, false);
				DMibExpect(EmptyExecute.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

				// The connection is still usable after the rejected statements.
				umint nInserted = co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO people (email) VALUES ('empty-stmt-after@example.com')"));
				DMibExpect(nInserted, ==, umint(1));

				co_return {};
			};

			DMibTestSuite("SQLite custom backend settings") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseSettings.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSQLiteDatabaseBackendSettings Settings;
				Settings.m_Path = DatabasePath;
				Settings.m_nReadConnections = 2;
				Settings.m_nSelectRowsPerBatch = 1;

				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendSQLite(&NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings));
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				NDatabaseBackend::CPersonRow Person;
				Person.m_Email = "settings-batch@example.com";
				co_await Database.f_Insert(NDatabaseBackend::gc_PersonTable, fg_TempCopy(Person));
				co_await Database.f_Insert(NDatabaseBackend::gc_PersonTable, fg_TempCopy(Person));

				auto Rows = Database.template f_Query<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("settings-batch@example.com"));
				umint nBatches = 0;
				umint nRows = 0;
				for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
					++nBatches;
					DMibTestPath("Batch {}"_f << nBatches);
					DMibExpect((*iBatch).f_GetLen(), ==, umint(1));

					for (auto const &pRow : *iBatch)
					{
						++nRows;
						DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("settings-batch@example.com"));
					}
				}

				DMibExpect(nBatches, ==, umint(2));
				DMibExpect(nRows, ==, umint(2));

				co_return {};
			};

			DMibTestSuite("SQLite raw query transaction-control does not leak an open transaction") -> TCFuture<void>
			{
				// BEGIN/SAVEPOINT report as read-only, so f_QueryRaw and f_QueryRawStream run them on a pooled read
				// connection. With a single read connection the leak is deterministic: the next read reuses the very
				// connection a leaked BEGIN left mid-transaction. The raw query paths must roll it back to autocommit
				// before releasing it, like f_ExecuteRaw does.
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRawQueryLeak.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSQLiteDatabaseBackendSettings Settings;
				Settings.m_Path = DatabasePath;
				Settings.m_nReadConnections = 1;

				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendSQLite(&NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings));
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// Read path: an un-rolled-back BEGIN leaves the single read connection in a transaction, so the next
				// f_QueryRaw BEGIN (reusing it) fails with "cannot start a transaction within a transaction".
				auto FirstBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
				DMibExpect(bool(FirstBegin), ==, true);
				auto SecondBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
				DMibExpect(bool(SecondBegin), ==, true);

				// Stream path: same release point at the end of the stream generator. Draining a streamed BEGIN must
				// likewise leave the read connection in autocommit, so the following BEGIN still starts cleanly.
				auto StreamResult = co_await Database.f_QueryRawStream(NStr::CStr("BEGIN")).f_Wrap();
				DMibExpect(bool(StreamResult), ==, true);
				if (StreamResult)
				{
					for (auto iBatch = co_await fg_Move(StreamResult->m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
					{
					}
				}
				auto ThirdBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
				DMibExpect(bool(ThirdBegin), ==, true);

				co_return {};
			};

			DMibTestSuite("SQLite single-connection raw query transaction-control does not leak") -> TCFuture<void>
			{
				// On a single-connection database (no read pool) f_QueryRaw takes the write fallback. A BEGIN left open
				// there blocks the next real transaction, so f_WithTransaction must still start and commit afterwards.
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				auto RawBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
				DMibExpect(bool(RawBegin), ==, true);

				auto TransactionResult = co_await Database.f_WithTransaction
					(
						[](CSqlTransaction) -> NConcurrency::TCFuture<void>
						{
							co_return {};
						}
					)
					.f_Wrap()
				;
				DMibExpect(bool(TransactionResult), ==, true);

				co_return {};
			};

			DMibTestSuite("SQLite type capabilities") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseBackendCapabilities Capabilities = co_await Backend(&ICSqlDatabaseBackendActor::f_Capabilities);

				DMibExpect(Capabilities.m_bUUID, ==, false);
				DMibExpect(Capabilities.m_bDate, ==, false);
				DMibExpect(Capabilities.m_bTimeOfDay, ==, false);
				DMibExpect(Capabilities.m_bTimestamp, ==, false);
				DMibExpect(Capabilities.m_bTimestampTz, ==, false);
				DMibExpect(Capabilities.m_bInterval, ==, false);
				DMibExpect(Capabilities.m_bJSON, ==, false);
				DMibExpect(Capabilities.m_bJSONB, ==, false);
				DMibExpect(Capabilities.m_bArrays, ==, false);
				DMibExpect(Capabilities.m_bUnrecognizedBackend, ==, false);
				DMibExpect(Capabilities.m_bForeignKeyEnforcement, ==, true);
				DMibExpect(Capabilities.m_bNumberedPlaceholders, ==, false);

				DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_Integer32), ==, true);
				DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_Text), ==, true);
				DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_UUID), ==, false);
				DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_Json), ==, false);
				DMibExpect(Capabilities.f_SupportsValueType(ESqlValueType::mc_Array_Integer32), ==, false);

				co_return {};
			};

			DMibTestSuite("SQLite schema migration applies additive changes") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();

				CSqlRawResult Result = co_await Database.f_QueryRaw("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND tbl_name = 'user_roles' AND name = 'user_roles_role'");
				DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));
				if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values.f_GetLen() == 1)
					DMibExpect(Result.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

				co_return {};
			};

			DMibTestSuite("SQLite schema migration applies renames") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseRenameMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseRenameMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite rejects unknown schema version") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseUnknownVersion.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseUnknownVersionValidation(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite rejects schema checksum mismatch") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseChecksumMismatch.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseChecksumValidation(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite rejects required column without default") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseRequiredColumn.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseRequiredColumnValidation(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite schema migration accepts backend-specific default") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseBackendDefault.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Version1Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_BackendDefaultVersion1SchemaVersions));
					co_await Version1Database.f_Open();
					co_await Version1Database.f_ApplySchema();
					co_await Version1Database.f_ExecuteRaw("INSERT INTO backend_default_test DEFAULT VALUES");
				}

				CSqlDatabaseClient Version2Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_BackendDefaultSchemaVersions));
				co_await Version2Database.f_Open();
				co_await Version2Database.f_ApplySchema();

				CSqlRawResult Result = co_await Version2Database.f_QueryRaw("SELECT backend_value FROM backend_default_test WHERE id = 1");
				DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));
				if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values.f_GetLen() == 1)
					DMibExpect(Result.m_Rows[0].m_Values[0].f_GetAsType<CStr>(), ==, CStr("sqlite-default"));

				co_return {};
			};

			DMibTestSuite("SQLite schema migration rebuilds table") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseRebuildMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseRebuildMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite rebuild preserves a user table sharing the scratch name") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseRebuildScratchNameCollision.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseRebuildScratchNameCollision(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite rebuild adds a foreign key to a table created in the same version") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseRebuildAddsForeignKeyToNewTable.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseRebuildAddsForeignKeyToNewTable(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite migration adds a column with an expression default") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAddExpressionDefault.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseAddColumnWithExpressionDefault(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite migration drops a column alongside an added column") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDropColumnWithAddedColumn.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseDropColumnWithAddedColumn(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite adopts an existing untracked table") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptUntrackedTable.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseAdoptUntrackedExistingTable(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite adopts a table missing a declared constraint") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptMissingConstraint.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					// Create adopt_unique without its declared UNIQUE constraint, outside version tracking, and seed a row.
					CSqlDatabaseClient Setup(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptUniqueSchemaVersions));
					co_await Setup.f_Open();
					co_await Setup.f_ExecuteRaw(NStr::CStr("CREATE TABLE \"adopt_unique\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"code\" TEXT NOT NULL)"));
					co_await Setup.f_ExecuteRaw(NStr::CStr("INSERT INTO \"adopt_unique\" (\"code\") VALUES ('first')"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptUniqueSchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				DMibTestPath("Adopted table enforces its declared UNIQUE constraint");

				// The pre-existing row survived the adoption rebuild.
				CSqlRawResult AdoptedCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_unique"));
				DMibExpect(AdoptedCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

				// The UNIQUE constraint is now enforced: re-inserting the same code is rejected.
				auto DuplicateResult = co_await Database.template f_Insert<NDatabaseBackend::gc_InsertAdoptUnique>(NStr::CStr("first")).f_Wrap();
				DMibExpect(bool(DuplicateResult), ==, false);

				// A distinct code still inserts.
				co_await Database.template f_Insert<NDatabaseBackend::gc_InsertAdoptUnique>(NStr::CStr("second"));

				co_return {};
			};

			DMibTestSuite("SQLite migration adds a unique column") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseAddUniqueColumnMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseAddUniqueColumnMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite generated column") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseGeneratedColumn.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseGeneratedColumn(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite generated column insert paths") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseGeneratedColumnInsertPaths.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseGeneratedColumnInsertPaths(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite generated column rebuild migration") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseGeneratedColumnRebuildMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseGeneratedColumnRebuildMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};


			DMibTestSuite("SQLite schema apply restores legacy_alter_table") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// Reproduce the connection state a failed rebuild leaves behind: a rebuild scopes legacy_alter_table=ON
				// around its rename, and if the rename fails before the matching OFF (for example onto a colliding
				// scratch name) the pragma stays ON on the pooled connection — it is connection-scoped, not
				// transactional, so the schema-apply rollback does not undo it.
				co_await Database.f_QueryRaw(NStr::CStr("PRAGMA legacy_alter_table=ON"));

				// A schema apply must restore the pragma in its cleanup (which also restores foreign_keys). Without
				// that, the connection keeps legacy ALTER TABLE semantics for later migrations.
				co_await Database.f_ApplySchema();

				// Probe in a foreign_keys=OFF context, as migrations run: renaming a referenced table must still rewrite
				// the child foreign key, which only happens when legacy_alter_table is OFF.
				co_await Database.f_QueryRaw(NStr::CStr("PRAGMA foreign_keys=OFF"));
				co_await Database.f_QueryRaw(NStr::CStr("CREATE TABLE \"legacy_parent\" (id INTEGER PRIMARY KEY)"));
				co_await Database.f_QueryRaw(NStr::CStr("CREATE TABLE \"legacy_child\" (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES \"legacy_parent\"(id))"));
				co_await Database.f_QueryRaw(NStr::CStr("ALTER TABLE \"legacy_parent\" RENAME TO \"legacy_parent_renamed\""));

				CSqlRawResult ChildSchema = co_await Database.f_QueryRaw(NStr::CStr("SELECT sql FROM sqlite_master WHERE name = 'legacy_child'"));
				DMibExpect(ChildSchema.m_Rows.f_GetLen(), ==, umint(1));
				if (ChildSchema.m_Rows.f_GetLen() == 1 && ChildSchema.m_Rows[0].m_Values.f_GetLen() == 1)
				{
					NStr::CStr ChildSql = ChildSchema.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>();
					DMibExpect(ChildSql.f_Find("legacy_parent_renamed") >= aint(0), ==, true);
				}

				co_return {};
			};

			DMibTestSuite("SQLite schema migration rebuilds a referenced table") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseForeignKeyRebuildMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseForeignKeyRebuildMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite schema applies forward foreign-key reference") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseForeignKeyOrdering.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseForeignKeyOrderingSchema(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite schema migration rebuilds a self-referential table") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseSelfReferentialForeignKeyRebuildMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseSelfReferentialForeignKeyRebuildMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite schema migration drops column") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseDropColumnMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseDropColumnMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite schema migration transforms data") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseTransformMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseTransformMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite schema migration creates indexes") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseIndexMigration.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseIndexMigration(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestSuite("SQLite migration rejects foreign key violations") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteFkViolation.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Version1Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_FkVersion1SchemaVersions));
					co_await Version1Database.f_Open();
					co_await Version1Database.f_ApplySchema();

					co_await Version1Database.f_Insert<NDatabaseBackend::gc_InsertFkParent>(NStr::CStr("parent"));
					co_await Version1Database.f_Insert<NDatabaseBackend::gc_InsertFkChild>(int64(1));
				}

				// The v2 migration rewrites every child parent_id to a non-existent value under foreign_keys=OFF.
				// Without a foreign_key_check before commit, SQLite would accept the migration and record the
				// version despite the dangling references.
				CSqlDatabaseClient Version2Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_FkViolationSchemaVersions));
				co_await Version2Database.f_Open();
				auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
				DMibExpect(bool(ApplyResult), ==, false);

				co_return {};
			};

			DMibTestSuite("SQLite rebuild of referenced table preserves child foreign key") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteFkRebuild.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Version1Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_FkVersion1SchemaVersions));
					co_await Version1Database.f_Open();
					co_await Version1Database.f_ApplySchema();

					co_await Version1Database.f_Insert<NDatabaseBackend::gc_InsertFkParent>(NStr::CStr("parent"));
					co_await Version1Database.f_Insert<NDatabaseBackend::gc_InsertFkChild>(int64(1));
				}

				// Rebuilding the referenced parent renames it to a temporary name. Under the default
				// legacy_alter_table=OFF, SQLite would rewrite the child's foreign-key clause to that temporary
				// name, which the later drop then leaves dangling - either failing the migration's foreign-key
				// check or breaking later child inserts with "no such table".
				CSqlDatabaseClient Version2Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_FkParentRebuildSchemaVersions));
				co_await Version2Database.f_Open();
				co_await Version2Database.f_ApplySchema();

				co_await Version2Database.f_Insert<NDatabaseBackend::gc_InsertFkChild>(int64(1));

				umint nChildren = co_await Version2Database.f_Count<NDatabaseBackend::gc_SelectFkChildByParent>(int64(1));
				DMibExpect(nChildren, ==, umint(2));

				co_return {};
			};

			DMibTestSuite("SQLite savepoint commit, release, rollback-to") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				CStr const KeyOuter = "sqlite-savepoint-outer@example.com";
				CStr const KeyInnerA = "sqlite-savepoint-inner-a@example.com";
				CStr const KeyInnerB = "sqlite-savepoint-inner-b@example.com";
				CStr const KeyInnerBSibling = "sqlite-savepoint-inner-b-sibling@example.com";

				{
					auto Transaction = co_await Database.f_BeginTransaction();
					co_await Transaction.f_Insert<NDatabaseBackend::gc_InsertPerson>(KeyOuter);

					CStr NameA = co_await Transaction.f_CreateSavepoint();
					co_await Transaction.f_Insert<NDatabaseBackend::gc_InsertPerson>(KeyInnerA);

					CStr NameB = co_await Transaction.f_CreateSavepoint();
					co_await Transaction.f_Insert<NDatabaseBackend::gc_InsertPerson>(KeyInnerB);

					co_await Transaction.f_RollbackToSavepoint(NameB);
					co_await Transaction.f_Insert<NDatabaseBackend::gc_InsertPerson>(KeyInnerBSibling);
					co_await Transaction.f_ReleaseSavepoint(NameB);

					co_await Transaction.f_ReleaseSavepoint(NameA);
					co_await Transaction.f_Commit();
				}

				DMibExpect(co_await Database.template f_Exists<NDatabaseBackend::gc_SelectPersonByEmail>(KeyOuter), ==, true);
				DMibExpect(co_await Database.template f_Exists<NDatabaseBackend::gc_SelectPersonByEmail>(KeyInnerA), ==, true);
				DMibExpect(co_await Database.template f_Exists<NDatabaseBackend::gc_SelectPersonByEmail>(KeyInnerB), ==, false);
				DMibExpect(co_await Database.template f_Exists<NDatabaseBackend::gc_SelectPersonByEmail>(KeyInnerBSibling), ==, true);

				CStr const KeyAbandoned = "sqlite-savepoint-abandoned@example.com";
				{
					auto Transaction = co_await Database.f_BeginTransaction();
					CStr Name = co_await Transaction.f_CreateSavepoint();
					co_await Transaction.f_Insert<NDatabaseBackend::gc_InsertPerson>(KeyAbandoned);
					co_await Transaction.f_RollbackToSavepoint(Name);
					co_await Transaction.f_ReleaseSavepoint(Name);
					co_await Transaction.f_Commit();
				}
				DMibExpect(co_await Database.template f_Exists<NDatabaseBackend::gc_SelectPersonByEmail>(KeyAbandoned), ==, false);

				co_return {};
			};

			DMibTestSuite("SQLite read transaction rejects mutating raw queries") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				CStr const KeyQuery = "sqlite-read-raw-query@example.com";
				CStr const KeyStream = "sqlite-read-raw-stream@example.com";

				{
					DMibTestPath("QueryRaw");

					auto Transaction = co_await Database.f_BeginReadTransaction();
					auto Result = co_await Transaction.f_QueryRaw("INSERT INTO people (email) VALUES ('sqlite-read-raw-query@example.com') RETURNING email").f_Wrap();
					DMibExpect(bool(Result), ==, false);
					co_await Transaction.f_Rollback();
				}

				{
					DMibTestPath("QueryRawStream");

					auto Transaction = co_await Database.f_BeginReadTransaction();
					auto Result = co_await Transaction.f_QueryRawStream("INSERT INTO people (email) VALUES ('sqlite-read-raw-stream@example.com') RETURNING email").f_Wrap();
					DMibExpect(bool(Result), ==, false);
					co_await Transaction.f_Rollback();
				}

				DMibExpect(co_await Database.template f_Exists<NDatabaseBackend::gc_SelectPersonByEmail>(KeyQuery), ==, false);
				DMibExpect(co_await Database.template f_Exists<NDatabaseBackend::gc_SelectPersonByEmail>(KeyStream), ==, false);

				co_return {};
			};

			DMibTestSuite("SQLite raw column value types") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				CSqlRawResult NullFirstResult = co_await Database.f_QueryRaw("SELECT NULL AS value UNION ALL SELECT 'text' AS value");
				DMibExpect(NullFirstResult.m_Columns.f_GetLen(), ==, umint(1));
				if (NullFirstResult.m_Columns.f_GetLen() == 1)
					DMibExpect(NullFirstResult.m_Columns[0].m_ValueType, ==, ESqlValueType::mc_Text);

				CSqlRawStream NullFirstStream = co_await Database.f_QueryRawStream("SELECT NULL AS value UNION ALL SELECT 'text' AS value");
				DMibExpect(NullFirstStream.m_Columns.f_GetLen(), ==, umint(1));
				if (NullFirstStream.m_Columns.f_GetLen() == 1)
					DMibExpect(NullFirstStream.m_Columns[0].m_ValueType, ==, ESqlValueType::mc_Text);

				umint nStreamRows = 0;
				for (auto iBatch = co_await fg_Move(NullFirstStream.m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
					nStreamRows += (*iBatch).f_GetLen();
				DMibExpect(nStreamRows, ==, umint(2));

				co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(NStr::CStr("sqlite-raw-column-type@example.com"));

				CSqlRawStream Stream = co_await Database.f_QueryRawStream("SELECT email FROM people WHERE email = 'sqlite-raw-column-type@example.com'");
				DMibExpect(Stream.m_Columns.f_GetLen(), ==, umint(1));
				if (Stream.m_Columns.f_GetLen() == 1)
					DMibExpect(Stream.m_Columns[0].m_ValueType, ==, ESqlValueType::mc_Text);

				co_return {};
			};

			DMibTestSuite("SQLite rowid autoincrement is monotonic across inserts") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRowidAutoincrement.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				NContainer::TCVector<int64> IDs;
				for (umint i = 0; i < 16; ++i)
				{
					NStr::CStr Email = "sqlite-rowid-{}@example.com"_f << i;
					int64 ID = co_await Database.template f_InsertReturning<&NDatabaseBackend::CPersonRow::m_ID, NDatabaseBackend::gc_InsertPerson>(Email);
					IDs.f_InsertLast(ID);
				}

				DMibExpect(IDs.f_GetLen(), ==, umint(16));
				for (umint i = 1; i < IDs.f_GetLen(); ++i)
				{
					DMibTestPath("step {}"_f << i);
					DMibExpect(IDs[i], >, IDs[i - 1]);
				}

				int64 const LastID = IDs[IDs.f_GetLen() - 1];

				co_await Database.template f_DeleteByID<NDatabaseBackend::gc_PersonTable, &NDatabaseBackend::CPersonRow::m_ID>(LastID);

				int64 NextID = co_await Database.template f_InsertReturning<&NDatabaseBackend::CPersonRow::m_ID, NDatabaseBackend::gc_InsertPerson>
					(
						NStr::CStr("sqlite-rowid-post-delete@example.com")
					)
				;
				DMibExpect(NextID, >, LastID);

				co_return {};
			};

			DMibTestSuite("SQLite statement cache reuses prepared select across calls") -> TCFuture<void>
			{
				NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
				CSqlDatabaseClient Database(Backend);
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				for (umint i = 0; i < 8; ++i)
				{
					NStr::CStr Email = "sqlite-cache-{}@example.com"_f << i;
					co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(Email);
				}

				for (umint Pass = 0; Pass < 32; ++Pass)
				{
					DMibTestPath("pass {}"_f << Pass);

					umint nMatching = 0;
					for (umint i = 0; i < 8; ++i)
					{
						NStr::CStr Key = "sqlite-cache-{}@example.com"_f << i;
						auto Person = co_await Database.template f_QueryOptional<NDatabaseBackend::gc_SelectPersonByEmail>(Key);
						if (bool(Person))
							++nMatching;
					}
					DMibExpect(nMatching, ==, umint(8));

					umint nCount = co_await Database.template f_Count<NDatabaseBackend::gc_SelectPersonByEmailLike>(NStr::CStr("sqlite-cache-%"));
					DMibExpect(nCount, ==, umint(8));
				}

				co_return {};
			};

			DMibTestSuite("SQLite foreign key pragma enforces references") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteForeignKeys.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				NMib::NSQL::NTest::CUserRow User;
				User.m_Email = "sqlite-fk-user@example.com";
				co_await Database.f_Insert(NMib::NSQL::NTest::gc_UserTable, fg_TempCopy(User));

				NMib::NSQL::NTest::CUserRoleRow ValidRole;
				ValidRole.m_UserID = 1;
				ValidRole.m_Role = "admin";
				co_await Database.f_Insert(NMib::NSQL::NTest::gc_UserRoleTable, fg_TempCopy(ValidRole));

				NMib::NSQL::NTest::CUserRoleRow OrphanRole;
				OrphanRole.m_UserID = 999999;
				OrphanRole.m_Role = "ghost";
				auto Bad = co_await Database.f_Insert(NMib::NSQL::NTest::gc_UserRoleTable, fg_Move(OrphanRole)).f_Wrap();
				NDatabaseBackend::fg_TestExpectSqlError(Bad, NStr::CStr("foreign key violation"), ESqlErrorCategory::mc_ForeignKeyViolation);

				co_return {};
			};

			DMibTestSuite("SQLite rolls back a failed commit and reuses the connection") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteFailedCommit.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				// Seed a user (id 1) so the role insert in the reuse phase satisfies the foreign key.
				NMib::NSQL::NTest::CUserRow User;
				User.m_Email = "sqlite-failed-commit-user@example.com";
				co_await Database.f_Insert(NMib::NSQL::NTest::gc_UserTable, fg_TempCopy(User));

				{
					DMibTestPath("Commit fails on a deferred foreign key violation");

					auto Transaction = co_await Database.f_BeginTransaction();

					// Defer foreign key enforcement so the orphan insert succeeds but the COMMIT fails.
					co_await Transaction.f_ExecuteRaw(NStr::CStr("PRAGMA defer_foreign_keys=ON"));

					NMib::NSQL::NTest::CUserRoleRow OrphanRole;
					OrphanRole.m_UserID = 999999;
					OrphanRole.m_Role = "ghost";
					co_await Transaction.f_Insert(NMib::NSQL::NTest::gc_UserRoleTable, fg_Move(OrphanRole));

					auto CommitResult = co_await Transaction.f_Commit().f_Wrap();
					DMibExpect(bool(CommitResult), ==, false);
				}

				{
					DMibTestPath("The pooled connection is rolled back and reusable");

					// If the failed commit had left the pooled write connection inside an open
					// transaction, this BEGIN would fail with "cannot start a transaction within a
					// transaction" and the role below would never persist.
					auto Transaction = co_await Database.f_BeginTransaction();

					NMib::NSQL::NTest::CUserRoleRow ValidRole;
					ValidRole.m_UserID = 1;
					ValidRole.m_Role = "admin";
					co_await Transaction.f_Insert(NMib::NSQL::NTest::gc_UserRoleTable, fg_Move(ValidRole));

					co_await Transaction.f_Commit();
				}

				// The orphan from the failed commit must have been rolled back; the role written over
				// the reused connection must have committed.
				CSqlRawResult OrphanCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT COUNT(*) FROM user_roles WHERE user_id = 999999"));
				DMibExpect(OrphanCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

				CSqlRawResult ValidCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT COUNT(*) FROM user_roles WHERE user_id = 1"));
				DMibExpect(ValidCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

				co_return {};
			};
		}
	};

	DMibTestRegister(CSQLiteDatabase_Tests, Malterlib::SQL);
}
