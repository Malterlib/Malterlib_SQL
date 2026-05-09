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

	struct CTextKeyRow
	{
		NStr::CStr m_Key;
		NStr::CStr m_Value;
	};

	constexpr auto gc_TextKeyTable = fg_SqlTable<CTextKeyRow>
		(
			NStr::gc_Str<"text_keys">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"key">, &CTextKeyRow::m_Key, ESqlColumnFlag::mc_PrimaryKey), fg_SqlColumn(NStr::gc_Str<"value">, &CTextKeyRow::m_Value))
		)
	;
	constexpr auto gc_TextKeyDatabase = fg_SqlDatabase(NStr::gc_Str<"text_key_database">, gc_TextKeyTable);
	constexpr auto gc_TextKeySchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"text_key_0001">, gc_TextKeyDatabase);
	constexpr auto gc_TextKeySchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_TextKeySchemaVersion));

	struct CIntegerKeyRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Value;
	};

	constexpr auto gc_IntegerKeyTable = fg_SqlTable<CIntegerKeyRow>
		(
			NStr::gc_Str<"integer_keys">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CIntegerKeyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey), fg_SqlColumn(NStr::gc_Str<"value">, &CIntegerKeyRow::m_Value))
		)
	;
	constexpr auto gc_IntegerKeyDatabase = fg_SqlDatabase(NStr::gc_Str<"integer_key_database">, gc_IntegerKeyTable);
	constexpr auto gc_IntegerKeySchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"integer_key_0001">, gc_IntegerKeyDatabase);
	constexpr auto gc_IntegerKeySchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_IntegerKeySchemaVersion));

	struct CWithoutRowidOldKeyRow
	{
		NStr::CStr m_OldKey;
	};

	struct CWithoutRowidNewKeyRow
	{
		NStr::CStr m_NewKey;
	};

	constexpr auto gc_WithoutRowidOldKeyTable = fg_SqlTable<CWithoutRowidOldKeyRow>
		(
			NStr::gc_Str<"without_rowid_rebuild">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"old_key">, &CWithoutRowidOldKeyRow::m_OldKey, ESqlColumnFlag::mc_PrimaryKey))
		)
	;
	constexpr auto gc_WithoutRowidNewKeyTable = fg_SqlTable<CWithoutRowidNewKeyRow>
		(
			NStr::gc_Str<"without_rowid_rebuild">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"new_key">, &CWithoutRowidNewKeyRow::m_NewKey, ESqlColumnFlag::mc_PrimaryKey, NStr::gc_Str<"'replacement'">))
		)
	;
	constexpr auto gc_WithoutRowidV1SchemaVersion = fg_SqlSchemaVersion
		(
			NStr::gc_Str<"without_rowid_rebuild_0001">
			, fg_SqlDatabase(NStr::gc_Str<"without_rowid_rebuild_database">, gc_WithoutRowidOldKeyTable)
		)
	;
	constexpr auto gc_WithoutRowidV2SchemaVersion = fg_SqlSchemaVersion
		(
			NStr::gc_Str<"without_rowid_rebuild_0002">
			, fg_SqlDatabase(NStr::gc_Str<"without_rowid_rebuild_database">, gc_WithoutRowidNewKeyTable)
		)
	;
	constexpr auto gc_WithoutRowidV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_WithoutRowidV1SchemaVersion));
	constexpr auto gc_WithoutRowidSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_WithoutRowidV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_WithoutRowidV2SchemaVersion, fg_SqlRebuildTable(NStr::gc_Str<"without_rowid_rebuild">))
		)
	;

	constexpr auto gc_RebuildConflictV1Table = fg_SqlTable<CIntegerKeyRow>
		(
			NStr::gc_Str<"rebuild_conflict">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CIntegerKeyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey), fg_SqlColumn(NStr::gc_Str<"value">, &CIntegerKeyRow::m_Value))
		)
	;
	constexpr auto gc_RebuildConflictV2Table = fg_SqlTable<CIntegerKeyRow>
		(
			NStr::gc_Str<"rebuild_conflict">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CIntegerKeyRow::m_ID, ESqlColumnFlag::mc_PrimaryKey)
				, fg_SqlColumn
				(
					NStr::gc_Str<"value">
					, &CIntegerKeyRow::m_Value
					, fg_SqlColumnOptions
					(
						ESqlColumnFlag::mc_None
						, NStr::gc_Str<"">
						, {{.m_pBackendID = &NStr::gc_Str<"sqlite">.m_Str, .m_pCustomSql = &NStr::gc_Str<"UNIQUE ON CONFLICT IGNORE">.m_Str}}
					)
				)
			)
		)
	;
	constexpr auto gc_RebuildConflictV1SchemaVersion = fg_SqlSchemaVersion
		(
			NStr::gc_Str<"rebuild_conflict_0001">
			, fg_SqlDatabase(NStr::gc_Str<"rebuild_conflict_database">, gc_RebuildConflictV1Table)
		)
	;
	constexpr auto gc_RebuildConflictV2SchemaVersion = fg_SqlSchemaVersion
		(
			NStr::gc_Str<"rebuild_conflict_0002">
			, fg_SqlDatabase(NStr::gc_Str<"rebuild_conflict_database">, gc_RebuildConflictV2Table)
		)
	;
	constexpr auto gc_RebuildConflictV1SchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_RebuildConflictV1SchemaVersion));
	constexpr auto gc_RebuildConflictSchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(gc_RebuildConflictV1SchemaVersion)
			, fg_SqlSchemaMigration(gc_RebuildConflictV2SchemaVersion)
		)
	;

	struct CThrowingMoveRow
	{
		CThrowingMoveRow() = default;
		CThrowingMoveRow(CThrowingMoveRow const &_Other) = default;
		CThrowingMoveRow(CThrowingMoveRow &&_Other);

		CThrowingMoveRow &operator = (CThrowingMoveRow const &_Other) = default;
		CThrowingMoveRow &operator = (CThrowingMoveRow &&_Other);

		int64 m_ID = 0;
		NStr::CStr m_Value;
	};

	CThrowingMoveRow::CThrowingMoveRow(CThrowingMoveRow &&_Other)
		: m_ID(_Other.m_ID)
		, m_Value(fg_Move(_Other.m_Value))
	{
		if (m_Value == "throw")
			DMibError("Throwing row move");
	}

	CThrowingMoveRow &CThrowingMoveRow::operator = (CThrowingMoveRow &&_Other)
	{
		m_ID = _Other.m_ID;
		m_Value = fg_Move(_Other.m_Value);
		if (m_Value == "throw")
			DMibError("Throwing row move");

		return *this;
	}

	constexpr auto gc_ThrowingMoveTable = fg_SqlTable<CThrowingMoveRow>
		(
			NStr::gc_Str<"throwing_move">
			, fg_SqlColumns(fg_SqlColumn(NStr::gc_Str<"id">, &CThrowingMoveRow::m_ID, ESqlColumnFlag::mc_PrimaryKey), fg_SqlColumn(NStr::gc_Str<"value">, &CThrowingMoveRow::m_Value))
		)
	;
	constexpr auto gc_SelectThrowingMove = fg_SqlPreparedSelect(gc_ThrowingMoveTable).f_Where(fg_SqlParamEq<&CThrowingMoveRow::m_ID>());
	constexpr auto gc_ThrowingMoveSchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"throwing_move_0001">, fg_SqlDatabase(NStr::gc_Str<"throwing_move_database">, gc_ThrowingMoveTable));
	constexpr auto gc_ThrowingMoveSchemaVersions = fg_SqlSchemaVersions(fg_SqlSchemaMigration(gc_ThrowingMoveSchemaVersion));

	TCFuture<umint> fg_CountRawStreamRows(CSqlRawStream _Stream)
	{
		umint nRows = 0;
		for (auto iBatch = co_await fg_Move(_Stream.m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			nRows += (*iBatch).f_GetLen();

		co_return nRows;
	}

	auto fg_CreateSqliteBackendFactory(CStr const &_Path)
	{
		return [Path = _Path](ICSqlSchemaVersions const *_pSchemaVersions)
			{
				CSQLiteDatabaseBackendSettings Settings;
				Settings.m_Path = Path;

				return fg_CreateDatabaseBackendSQLite(_pSchemaVersions, fg_Move(Settings));
			}
		;
	}

	struct CSQLiteDatabase_Tests : public NMib::NTest::CTest
	{
		void f_DoTests();
	};

	void CSQLiteDatabase_Tests::f_DoTests()
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
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

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
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			// An empty path gives each connection its own private temporary database
			co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(NStr::CStr("empty-path@example.com"));

			umint nCount = co_await Database.f_Count<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("empty-path@example.com"));
			DMibExpect(nCount, ==, umint(1));

			co_return {};
		};

		DMibTestSuite("SQLite unique column flag is enforced") -> TCFuture<void>
		{
			NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

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

			// A file-backed database has a query-only read pool
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
			// late_value + 0 has no declared type, and its only non-NULL value lies beyond the inference cap
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

			// SQLite would bind a missing value as NULL
			auto MissingValueResult = co_await Database.f_QueryRaw(NStr::CStr("SELECT ? AS value")).f_Wrap();
			DMibExpect(bool(MissingValueResult), ==, false);

			NContainer::TCVector<CSqlValue> OneValue;
			OneValue.f_InsertLast(NStr::CStr("only-one@example.com"));
			auto TooFewResult = co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO people (email) VALUES (?), (?)"), fg_Move(OneValue)).f_Wrap();
			DMibExpect(bool(TooFewResult), ==, false);

			co_return {};
		};

		DMibTestSuite("SQLite open can be retried after WAL setup fails") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteOpenRetry.sqlite";
			auto WritableAttributes = NFile::EFileAttrib_UnixAttributesValid | NFile::EFileAttrib_UserRead | NFile::EFileAttrib_UserWrite;
			if (NFile::CFile::fs_FileExists(DatabasePath))
				NFile::CFile::fs_SetAttributes(DatabasePath, WritableAttributes);
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			// An empty file without write permission is an empty database that SQLite opens but cannot switch to WAL
			auto ReadOnlyAttributes = NFile::EFileAttrib_UnixAttributesValid | NFile::EFileAttrib_UserRead;
#ifdef DPlatformFamily_Windows
			// Windows ignores the unix permissions. On macOS the read-only attribute makes the file immutable, which keeps its mode from being restored
			ReadOnlyAttributes |= NFile::EFileAttrib_ReadOnly;
#endif
			NFile::CFile::fs_WriteFile(DatabasePath, NContainer::CByteVector());
			NFile::CFile::fs_SetAttributes(DatabasePath, ReadOnlyAttributes);
			auto RestoreWritable = g_OnScopeExit / [&]
				{
					NFile::CFile::fs_SetAttributes(DatabasePath, WritableAttributes);
				}
			;

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
			for (umint iAttempt = 0; iAttempt < 2; ++iAttempt)
			{
				DMibTestPath("Attempt {}"_f << iAttempt);

				auto OpenResult = co_await Database.f_Open().f_Wrap();
				DMibExpect(bool(OpenResult), ==, false);
				if (!OpenResult)
					DMibExpect(OpenResult.f_GetExceptionStr().f_Find("readonly") >= aint(0), ==, true);
			}

			NFile::CFile::fs_SetAttributes(DatabasePath, WritableAttributes);
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			CSqlRawResult Count = co_await Database.f_QueryRaw(CStr("SELECT count(*) AS value FROM people"));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

			co_return {};
		};

		DMibTestSuite("SQLite raw columns follow a schema change between prepare and step") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRawReprepare.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			NPrivate::CSQLiteDatabaseHandle Reader;
			NPrivate::CSQLiteDatabaseHandle Writer;
			co_await Writer.f_Open(DatabasePath);
			co_await Reader.f_Open(DatabasePath);
			co_await Writer.f_Execute(CStr("CREATE TABLE reprepare (a INTEGER)"));
			co_await Writer.f_Execute(CStr("INSERT INTO reprepare (a) VALUES (1)"));

			// The first step prepares SELECT * again for the added column
			{
				auto Statement = co_await Reader.f_PrepareRaw(CStr("SELECT * FROM reprepare"), {});
				co_await Writer.f_Execute(CStr("ALTER TABLE reprepare ADD COLUMN b INTEGER DEFAULT 2"));

				auto Result = co_await Reader.f_SelectRaw(fg_Move(Statement));
				DMibExpect(Result.m_Columns.f_GetLen(), ==, umint(2));
				DMibAssert(Result.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(Result.m_Rows[0].m_Values.f_GetLen(), ==, umint(2));
			}

			{
				auto Statement = co_await Reader.f_PrepareRaw(CStr("SELECT * FROM reprepare"), {});
				co_await Writer.f_Execute(CStr("ALTER TABLE reprepare ADD COLUMN c INTEGER DEFAULT 3"));

				auto Start = co_await NPrivate::fg_SqliteRawStreamStart(Reader, *Statement.m_pStatement);
				DMibExpect(Start.m_Columns.f_GetLen(), ==, umint(3));
				DMibAssert(Start.m_FirstRows.f_GetLen(), ==, umint(1));
				DMibExpect(Start.m_FirstRows[0].m_Values.f_GetLen(), ==, umint(3));
			}

			co_return {};
		};

		DMibTestSuite("SQLite rejects zero pipeline length") -> TCFuture<void>
		{
			CSQLiteDatabaseBackendSettings Settings;
			Settings.m_Path = ":memory:";
			Settings.m_nPipelineLength = 0;

			CSqlDatabaseClient Database(fg_CreateDatabaseBackendSQLite(&NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings)));
			auto OpenResult = co_await Database.f_Open().f_Wrap();
			DMibExpect(bool(OpenResult), ==, false);
			if (!OpenResult)
				DMibExpect(OpenResult.f_GetExceptionStr().f_Find("pipeline length must be at least one") >= aint(0), ==, true);

			co_return {};
		};

		DMibTestSuite("SQLite raw empty statement is rejected") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteEmptyStatement.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			// Empty or comment-only SQL prepares no statement; require the prepare-time error, not SQLITE_MISUSE from stepping a null statement
			auto EmptyQuery = co_await Database.f_QueryRaw(NStr::CStr("")).f_Wrap();
			DMibExpect(bool(EmptyQuery), ==, false);
			DMibExpect(EmptyQuery.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

			auto WhitespaceQuery = co_await Database.f_QueryRaw(NStr::CStr("   \n\t")).f_Wrap();
			DMibExpect(bool(WhitespaceQuery), ==, false);
			DMibExpect(WhitespaceQuery.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

			auto CommentQuery = co_await Database.f_QueryRaw(NStr::CStr("-- just a comment")).f_Wrap();
			DMibExpect(bool(CommentQuery), ==, false);
			DMibExpect(CommentQuery.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

			CSqlRawResult TrailingCommentQuery = co_await Database.f_QueryRaw
				(
					NStr::CStr
					(
						"SELECT 1 AS value; -- trailing comment\n"
						"/* and another */"
					)
				)
			;
			DMibExpect(TrailingCommentQuery.m_Rows.f_GetLen(), ==, umint(1));

			auto TrailingStatementQuery = co_await Database.f_QueryRaw
				(
					NStr::CStr
					(
						"SELECT 1 AS value; -- comment\n"
						"SELECT 2 AS value"
					)
				)
				.f_Wrap()
			;
			DMibExpect(bool(TrailingStatementQuery), ==, false);

			// SQLite skips empty statements before the first one
			CSqlRawResult LeadingEmptyQuery = co_await Database.f_QueryRaw(NStr::CStr(";SELECT 1 AS value"));
			DMibExpect(LeadingEmptyQuery.m_Rows.f_GetLen(), ==, umint(1));

			CSqlRawResult LeadingEmptiesQuery = co_await Database.f_QueryRaw
				(
					NStr::CStr
					(
						" ; -- comment\n"
						" ; SELECT 1 AS value;"
					)
				)
			;
			DMibExpect(LeadingEmptiesQuery.m_Rows.f_GetLen(), ==, umint(1));

			auto LeadingEmptyTrailingQuery = co_await Database.f_QueryRaw(NStr::CStr(";SELECT 1 AS value; SELECT 2 AS value")).f_Wrap();
			DMibExpect(bool(LeadingEmptyTrailingQuery), ==, false);

			auto SemicolonQuery = co_await Database.f_QueryRaw(NStr::CStr(";")).f_Wrap();
			DMibExpect(bool(SemicolonQuery), ==, false);

			auto TrailingPragmaExecute = co_await Database.f_ExecuteRaw(NStr::CStr("SELECT 1; PRAGMA foreign_keys=OFF")).f_Wrap();
			DMibExpect(bool(TrailingPragmaExecute), ==, false);
			auto LeadingPragmaExecute = co_await Database.f_ExecuteRaw(NStr::CStr("PRAGMA foreign_keys=OFF; SELECT 1")).f_Wrap();
			DMibExpect(bool(LeadingPragmaExecute), ==, false);
			CSqlRawResult ForeignKeys = co_await Database.f_QueryRaw(NStr::CStr("PRAGMA foreign_keys"));
			DMibExpect(ForeignKeys.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			auto EmptyExecute = co_await Database.f_ExecuteRaw(NStr::CStr("")).f_Wrap();
			DMibExpect(bool(EmptyExecute), ==, false);
			DMibExpect(EmptyExecute.f_GetExceptionStr().f_Find("no statement to execute") >= aint(0), ==, true);

			umint nInserted = co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO people (email) VALUES ('empty-stmt-after@example.com')"));
			DMibExpect(nInserted, ==, umint(1));

			co_return {};
		};

		DMibTestSuite("SQLite raw DROP TABLE of a referenced table reports zero affected rows") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRawDropReferencedTable.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE raw_drop_parent (id INTEGER PRIMARY KEY)"));
			co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE raw_drop_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES raw_drop_parent (id) ON DELETE CASCADE)"));
			co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO raw_drop_parent (id) VALUES (1), (2)"));
			co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO raw_drop_child (id, parent_id) VALUES (1, 1)"));

			umint nDropped = co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE raw_drop_parent"));
			DMibExpect(nDropped, ==, umint(0));

			umint nInserted = co_await Database.f_ExecuteRaw
				(
					NStr::CStr
					(
						" ; /* comment */ -- comment\n"
						" insert INTO people (email) VALUES ('raw-drop-after@example.com')"
					)
				)
			;
			DMibExpect(nInserted, ==, umint(1));

			umint nDeleted = co_await Database.f_ExecuteRaw
				(
					NStr::CStr("WITH Target_ AS (SELECT 'raw-drop-after@example.com' AS email) DELETE FROM people WHERE email IN (SELECT email FROM Target_)")
				)
			;
			DMibExpect(nDeleted, ==, umint(1));

			co_await Database.f_ExecuteRaw(NStr::CStr("DROP TABLE raw_drop_child"));

			co_return {};
		};

		DMibTestSuite("SQLite raw mutation after a byte order mark reports its affected rows") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			umint nInserted = co_await Database.f_ExecuteRaw(NStr::CStr("\xEF\xBB\xBF" "INSERT INTO people (email) VALUES ('raw-bom@example.com')"));
			DMibExpect(nInserted, ==, umint(1));

			umint nDeleted = co_await Database.f_ExecuteRaw(NStr::CStr(" ; \xEF\xBB\xBF DELETE FROM people WHERE email = 'raw-bom@example.com'"));
			DMibExpect(nDeleted, ==, umint(1));

			co_return {};
		};

		DMibTestSuite("SQLite mapped conversion that throws fails the operation") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_CheckedSchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			auto Repository = Database.f_Repository<NDatabaseBackend::gc_CheckedRepository>();

			DMibTestCategory("Save of a new row with an invalid member") -> TCFuture<void>
			{
				NDatabaseBackend::CCheckedRow Row;
				Row.m_Value.m_Value = -1;

				CStr Error = co_await NDatabaseBackend::fg_CallOnOtherActor
					(
						[Repository, Row = fg_Move(Row)]() mutable -> TCFuture<CStr>
						{
							auto Result = co_await Repository.f_Save(fg_Move(Row)).f_Wrap();
							co_return Result ? CStr() : Result.f_GetExceptionStr();
						}
					)
				;
				DMibExpect(Error, ==, CStr("Checked value is out of range"));

				co_return {};
			};

			NDatabaseBackend::CCheckedRow Row;
			Row.m_Value.m_Value = 5;
			auto InsertResult = co_await Repository.f_Save(fg_Move(Row));
			DMibAssert(InsertResult.m_Result, ==, ESqlSaveResult::mc_Inserted);

			DMibTestCategory("Save of an existing row with an invalid member") -> TCFuture<void>
			{
				NDatabaseBackend::CCheckedRow Invalid = InsertResult.m_Row;
				Invalid.m_Value.m_Value = -1;

				CStr Error = co_await NDatabaseBackend::fg_CallOnOtherActor
					(
						[Repository, Invalid = fg_Move(Invalid)]() mutable -> TCFuture<CStr>
						{
							auto Result = co_await Repository.f_Save(fg_Move(Invalid)).f_Wrap();
							co_return Result ? CStr() : Result.f_GetExceptionStr();
						}
					)
				;
				DMibExpect(Error, ==, CStr("Checked value is out of range"));

				co_return {};
			};

			DMibTestCategory("Insert returning an invalid generated key") -> TCFuture<void>
			{
				co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO checked_values (id, version, value) VALUES (2000000, 1, 1)"));

				CStr Error = co_await NDatabaseBackend::fg_CallOnOtherActor
					(
						[Database]() mutable -> TCFuture<CStr>
						{
							auto Result = co_await Database.f_InsertReturning<&NDatabaseBackend::CCheckedRow::m_ID, NDatabaseBackend::gc_InsertCheckedRow>
								(
									int64(1)
									, NDatabaseBackend::CCheckedValue{.m_Value = 1}
								)
								.f_Wrap()
							;
							co_return Result ? CStr() : Result.f_GetExceptionStr();
						}
					)
				;
				DMibExpect(Error, ==, CStr("Stored checked value is out of range"));

				co_return {};
			};

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
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
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
			// SQLite classifies BEGIN as read-only, so it runs on the one read connection, or on the write connection while the read connection is busy
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRawQueryLeak.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			CSQLiteDatabaseBackendSettings Settings;
			Settings.m_Path = DatabasePath;
			Settings.m_nReadConnections = 1;

			NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendSQLite(&NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings));
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			auto FirstBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
			DMibExpect(bool(FirstBegin), ==, true);
			auto SecondBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
			DMibExpect(bool(SecondBegin), ==, true);

			auto StreamResult = co_await Database.f_QueryRawStream(NStr::CStr("BEGIN")).f_Wrap();
			DMibExpect(bool(StreamResult), ==, true);
			if (StreamResult)
			{
				// Release the stream's read checkout before the next BEGIN
				for (auto iBatch = co_await fg_Move(StreamResult->m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
				}
			}
			auto ThirdBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
			DMibExpect(bool(ThirdBegin), ==, true);

			co_return {};
		};

		DMibTestSuite("SQLite raw queries use the write connection while the read pool is busy") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteBusyReadPool.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			CSQLiteDatabaseBackendSettings Settings;
			Settings.m_Path = DatabasePath;
			Settings.m_nReadConnections = 1;

			NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateDatabaseBackendSQLite(&NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings));
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			for (umint iIteration = 0; iIteration < 3; ++iIteration)
				co_await Database.f_ExecuteRaw(CStr::CFormat("INSERT INTO people (email) VALUES ('busy-read-pool-{}@example.com')") << iIteration);

			CSqlRawOperation FirstOperation;
			FirstOperation.m_Sql = "SELECT email FROM people WHERE email LIKE 'busy-read-pool-%' ORDER BY email";
			FirstOperation.m_nRowsPerBatch = 1;
			auto FirstStream = co_await Database.f_QueryRawStream(fg_Move(FirstOperation));
			auto iFirstBatch = co_await fg_Move(FirstStream.m_Rows).f_GetSimpleIterator();

			CSqlRawOperation SecondOperation;
			SecondOperation.m_Sql = "SELECT email FROM people WHERE email LIKE 'busy-read-pool-%' ORDER BY email";
			auto SecondStream = co_await Database.f_QueryRawStream(fg_Move(SecondOperation));
			umint nSecondRows = 0;
			for (auto iBatch = co_await fg_Move(SecondStream.m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				nSecondRows += (*iBatch).f_GetLen();
			DMibExpect(nSecondRows, ==, umint(3));

			auto Inserted = co_await Database.f_QueryRaw(CStr("INSERT INTO people (email) VALUES ('busy-read-pool-returning@example.com') RETURNING id"));
			DMibExpect(Inserted.m_Rows.f_GetLen(), ==, umint(1));

			for (; iFirstBatch; co_await ++iFirstBatch)
			{
			}

			co_return {};
		};

		DMibTestSuite("SQLite raw query takes the connection that frees first while both pools are busy") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteBusyBothPools.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			CSQLiteDatabaseBackendSettings Settings;
			Settings.m_Path = DatabasePath;
			Settings.m_nReadConnections = 1;

			CSqlDatabaseClient Database(fg_CreateDatabaseBackendSQLite(&NDatabaseBackend::gc_SchemaVersions, fg_Move(Settings)));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			for (umint iIteration = 0; iIteration < 3; ++iIteration)
				co_await Database.f_ExecuteRaw(CStr::CFormat("INSERT INTO people (email) VALUES ('busy-both-pools-{}@example.com')") << iIteration);

			CSqlRawOperation StreamOperation;
			StreamOperation.m_Sql = "SELECT email FROM people WHERE email LIKE 'busy-both-pools-%' ORDER BY email";
			StreamOperation.m_nRowsPerBatch = 1;
			auto Stream = co_await Database.f_QueryRawStream(fg_Move(StreamOperation));
			auto iBatch = co_await fg_Move(Stream.m_Rows).f_GetSimpleIterator();

			// The transaction holds the write connection until the query returns, so only the read connection can serve it
			auto Transaction = co_await Database.f_BeginTransaction();
			auto Query = Database.f_QueryRaw(CStr("SELECT count(*) AS value FROM people WHERE email LIKE 'busy-both-pools-%'"));

			for (; iBatch; co_await ++iBatch)
			{
			}

			CSqlRawResult Count = co_await fg_Move(Query).f_Timeout(10.0, "Timed out waiting for the read connection the stream released");
			DMibAssert(Count.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(3));

			co_await Transaction.f_Commit();

			co_return {};
		};

		DMibTestSuite("SQLite a wait for either pool leaves both lists when one pool wakes it") -> TCFuture<void>
		{
			NPrivate::CSQLiteDatabasePool ReadPool;
			NPrivate::CSQLiteDatabasePool WritePool;

			auto First = NPrivate::fg_SqliteWaitForEitherPool(ReadPool, WritePool);
			auto Second = NPrivate::fg_SqliteWaitForEitherPool(ReadPool, WritePool);

			NPrivate::fg_SqliteWakePoolWaiter(WritePool, ReadPool);
			DMibExpect(ReadPool.m_Waiters.f_GetLen(), ==, umint(1));
			DMibExpect(WritePool.m_Waiters.f_GetLen(), ==, umint(1));
			co_await fg_Move(First).f_Timeout(10.0, "Timed out waiting for the first waiter");

			NPrivate::fg_SqliteWakePoolWaiter(ReadPool, WritePool);
			DMibExpect(ReadPool.m_Waiters.f_GetLen(), ==, umint(0));
			DMibExpect(WritePool.m_Waiters.f_GetLen(), ==, umint(0));
			co_await fg_Move(Second).f_Timeout(10.0, "Timed out waiting for the second waiter");

			co_return {};
		};

		DMibTestSuite("SQLite read issued while the database opens gets a read connection") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteReadDuringOpen.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();
				co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(NStr::CStr("read-during-open@example.com"));
			}

			// The read reaches the backend while it opens the connections, and waits for an empty read pool
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
			auto Open = Database.f_Open();
			auto Count = Database.f_Count<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("read-during-open@example.com"));
			co_await fg_Move(Open);

			umint nCount = co_await fg_Move(Count).f_Timeout(10.0, "Timed out waiting for a read connection after opening");
			DMibExpect(nCount, ==, umint(1));

			co_return {};
		};

		DMibTestSuite("SQLite read issued while the database opens fails when opening fails") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteReadDuringFailedOpen" / "Missing" / "Database.sqlite";

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
			auto Open = Database.f_Open();
			auto Count = Database.f_Count<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("read-during-failed-open@example.com"));

			auto OpenResult = co_await fg_Move(Open).f_Wrap();
			DMibExpect(bool(OpenResult), ==, false);

			auto CountResult = co_await fg_Move(Count).f_Timeout(10.0, "Timed out waiting for a read queued during a failed open").f_Wrap();
			DMibAssert(bool(CountResult), ==, false);
			DMibExpect(CountResult.f_GetExceptionStr(), ==, OpenResult.f_GetExceptionStr());

			co_return {};
		};

		DMibTestSuite("SQLite operations issued while the database opens fail when opening fails") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteWriteDuringFailedOpen" / "Missing" / "Database.sqlite";

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));
			auto Open = Database.f_Open();
			auto Execute = Database.f_ExecuteRaw(CStr("CREATE TABLE never_created (id INTEGER)"));
			auto Query = Database.f_QueryRaw(CStr("SELECT 1 AS value"));

			auto OpenResult = co_await fg_Move(Open).f_Wrap();
			DMibExpect(bool(OpenResult), ==, false);

			auto ExecuteResult = co_await fg_Move(Execute).f_Timeout(10.0, "Timed out waiting for a write queued during a failed open").f_Wrap();
			DMibAssert(bool(ExecuteResult), ==, false);
			DMibExpect(ExecuteResult.f_GetExceptionStr().f_Find("SQLite database is not open") >= aint(0), ==, true);

			auto QueryResult = co_await fg_Move(Query).f_Timeout(10.0, "Timed out waiting for a raw query queued during a failed open").f_Wrap();
			DMibAssert(bool(QueryResult), ==, false);
			DMibExpect(QueryResult.f_GetExceptionStr().f_Find("SQLite database is not open") >= aint(0), ==, true);

			auto CountResult = co_await Database.f_Count<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("after-failed-open@example.com"))
				.f_Timeout(10.0, "Timed out waiting for a read after a failed open")
				.f_Wrap()
			;
			DMibAssert(bool(CountResult), ==, false);
			DMibExpect(CountResult.f_GetExceptionStr().f_Find("SQLite database is not open") >= aint(0), ==, true);

			co_return {};
		};

		DMibTestSuite("SQLite raw SQL page limit does not carry over to the next user") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			CSqlRawResult Before = co_await Database.f_QueryRaw(CStr("PRAGMA max_page_count"));
			DMibAssert(Before.m_Rows.f_GetLen(), ==, umint(1));

			// SQLite raises a limit below the database's size to its current page count
			CSqlRawResult Limited = co_await Database.f_QueryRaw(CStr("PRAGMA max_page_count=1"));
			DMibAssert(Limited.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(Limited.m_Rows[0].m_Values[0].f_GetAsType<int64>(), <, Before.m_Rows[0].m_Values[0].f_GetAsType<int64>());

			CSqlRawResult After = co_await Database.f_QueryRaw(CStr("PRAGMA max_page_count"));
			DMibAssert(After.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(After.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, Before.m_Rows[0].m_Values[0].f_GetAsType<int64>());

			co_return {};
		};

		DMibTestSuite("SQLite raw SQL schema trust settings do not carry over to the next user") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			CSqlRawResult TrustedBefore = co_await Database.f_QueryRaw(CStr("PRAGMA trusted_schema"));
			DMibAssert(TrustedBefore.m_Rows.f_GetLen(), ==, umint(1));
			bool bTrustedBefore = TrustedBefore.m_Rows[0].m_Values[0].f_GetAsType<int64>() != 0;

			co_await Database.f_ExecuteRaw(CStr("PRAGMA writable_schema=ON"));
			co_await Database.f_ExecuteRaw(CStr(bTrustedBefore ? "PRAGMA trusted_schema=OFF" : "PRAGMA trusted_schema=ON"));

			CSqlRawResult WritableAfter = co_await Database.f_QueryRaw(CStr("PRAGMA writable_schema"));
			DMibAssert(WritableAfter.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(WritableAfter.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

			CSqlRawResult TrustedAfter = co_await Database.f_QueryRaw(CStr("PRAGMA trusted_schema"));
			DMibAssert(TrustedAfter.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(TrustedAfter.m_Rows[0].m_Values[0].f_GetAsType<int64>() != 0, ==, bTrustedBefore);

			co_return {};
		};

		DMibTestSuite("SQLite raw SQL performance and storage settings do not carry over to the next user") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			static constexpr ch8 const *c_Pragmas[] = {"automatic_index", "cache_size", "cell_size_check", "secure_delete"};
			static constexpr ch8 const *c_ChangeSql[] = {"PRAGMA automatic_index=OFF", "PRAGMA cache_size=123", "PRAGMA cell_size_check=ON", "PRAGMA secure_delete=FAST"};
			for (umint iPragma = 0; iPragma < 4; ++iPragma)
			{
				DMibTestPath(CStr(c_Pragmas[iPragma]));

				CStr QuerySql = "PRAGMA {}"_f << c_Pragmas[iPragma];
				CSqlRawResult Before = co_await Database.f_QueryRaw(fg_TempCopy(QuerySql));
				DMibAssert(Before.m_Rows.f_GetLen(), ==, umint(1));

				// A single-connection database runs the transaction and the queries after it on the same connection
				auto Transaction = co_await Database.f_BeginTransaction();
				co_await Transaction.f_ExecuteRaw(CStr(c_ChangeSql[iPragma]));
				CSqlRawResult Changed = co_await Transaction.f_QueryRaw(fg_TempCopy(QuerySql));
				co_await Transaction.f_Commit();
				DMibAssert(Changed.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(Changed.m_Rows[0].m_Values[0].f_GetAsType<int64>(), !=, Before.m_Rows[0].m_Values[0].f_GetAsType<int64>());

				CSqlRawResult After = co_await Database.f_QueryRaw(fg_TempCopy(QuerySql));
				DMibAssert(After.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(After.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, Before.m_Rows[0].m_Values[0].f_GetAsType<int64>());
			}

			co_return {};
		};

		DMibTestSuite("SQLite single-connection raw query transaction-control does not leak") -> TCFuture<void>
		{
			// On a single-connection database (no read pool) f_QueryRaw takes the write fallback. A BEGIN left open
			// there blocks the next real transaction, so f_WithTransaction must still start and commit afterwards.
			NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			auto RawBegin = co_await Database.f_QueryRaw(NStr::CStr("BEGIN")).f_Wrap();
			DMibExpect(bool(RawBegin), ==, true);

			// The pragma takes effect while the statement is prepared, before the extra parameter fails it
			NContainer::TCVector<CSqlValue> ExtraParameters;
			ExtraParameters.f_InsertLast(int64(1));
			auto FailedPragma = co_await Database.f_ExecuteRaw(NStr::CStr("PRAGMA foreign_keys=OFF"), fg_Move(ExtraParameters)).f_Wrap();
			DMibExpect(bool(FailedPragma), ==, false);
			CSqlRawResult ForeignKeys = co_await Database.f_QueryRaw(NStr::CStr("PRAGMA foreign_keys"));
			DMibExpect(ForeignKeys.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			co_await Database.f_ExecuteRaw(NStr::CStr("PRAGMA full_column_names=ON"));
			co_await Database.f_ExecuteRaw(NStr::CStr("PRAGMA short_column_names=OFF"));
			CSqlRawResult ColumnNames = co_await Database.f_QueryRaw(NStr::CStr("SELECT email FROM people"));
			DMibAssert(ColumnNames.m_Columns.f_GetLen(), ==, umint(1));
			DMibExpect(ColumnNames.m_Columns[0].m_Name, ==, CStr("email"));

			// A temporary table of the same name would otherwise take the typed insert
			co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TEMP TABLE people (id INTEGER PRIMARY KEY, email TEXT)"));
			co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(NStr::CStr("temp-shadow@example.com"));

			// SQLite's own temp.sqlite_sequence cannot be dropped, a temporary trigger acts on main's table, and query_only
			// would make dropping the temporary objects fail
			co_await Database.f_WithTransaction
				(
					[](CSqlTransaction _Transaction) -> NConcurrency::TCFuture<void>
					{
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY AUTOINCREMENT)"));
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("INSERT INTO scratch DEFAULT VALUES"));
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("CREATE TEMP TRIGGER ignore_people BEFORE INSERT ON main.people BEGIN SELECT RAISE(IGNORE); END"));
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("CREATE TEMP TABLE temp_parent (id INTEGER PRIMARY KEY)"));
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("CREATE TEMP TABLE temp_child (parent INTEGER REFERENCES temp_parent(id))"));
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("INSERT INTO temp_parent VALUES (1)"));
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("INSERT INTO temp_child VALUES (1)"));
						co_await _Transaction.f_ExecuteRaw(NStr::CStr("PRAGMA query_only=ON"));

						co_return {};
					}
				)
			;
			co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(NStr::CStr("temp-shadow@example.com"));
			CSqlRawResult MainCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) FROM main.people WHERE email = 'temp-shadow@example.com'"));
			DMibExpect(MainCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

			auto TransactionResult = co_await Database.f_WithTransaction
				(
					[](CSqlTransaction _Transaction) -> NConcurrency::TCFuture<void>
					{
						co_return {};
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(TransactionResult), ==, true);

			co_await Database.f_ExecuteRaw(NStr::CStr("PRAGMA journal_mode=OFF"));
			auto RolledBack = co_await Database.f_WithTransaction
				(
					[](CSqlTransaction _Transaction) -> NConcurrency::TCFuture<void>
					{
						co_await _Transaction.f_Insert<NDatabaseBackend::gc_InsertPerson>(NStr::CStr("journal-off@example.com"));
						co_return DMibErrorInstance("Roll back");
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(RolledBack), ==, false);
			CSqlRawResult JournalOffCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) FROM main.people WHERE email = 'journal-off@example.com'"));
			DMibExpect(JournalOffCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

			// Opening an open in-memory database again keeps its contents
			co_await Database.f_Open();
			CSqlRawResult ReopenedCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) FROM main.people WHERE email = 'temp-shadow@example.com'"));
			DMibExpect(ReopenedCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

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

		DMibTestSuite("SQLite rebuild refuses to drop a column the schema only omits") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteImplicitDrop.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);
			auto fCreateBackend = fg_CreateSqliteBackendFactory(DatabasePath);

			{
				CSqlDatabaseClient Version1Database(fCreateBackend(&NDatabaseBackend::gc_ImplicitDropV1SchemaVersions));
				co_await Version1Database.f_Open();
				co_await Version1Database.f_ApplySchema();
			}

			CSqlDatabaseClient Version2Database(fCreateBackend(&NDatabaseBackend::gc_ImplicitDropSchemaVersions));
			co_await Version2Database.f_Open();
			auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
			DMibExpect(bool(ApplyResult), ==, false);

			co_return {};
		};

		DMibTestSuite("SQLite rebuilds a table whose CHECK changes under the same name") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteChangedCheck.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);
			auto fCreateBackend = fg_CreateSqliteBackendFactory(DatabasePath);

			{
				CSqlDatabaseClient Version1Database(fCreateBackend(&NDatabaseBackend::gc_ChangedCheckV1SchemaVersions));
				co_await Version1Database.f_Open();
				co_await Version1Database.f_ApplySchema();
			}

			CSqlDatabaseClient Version2Database(fCreateBackend(&NDatabaseBackend::gc_ChangedCheckSchemaVersions));
			co_await Version2Database.f_Open();
			co_await Version2Database.f_ApplySchema();

			auto ShortResult = co_await Version2Database.f_Insert<NDatabaseBackend::gc_InsertChangedCheckV2>(CStr("abc")).f_Wrap();
			DMibExpect(bool(ShortResult), ==, false);

			co_await Version2Database.f_Insert<NDatabaseBackend::gc_InsertChangedCheckV2>(CStr("abcd"));

			co_return {};
		};

		DMibTestSuite("SQLite renames a column used by a CHECK") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRenamedCheck.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseRenameColumnWithCheck(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite adopts an existing untracked table") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptUntrackedTable.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseAdoptUntrackedExistingTable(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite upsert into a table named excluded") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteUpsertExcludedName.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseUpsertIntoTableNamedExcluded(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite schema adoption rejects undeclared constraints") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptUndeclaredConstraints.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseAdoptRejectsUndeclaredConstraints(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite schema adoption keeps foreign key behavior") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptForeignKeyBehavior.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseAdoptKeepsForeignKeyBehavior(fg_CreateSqliteBackendFactory(DatabasePath), false);

			co_return {};
		};

		DMibTestSuite("SQLite schema rejects a view named like a declared table") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteViewNamedLikeTable.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseRejectsViewNamedLikeTable(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite schema adoption rejects a unique key with a coarser collation") -> TCFuture<void>
		{
			// Rebuilt with the declared BINARY collation, the key would accept values that differ only in case
			static constexpr ch8 const *c_CreateSqls[] =
				{
					"CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY, value TEXT NOT NULL COLLATE NOCASE UNIQUE)"
					, "CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY, value TEXT NOT NULL, UNIQUE (value COLLATE NOCASE))"
					, "CREATE TABLE adopt_existing (id INTEGER NOT NULL, value TEXT NOT NULL COLLATE NOCASE PRIMARY KEY)"
				}
			;

			for (umint iCreateSql = 0; iCreateSql < fg_ArraySize(c_CreateSqls); ++iCreateSql)
			{
				ch8 const *pCreateSql = c_CreateSqls[iCreateSql];
				DMibTestPath(CStr(pCreateSql));

				// The clients of the earlier iterations can still have their database open, and Windows can't delete an open file
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / CStr("TestSQLiteAdoptUniqueCollation{}.sqlite"_f << iCreateSql);
				fg_DeleteSqliteDatabaseFiles(DatabasePath);
				{
					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptSchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ExecuteRaw(CStr(pCreateSql));
					co_await Database.f_ExecuteRaw(CStr("INSERT INTO adopt_existing (id, value) VALUES (1, 'Name')"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptSchemaVersions));
				co_await Database.f_Open();
				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				DMibExpect(bool(ApplyResult), ==, false);
				if (!ApplyResult)
					DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("COLLATE NOCASE") >= aint(0), ==, true);

				auto DuplicateResult = co_await Database.f_ExecuteRaw(CStr("INSERT INTO adopt_existing (id, value) VALUES (2, 'name')")).f_Wrap();
				DMibExpect(bool(DuplicateResult), ==, false);
			}

			co_return {};
		};

		DMibTestSuite("SQLite rebuild keeps the rowids of a table without an INTEGER PRIMARY KEY") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRebuildKeepsRowids.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptNullableSchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ExecuteRaw(CStr("CREATE TABLE adopt_nullable_parent (code TEXT NOT NULL PRIMARY KEY)"));
				co_await Database.f_ExecuteRaw(CStr("INSERT INTO adopt_nullable_parent (rowid, code) VALUES (3, 'first'), (11, 'second')"));
			}

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptNullableSchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			auto Rows = co_await Database.f_QueryRaw(CStr("SELECT rowid, code FROM adopt_nullable_parent ORDER BY rowid"));
			DMibAssert(Rows.m_Rows.f_GetLen(), ==, umint(2));
			DMibExpect(Rows.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(3));
			DMibExpect(Rows.m_Rows[0].m_Values[1].f_GetAsType<CStr>(), ==, CStr("first"));
			DMibExpect(Rows.m_Rows[1].m_Values[0].f_GetAsType<int64>(), ==, int64(11));
			DMibExpect(Rows.m_Rows[1].m_Values[1].f_GetAsType<CStr>(), ==, CStr("second"));

			co_return {};
		};

		DMibTestSuite("SQLite rebuild fails on rows that conflict under an ON CONFLICT IGNORE constraint") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRebuildConflictIgnore.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_RebuildConflictV1SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();
				co_await Database.f_ExecuteRaw(CStr("INSERT INTO rebuild_conflict (id, value) VALUES (1, 'same'), (2, 'same')"));
			}

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_RebuildConflictSchemaVersions));
			co_await Database.f_Open();
			auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
			DMibExpect(bool(ApplyResult), ==, false);

			CSqlRawResult Count = co_await Database.f_QueryRaw(CStr("SELECT count(*) AS value FROM rebuild_conflict"));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

			co_return {};
		};

		DMibTestSuite("SQLite query of a row type whose move throws fails the query") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&gc_ThrowingMoveSchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();
			co_await Database.f_ExecuteRaw(CStr("INSERT INTO throwing_move (id, value) VALUES (1, 'throw')"));

			{
				DMibTestPath("Vector");
				auto Result = co_await Database.f_QueryVector<gc_SelectThrowingMove>(int64(1)).f_Wrap();
				DMibAssert(bool(Result), ==, false);
				DMibExpect(Result.f_GetExceptionStr(), ==, CStr("Throwing row move"));
			}

			{
				DMibTestPath("Optional");
				auto Result = co_await Database.f_QueryOptional<gc_SelectThrowingMove>(int64(1)).f_Wrap();
				DMibAssert(bool(Result), ==, false);
				DMibExpect(Result.f_GetExceptionStr(), ==, CStr("Throwing row move"));
			}

			{
				DMibTestPath("One");
				auto Result = co_await Database.f_QueryOne<gc_SelectThrowingMove>(int64(1)).f_Wrap();
				DMibAssert(bool(Result), ==, false);
				DMibExpect(Result.f_GetExceptionStr(), ==, CStr("Throwing row move"));
			}

			co_return {};
		};

		DMibTestSuite("SQLite index definition change") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteIndexDefinitionChange.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseIndexDefinitionChange(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite index check reads quoted collations as the catalog does")
		{
			using NMib::NSQL::NPrivate::fg_SqliteCollationName;

			DMibExpect(fg_SqliteCollationName("nocase"), ==, CStr("NOCASE"));
			DMibExpect(fg_SqliteCollationName("\"NoCase\""), ==, CStr("NOCASE"));
			DMibExpect(fg_SqliteCollationName("'rtrim'"), ==, CStr("RTRIM"));
			DMibExpect(fg_SqliteCollationName("[binary]"), ==, CStr("BINARY"));
			DMibExpect(fg_SqliteCollationName("`a``b`"), ==, CStr("A`B"));
			DMibExpect(fg_SqliteCollationName("\"a\"\"b\""), ==, CStr("A\"B"));
		};

		DMibTestSuite("SQLite backfill of an added column") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteBackfillAddedColumn.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseBackfillOfAddedColumn(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite adopted table enforces a column constraint") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptColumnConstraint.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseAdoptExistingColumnConstraint(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite adopts a table missing a declared constraint") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptMissingConstraint.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient UntrackedDatabase(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptUniqueSchemaVersions));
				co_await UntrackedDatabase.f_Open();
				co_await UntrackedDatabase.f_ExecuteRaw(NStr::CStr("CREATE TABLE \"adopt_unique\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"code\" TEXT NOT NULL)"));
				co_await UntrackedDatabase.f_ExecuteRaw(NStr::CStr("INSERT INTO \"adopt_unique\" (\"code\") VALUES ('first')"));
			}

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptUniqueSchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			DMibTestPath("Adopted table enforces its declared UNIQUE constraint");

			CSqlRawResult AdoptedCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_unique"));
			DMibExpect(AdoptedCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			auto DuplicateResult = co_await Database.template f_Insert<NDatabaseBackend::gc_InsertAdoptUnique>(NStr::CStr("first")).f_Wrap();
			DMibExpect(bool(DuplicateResult), ==, false);

			co_await Database.template f_Insert<NDatabaseBackend::gc_InsertAdoptUnique>(NStr::CStr("second"));

			co_return {};
		};

		DMibTestSuite("SQLite adoption keeps conflict resolution") -> TCFuture<void>
		{
			struct CConflictCase
			{
				ch8 const *m_pName;
				ch8 const *m_pColumns;
				ch8 const *m_pExpectedInError; // A part of the error naming the clause, or nullptr when the adoption succeeds
			};

			static constexpr CConflictCase c_Cases[] =
				{
					{"UniqueIgnore", "\"code\" TEXT NOT NULL UNIQUE ON CONFLICT IGNORE", "\"code\" TEXT NOT NULL UNIQUE ON CONFLICT IGNORE"}
					, {"NotNullFail", "\"code\" TEXT NOT NULL ON CONFLICT FAIL UNIQUE", "NOT NULL ON CONFLICT FAIL"}
					, {"TableUniqueReplace", "\"code\" TEXT NOT NULL, UNIQUE (\"code\") ON CONFLICT REPLACE", "UNIQUE (\"code\") ON CONFLICT REPLACE"}
					, {"TableUniqueAbort", "\"code\" TEXT NOT NULL, UNIQUE (\"code\") ON CONFLICT ABORT", nullptr}
				}
			;

			umint iCase = 0;
			for (auto const &Case : c_Cases)
			{
				DMibTestPath(CStr(Case.m_pName));
				++iCase;

				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / (CStr::CFormat("TestSQLiteAdoptConflictResolution{}.sqlite") << iCase);
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient UntrackedDatabase(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptUniqueSchemaVersions));
					co_await UntrackedDatabase.f_Open();
					co_await UntrackedDatabase.f_ExecuteRaw(CStr::CFormat("CREATE TABLE \"adopt_unique\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, {})") << Case.m_pColumns);
					co_await UntrackedDatabase.f_ExecuteRaw(NStr::CStr("INSERT INTO \"adopt_unique\" (\"code\") VALUES ('first')"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptUniqueSchemaVersions));
				co_await Database.f_Open();

				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				if (Case.m_pExpectedInError)
				{
					DMibAssert(bool(ApplyResult), ==, false);
					DMibExpect(ApplyResult.f_GetExceptionStr().f_Find(Case.m_pExpectedInError) >= aint(0), ==, true);
				}
				else
					DMibExpect(bool(ApplyResult), ==, true);

				CSqlRawResult Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM adopt_unique"));
				DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));
			}

			co_return {};
		};

		DMibTestSuite("SQLite schema adoption rejects an undeclared NOT NULL on a generated column") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptGeneratedNotNull.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_GenColV1SchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ExecuteRaw
					(
						CStr
						(
							"CREATE TABLE gen_col_test (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL, "
							"value_lower TEXT GENERATED ALWAYS AS (lower(value)) STORED NOT NULL)"
						)
					)
				;
				co_await Database.f_ExecuteRaw(CStr("INSERT INTO gen_col_test (value) VALUES ('Name')"));
			}

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_GenColV1SchemaVersions));
			co_await Database.f_Open();
			auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
			DMibAssert(bool(ApplyResult), ==, false);
			DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("value_lower") >= aint(0), ==, true);

			CSqlRawResult NotNull = co_await Database.f_QueryRaw(CStr("SELECT \"notnull\" FROM pragma_table_xinfo('gen_col_test') WHERE name = 'value_lower'"));
			DMibAssert(NotNull.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(NotNull.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

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

		DMibTestSuite("SQLite transaction ended by SQLite stays ended") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&gc_TextKeySchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO text_keys (key, value) VALUES ('taken', 'first')"));

			auto Transaction = co_await Database.f_BeginTransaction();
			co_await Transaction.f_ExecuteRaw(NStr::CStr("INSERT INTO text_keys (key, value) VALUES ('in-transaction', 'value')"));

			// OR ROLLBACK makes SQLite roll back the whole transaction on the conflict
			auto ConflictResult = co_await Transaction.f_ExecuteRaw(NStr::CStr("INSERT OR ROLLBACK INTO text_keys (key, value) VALUES ('taken', 'second')")).f_Wrap();
			DMibExpect(bool(ConflictResult), ==, false);

			auto AfterEndResult = co_await Transaction.f_ExecuteRaw(NStr::CStr("INSERT INTO text_keys (key, value) VALUES ('after-end', 'value')")).f_Wrap();
			DMibExpect(bool(AfterEndResult), ==, false);

			auto CommitResult = co_await Transaction.f_Commit().f_Wrap();
			DMibExpect(bool(CommitResult), ==, false);
			co_await Transaction.f_Rollback();

			CSqlRawResult Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT COUNT(*) FROM text_keys"));
			DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			co_return {};
		};

		DMibTestSuite("SQLite non-integer primary key rejects NULL") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&gc_TextKeySchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			auto NullKeyResult = co_await Database.f_ExecuteRaw(NStr::CStr("INSERT INTO text_keys (key, value) VALUES (NULL, 'value')")).f_Wrap();
			DMibExpect(bool(NullKeyResult), ==, false);

			co_return {};
		};

		DMibTestSuite("SQLite narrow autoincrement key") -> TCFuture<void>
		{
			co_await NDatabaseBackend::fg_TestSqlDatabaseNarrowAutoIncrementKey(fg_CreateSqliteBackendFactory(":memory:"));

			co_return {};
		};

		DMibTestSuite("SQLite narrow generated key outside its field fails the insert") -> TCFuture<void>
		{
			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_NarrowKeySchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			// SQLite generates the next key past the largest one, which the int32 field cannot hold
			co_await Database.f_ExecuteRaw(CStr("INSERT INTO narrow_keys (id, value) VALUES (2147483647, 'largest')"));

			auto StandaloneResult = co_await Database.template f_InsertReturning<&NDatabaseBackend::CNarrowKeyRow::m_ID, NDatabaseBackend::gc_InsertNarrowKey>(CStr("outside-standalone"))
				.f_Wrap()
			;
			DMibExpect(bool(StandaloneResult), ==, false);

			{
				auto Transaction = co_await Database.f_BeginTransaction();
				auto TransactionResult = co_await Transaction.template f_InsertReturning<&NDatabaseBackend::CNarrowKeyRow::m_ID, NDatabaseBackend::gc_InsertNarrowKey>
					(
						CStr("outside-transaction")
					)
					.f_Wrap()
				;
				DMibExpect(bool(TransactionResult), ==, false);

				// The transaction goes on without the failed insert
				co_await Transaction.f_ExecuteRaw(CStr("INSERT INTO narrow_keys (id, value) VALUES (1, 'after-outside')"));
				co_await Transaction.f_Commit();
			}

			CSqlRawResult Outside = co_await Database.f_QueryRaw(CStr("SELECT count(*) FROM narrow_keys WHERE value LIKE 'outside-%'"));
			DMibExpect(Outside.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

			CSqlRawResult After = co_await Database.f_QueryRaw(CStr("SELECT count(*) FROM narrow_keys WHERE value = 'after-outside'"));
			DMibExpect(After.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			co_return {};
		};

		DMibTestSuite("SQLite rebuild keeps table guarantees") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseRebuildKeepsTableGuarantees.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseRebuildKeepsTableGuarantees(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite schema apply restores legacy_alter_table") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseRestoresLegacyAlterTable.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient Version1Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildVersion1SchemaVersions));
				co_await Version1Database.f_Open();
				co_await Version1Database.f_ApplySchema();
			}

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildSchemaVersions));
			co_await Database.f_Open();

			// Raw SQL restores the session settings, so only the schema apply below can leave legacy_alter_table ON. A user table with
			// the rebuild's scratch name makes the rebuild fail after it turned legacy_alter_table ON.
			co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE \"legacy_parent\" (id INTEGER PRIMARY KEY)"));
			co_await Database.f_ExecuteRaw(NStr::CStr("CREATE VIEW \"legacy_view\" AS SELECT id FROM \"legacy_parent\""));
			co_await Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE \"__mib_rebuild_old_rebuild_test\" (id INTEGER)"));

			auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
			DMibExpectFalse(bool(ApplyResult));

			// With legacy_alter_table ON a rename leaves views naming the old table
			co_await Database.f_ExecuteRaw(NStr::CStr("ALTER TABLE \"legacy_parent\" RENAME TO \"legacy_parent_renamed\""));

			CSqlRawResult ViewSchema = co_await Database.f_QueryRaw(NStr::CStr("SELECT sql FROM sqlite_master WHERE name = 'legacy_view'"));
			DMibAssert(ViewSchema.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(ViewSchema.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>().f_Find("legacy_parent_renamed") >= aint(0), ==, true);

			co_return {};
		};

		DMibTestSuite("SQLite schema migration rebuilds a referenced table") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseForeignKeyRebuildMigration.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseForeignKeyRebuildMigration(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite schema upgrade recreates a dropped table with its foreign keys") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRecreatedTableForeignKeys.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseRecreatedTableForeignKeys(fg_CreateSqliteBackendFactory(DatabasePath), NStr::CStr("DROP TABLE fk_parent"));

			co_return {};
		};

		DMibTestSuite("SQLite schema migration rejects dropping a referenced table") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDropReferencedTable.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseDropReferencedTable(fg_CreateSqliteBackendFactory(DatabasePath));

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

		DMibTestSuite("SQLite schema migration drop column keeps undeclared columns") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseDropColumnUndeclaredMigration.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			co_await NDatabaseBackend::fg_TestSqlDatabaseDropColumnKeepsUndeclaredColumn(fg_CreateSqliteBackendFactory(DatabasePath));

			co_return {};
		};

		DMibTestSuite("SQLite schema migration keeps live guarantees") -> TCFuture<void>
		{
			DMibTestCategory("Rebuild keeps undeclared columns") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRebuildUndeclaredColumn.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseRebuildKeepsUndeclaredColumn(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestCategory("Drop column keeps a column a later operation drops") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDropLaterColumn.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseDropColumnKeepsLaterDroppedColumn(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestCategory("Rebuild keeps a column a later operation drops") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRebuildDropLaterColumn.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseRebuildKeepsLaterDroppedColumn(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestCategory("Gap in the applied history") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteHistoryGap.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				co_await NDatabaseBackend::fg_TestSqlDatabaseHistoryGapValidation(fg_CreateSqliteBackendFactory(DatabasePath));

				co_return {};
			};

			DMibTestCategory("Adoption keeps a STRICT table STRICT") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptStrict.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptSchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ExecuteRaw(CStr("CREATE TABLE adopt_existing (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL) STRICT"));
					co_await Database.f_ExecuteRaw(CStr("INSERT INTO adopt_existing (id, value) VALUES (1, 'seed')"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_AdoptSchemaVersions));
				co_await Database.f_Open();
				co_await Database.f_ApplySchema();

				CSqlRawResult Strict = co_await Database.f_QueryRaw(CStr("SELECT strict FROM pragma_table_list WHERE schema = 'main' AND name = 'adopt_existing'"));
				DMibAssert(Strict.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(Strict.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

				auto BlobResult = co_await Database.f_ExecuteRaw(CStr("INSERT INTO adopt_existing (id, value) VALUES (2, X'00')")).f_Wrap();
				DMibExpect(bool(BlobResult), ==, false);

				CSqlRawResult Count = co_await Database.f_QueryRaw(CStr("SELECT count(*) FROM adopt_existing"));
				DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

				co_return {};
			};

			DMibTestCategory("Adoption keeps a WITHOUT ROWID table WITHOUT ROWID") -> TCFuture<void>
			{
				// A rowid table would generate a key for an insert that omits its INTEGER PRIMARY KEY
				static constexpr ch8 const *c_Options[] = {"WITHOUT ROWID", "STRICT, WITHOUT ROWID"};
				for (umint iOptions = 0; iOptions < 2; ++iOptions)
				{
					ch8 const *pOptions = c_Options[iOptions];
					DMibTestPath(CStr(pOptions));

					CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / (CStr::CFormat("TestSQLiteAdoptWithoutRowid{}.sqlite") << iOptions);
					fg_DeleteSqliteDatabaseFiles(DatabasePath);

					{
						CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_IntegerKeySchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ExecuteRaw(CStr::CFormat("CREATE TABLE integer_keys (id INTEGER PRIMARY KEY, value TEXT NOT NULL) {}") << pOptions);
						co_await Database.f_ExecuteRaw(CStr("INSERT INTO integer_keys (id, value) VALUES (1, 'seed')"));
					}

					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_IntegerKeySchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ApplySchema();

					CSqlRawResult TableList = co_await Database.f_QueryRaw(CStr("SELECT wr FROM pragma_table_list WHERE schema = 'main' AND name = 'integer_keys'"));
					DMibAssert(TableList.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(TableList.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

					auto MissingKeyResult = co_await Database.f_ExecuteRaw(CStr("INSERT INTO integer_keys (value) VALUES ('missing-key')")).f_Wrap();
					DMibExpect(bool(MissingKeyResult), ==, false);

					CSqlRawResult Count = co_await Database.f_QueryRaw(CStr("SELECT count(*) FROM integer_keys"));
					DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));
				}

				co_return {};
			};

			DMibTestCategory("Rebuild replacing every column of a WITHOUT ROWID table") -> TCFuture<void>
			{
				for (umint nRows = 0; nRows < 2; ++nRows)
				{
					DMibTestPath("{} rows"_f << nRows);

					CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / (CStr::CFormat("TestSQLiteRebuildWithoutRowid{}.sqlite") << nRows);
					fg_DeleteSqliteDatabaseFiles(DatabasePath);

					{
						CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_WithoutRowidV1SchemaVersions));
						co_await Database.f_Open();
						co_await Database.f_ExecuteRaw(CStr("CREATE TABLE without_rowid_rebuild (old_key TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID"));
						if (nRows != 0)
							co_await Database.f_ExecuteRaw(CStr("INSERT INTO without_rowid_rebuild (old_key) VALUES ('old')"));
						co_await Database.f_ApplySchema();
					}

					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_WithoutRowidSchemaVersions));
					co_await Database.f_Open();
					auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
					DMibAssertTrue(bool(ApplyResult));

					CSqlRawResult TableList = co_await Database.f_QueryRaw(CStr("SELECT wr FROM pragma_table_list WHERE schema = 'main' AND name = 'without_rowid_rebuild'"));
					DMibAssert(TableList.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(TableList.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

					CSqlRawResult Keys = co_await Database.f_QueryRaw(CStr("SELECT new_key FROM without_rowid_rebuild"));
					DMibAssert(Keys.m_Rows.f_GetLen(), ==, nRows);
					if (nRows != 0)
						DMibExpect(Keys.m_Rows[0].m_Values[0].f_GetAsType<CStr>(), ==, CStr("replacement"));
				}

				co_return {};
			};

			DMibTestCategory("Adoption refuses to drop AUTOINCREMENT") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptAutoIncrement.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_IntegerKeySchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ExecuteRaw(CStr("CREATE TABLE integer_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)"));
					co_await Database.f_ExecuteRaw(CStr("INSERT INTO integer_keys (id, value) VALUES (1, 'seed'), (2, 'deleted')"));
					co_await Database.f_ExecuteRaw(CStr("DELETE FROM integer_keys WHERE id = 2"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&gc_IntegerKeySchemaVersions));
				co_await Database.f_Open();
				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				DMibAssert(bool(ApplyResult), ==, false);
				DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("AUTOINCREMENT") >= aint(0), ==, true);

				CSqlRawResult Sequence = co_await Database.f_QueryRaw(CStr("SELECT seq FROM sqlite_sequence WHERE name = 'integer_keys'"));
				DMibAssert(Sequence.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(Sequence.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(2));

				co_return {};
			};

			DMibTestCategory("Rebuild refuses to break a view that selects all columns") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRebuildWildcardView.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildVersion1SchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ApplySchema();
					co_await Database.f_ExecuteRaw(CStr("INSERT INTO rebuild_test (token, legacy_value) VALUES ('view-token', 'legacy')"));
					co_await Database.f_ExecuteRaw(CStr("CREATE VIEW rebuild_view (first, second, third) AS SELECT * FROM rebuild_test"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildSchemaVersions));
				co_await Database.f_Open();

				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				DMibExpect(bool(ApplyResult), ==, false);
				if (!ApplyResult)
					DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("rebuild_view") >= aint(0), ==, true);

				CSqlRawResult ViewRows = co_await Database.f_QueryRaw(CStr("SELECT third FROM rebuild_view"));
				DMibAssert(ViewRows.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(ViewRows.m_Rows[0].m_Values[0].f_GetAsType<CStr>(), ==, CStr("legacy"));

				co_return {};
			};

			DMibTestCategory("Rebuild refuses to break a view that selects all columns through another view") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRebuildNestedWildcardView.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildVersion1SchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ApplySchema();
					co_await Database.f_ExecuteRaw(CStr("INSERT INTO rebuild_test (token, legacy_value) VALUES ('view-token', 'legacy')"));
					co_await Database.f_ExecuteRaw(CStr("CREATE VIEW rebuild_inner_view AS SELECT * FROM rebuild_test"));
					co_await Database.f_ExecuteRaw(CStr("CREATE VIEW rebuild_outer_view (first, second, third) AS SELECT * FROM rebuild_inner_view"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildSchemaVersions));
				co_await Database.f_Open();

				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				DMibAssert(bool(ApplyResult), ==, false);
				DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("rebuild_outer_view") >= aint(0), ==, true);

				CSqlRawResult ViewRows = co_await Database.f_QueryRaw(CStr("SELECT third FROM rebuild_outer_view"));
				DMibAssert(ViewRows.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(ViewRows.m_Rows[0].m_Values[0].f_GetAsType<CStr>(), ==, CStr("legacy"));

				co_return {};
			};

			DMibTestCategory("Rebuild refuses to break a trigger that reads a dropped column through a view") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteRebuildTriggerThroughView.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildVersion1SchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ApplySchema();
					co_await Database.f_ExecuteRaw(CStr("INSERT INTO rebuild_test (token, legacy_value) VALUES ('view-token', 'legacy')"));
					co_await Database.f_ExecuteRaw(CStr("CREATE VIEW rebuild_all_view AS SELECT * FROM rebuild_test"));
					co_await Database.f_ExecuteRaw(CStr("CREATE TABLE rebuild_audit (value TEXT)"));
					co_await Database.f_ExecuteRaw(CStr("CREATE TRIGGER rebuild_audit_trigger AFTER INSERT ON rebuild_audit BEGIN SELECT legacy_value FROM rebuild_all_view; END"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildSchemaVersions));
				co_await Database.f_Open();

				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				DMibAssert(bool(ApplyResult), ==, false);
				DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("rebuild_audit_trigger") >= aint(0), ==, true);

				co_await Database.f_ExecuteRaw(CStr("INSERT INTO rebuild_audit (value) VALUES ('audited')"));

				co_return {};
			};

			DMibTestCategory("Adoption resolves a reference without columns to the primary key") -> TCFuture<void>
			{
				CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteAdoptImplicitForeignKey.sqlite";
				fg_DeleteSqliteDatabaseFiles(DatabasePath);

				{
					CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildKeepV1SchemaVersions));
					co_await Database.f_Open();
					co_await Database.f_ApplySchema();
					co_await Database.f_ExecuteRaw(CStr("DROP TABLE rebuild_keep_child"));
					// The declaration references the parent's unique code, the live table its primary key
					co_await Database.f_ExecuteRaw(CStr("CREATE TABLE rebuild_keep_child (id INTEGER PRIMARY KEY AUTOINCREMENT, parent_code TEXT NOT NULL REFERENCES rebuild_keep_parent)"));
					co_await Database.f_ExecuteRaw(CStr("DELETE FROM schema_migrations"));
				}

				CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_RebuildKeepV1SchemaVersions));
				co_await Database.f_Open();

				auto ApplyResult = co_await Database.f_ApplySchema().f_Wrap();
				DMibExpect(bool(ApplyResult), ==, false);
				if (!ApplyResult)
				{
					CStr Error = ApplyResult.f_GetExceptionStr();
					DMibExpect(Error.f_Find("does not declare") >= aint(0), ==, true);
					DMibExpect(Error.f_Find("REFERENCES rebuild_keep_parent") >= aint(0), ==, true);
				}

				co_return {};
			};

			DMibTestCategory("CHECK comparison keeps token bounds") -> TCFuture<void>
			{
				using NMib::NSQL::NPrivate::fg_SqliteTableChecks;

				auto fExpression = [](ch8 const *_pCreateSql)
					{
						auto Checks = fg_SqliteTableChecks(CStr(_pCreateSql));
						return Checks.f_GetLen() == 1 ? Checks[0].m_Expression : CStr();
					}
				;

				CStr Separate = fExpression("CREATE TABLE t (a, b, \"a + b\", CHECK (a + b > 0))");
				DMibExpect(Separate.f_IsEmpty(), ==, false);
				DMibExpect(fExpression("CREATE TABLE t (a, b, \"a + b\", CHECK (\"a + b\" > 0))"), !=, Separate);
				DMibExpect(fExpression("CREATE TABLE t (a, b, \"a + b\", CHECK (\"a\" + B > 0))"), ==, Separate);
				DMibExpect(fExpression("CREATE TABLE t (a, b, \"a + b\", CHECK ('a' + b > 0))"), !=, Separate);

				// Quoted, a keyword names a column, and "true" a column or else a string
				DMibExpect(fExpression("CREATE TABLE t (\"null\", CHECK (\"null\"))"), !=, fExpression("CREATE TABLE t (\"null\", CHECK (NULL))"));
				DMibExpect(fExpression("CREATE TABLE t (a, CHECK (\"true\" = a))"), !=, fExpression("CREATE TABLE t (a, CHECK (TRUE = a))"));
				DMibExpect(fExpression("CREATE TABLE t (\"null\", CHECK (\"null\"))"), ==, fExpression("CREATE TABLE t (\"null\", CHECK (\"NULL\"))"));

				co_return {};
			};

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

		DMibTestSuite("SQLite schema migration keeps an index name differing in case") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDatabaseIndexNameCase.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient Version1Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_IndexVersion1SchemaVersions));
				co_await Version1Database.f_Open();
				co_await Version1Database.f_ApplySchema();

				co_await Version1Database.f_ExecuteRaw(NStr::CStr("CREATE UNIQUE INDEX \"IDX_INDEXED_PEOPLE_EMAIL\" ON \"indexed_people\" (\"email\")"));
			}

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_IndexSchemaVersions));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			CSqlRawResult Indexes = co_await Database.f_QueryRaw(NStr::CStr("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'indexed_people'"));
			DMibAssert(Indexes.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(Indexes.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("IDX_INDEXED_PEOPLE_EMAIL"));

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

		DMibTestSuite("SQLite migration rejects dropping a table that a view reads") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteDropTableView.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			{
				CSqlDatabaseClient Version1Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_FkVersion1SchemaVersions));
				co_await Version1Database.f_Open();
				co_await Version1Database.f_ApplySchema();
				co_await Version1Database.f_ExecuteRaw(NStr::CStr("CREATE VIEW fk_child_view AS SELECT id FROM fk_child"));
			}

			CSqlDatabaseClient Version2Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_FkChildDropTableAfterParentRebuildSchemaVersions));
			co_await Version2Database.f_Open();
			auto ApplyResult = co_await Version2Database.f_ApplySchema().f_Wrap();
			DMibAssert(bool(ApplyResult), ==, false);
			DMibExpect(ApplyResult.f_GetExceptionStr().f_Find("fk_child_view"), >=, aint(0));

			CSqlRawResult Children = co_await Version2Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM fk_child_view"));
			DMibExpect(Children.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

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
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
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
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
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

		DMibTestSuite("SQLite file-backed read transaction waits for the open") -> TCFuture<void>
		{
			CStr DatabasePath = NFile::CFile::fs_GetProgramDirectory() / "TestSQLiteReadTransactionOpen.sqlite";
			fg_DeleteSqliteDatabaseFiles(DatabasePath);

			CSqlDatabaseClient Database(fg_CreateSqliteBackendFactory(DatabasePath)(&NDatabaseBackend::gc_SchemaVersions));

			{
				DMibTestPath("BeforeOpen");
				auto Result = co_await Database.f_BeginReadTransaction().f_Wrap();
				DMibExpect(bool(Result), ==, false);
			}

			{
				DMibTestPath("DuringOpen");
				auto Open = Database.f_Open();
				auto TransactionResult = co_await Database.f_BeginReadTransaction().f_Wrap();
				co_await fg_Move(Open);

				DMibAssert(bool(TransactionResult), ==, true);
				auto Result = co_await TransactionResult->f_QueryRaw("SELECT 1 AS value");
				DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));
				co_await TransactionResult->f_Rollback();
			}

			co_return {};
		};

		DMibTestSuite("SQLite raw column value types") -> TCFuture<void>
		{
			NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = fg_CreateSqliteBackendFactory(":memory:")(&NDatabaseBackend::gc_SchemaVersions);
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
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

			DMibTestCategory("Raw mutation with RETURNING reports its affected rows") -> TCFuture<void>
			{
				umint nAffected = co_await Database.f_ExecuteRaw
					(
						NStr::CStr("INSERT INTO people (email) VALUES ('sqlite-returning-affected-1@example.com'), ('sqlite-returning-affected-2@example.com') RETURNING id")
					)
				;
				DMibExpect(nAffected, ==, umint(2));

				co_return {};
			};

			DMibTestCategory("Typed fields reject values of another storage class") -> TCFuture<void>
			{
				auto Row = NDatabaseBackend::fg_TestValueTypesRow("sqlite-storage-class@example.com");
				co_await Database.f_Insert<NDatabaseBackend::gc_InsertValueTypes>(fg_TempCopy(Row));
				co_await Database.f_ExecuteRaw(NStr::CStr("UPDATE value_types SET float64_value = 'not a number' WHERE key = 'sqlite-storage-class@example.com'"));

				auto Result = co_await Database.f_QueryOne<NDatabaseBackend::gc_SelectValueTypesByKey>(NStr::CStr(Row.m_Key)).f_Wrap();
				DMibExpect(bool(Result), ==, false);

				co_return {};
			};

			DMibTestCategory("Time fields accept CURRENT_TIMESTAMP values") -> TCFuture<void>
			{
				auto Row = NDatabaseBackend::fg_TestValueTypesRow("sqlite-current-timestamp@example.com");
				co_await Database.f_Insert<NDatabaseBackend::gc_InsertValueTypes>(fg_TempCopy(Row));
				co_await Database.f_ExecuteRaw(NStr::CStr("UPDATE value_types SET time_value = CURRENT_TIMESTAMP WHERE key = 'sqlite-current-timestamp@example.com'"));

				auto Result = co_await Database.f_QueryOne<NDatabaseBackend::gc_SelectValueTypesByKey>(NStr::CStr(Row.m_Key)).f_Wrap();
				DMibExpect(bool(Result), ==, true);

				co_return {};
			};

			DMibTestCategory("Text keeps embedded NUL characters") -> TCFuture<void>
			{
				NStr::CStr const TextWithNul("a\0b", 3);
				DMibExpect(TextWithNul.f_GetLen(), ==, umint(3))(NMib::NTest::ETest_FailAndStop);

				NContainer::TCVector<CSqlValue> Parameters;
				Parameters.f_InsertLast(TextWithNul);
				Parameters.f_InsertLast(TextWithNul);
				CSqlRawResult Result = co_await Database.f_QueryRaw(NStr::CStr("SELECT ? AS value, length(CAST(? AS BLOB)) AS bytes"), fg_Move(Parameters));
				DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1))(NMib::NTest::ETest_FailAndStop);
				DMibExpect(Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>().f_GetLen(), ==, umint(3));
				DMibExpect(Result.m_Rows[0].m_Values[1].f_GetAsType<int64>(), ==, int64(3));

				co_return {};
			};

			DMibTestCategory("Inference reaching the end does not execute the statement again") -> TCFuture<void>
			{
				DMibExpect(co_await fg_CountRawStreamRows(co_await Database.f_QueryRawStream("SELECT NULL AS value")), ==, umint(1));

				NStr::CStr const InsertReturningNull = "INSERT INTO people (email) VALUES ('sqlite-raw-stream-returning-null@example.com') RETURNING NULL";
				DMibExpect(co_await fg_CountRawStreamRows(co_await Database.f_QueryRawStream(InsertReturningNull)), ==, umint(1));

				{
					auto Transaction = co_await Database.f_BeginTransaction();
					DMibExpect(co_await fg_CountRawStreamRows(co_await Transaction.f_QueryRawStream("SELECT NULL AS value")), ==, umint(1));
					DMibExpect(co_await fg_CountRawStreamRows(co_await Transaction.f_QueryRawStream(InsertReturningNull)), ==, umint(1));
					co_await Transaction.f_Commit();
				}

				umint nInserted = co_await Database.f_Count<NDatabaseBackend::gc_SelectPersonByEmail>(NStr::CStr("sqlite-raw-stream-returning-null@example.com"));
				DMibExpect(nInserted, ==, umint(2));

				co_return {};
			};

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
			for (umint iIteration = 0; iIteration < 16; ++iIteration)
			{
				NStr::CStr Email = "sqlite-rowid-{}@example.com"_f << iIteration;
				int64 ID = co_await Database.template f_InsertReturning<&NDatabaseBackend::CPersonRow::m_ID, NDatabaseBackend::gc_InsertPerson>(Email);
				IDs.f_InsertLast(ID);
			}

			DMibExpect(IDs.f_GetLen(), ==, umint(16));
			for (umint iID = 1; iID < IDs.f_GetLen(); ++iID)
			{
				DMibTestPath("step {}"_f << iID);
				DMibExpect(IDs[iID], >, IDs[iID - 1]);
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
			CSqlDatabaseClient Database(fg_TempCopy(Backend));
			co_await Database.f_Open();
			co_await Database.f_ApplySchema();

			for (umint iIteration = 0; iIteration < 8; ++iIteration)
			{
				NStr::CStr Email = "sqlite-cache-{}@example.com"_f << iIteration;
				co_await Database.f_Insert<NDatabaseBackend::gc_InsertPerson>(Email);
			}

			for (umint Pass = 0; Pass < 32; ++Pass)
			{
				DMibTestPath("pass {}"_f << Pass);

				umint nMatching = 0;
				for (umint iIteration = 0; iIteration < 8; ++iIteration)
				{
					NStr::CStr Key = "sqlite-cache-{}@example.com"_f << iIteration;
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

				auto Transaction = co_await Database.f_BeginTransaction();

				NMib::NSQL::NTest::CUserRoleRow ValidRole;
				ValidRole.m_UserID = 1;
				ValidRole.m_Role = "admin";
				co_await Transaction.f_Insert(NMib::NSQL::NTest::gc_UserRoleTable, fg_Move(ValidRole));

				co_await Transaction.f_Commit();
			}

			CSqlRawResult OrphanCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT COUNT(*) FROM user_roles WHERE user_id = 999999"));
			DMibExpect(OrphanCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

			CSqlRawResult ValidCount = co_await Database.f_QueryRaw(NStr::CStr("SELECT COUNT(*) FROM user_roles WHERE user_id = 1"));
			DMibExpect(ValidCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

			co_return {};
		};
	}

	DMibTestRegister(CSQLiteDatabase_Tests, Malterlib::SQL);
}
