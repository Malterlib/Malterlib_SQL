// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/PostgresClient>
#include <Mib/SQL/PostgresDatabase>
#include <Mib/SQL/SQLiteDatabase>
#include <Mib/Concurrency/AsyncDestroy>
#include <Mib/File/File>

#include "Test_Malterlib_SQL_DatabaseBackendShared.h"
#include "Test_Malterlib_SQL_PostgresShared.h"

using namespace NMib;
using namespace NMib::NConcurrency;
using namespace NMib::NSQL;
using namespace NMib::NSQL::NTest::NPostgres;
using namespace NMib::NStr;

namespace
{
	void fg_ResetSqliteBenchmarkFiles(CStr const &_DatabasePath)
	{
		NMib::NTest::fg_TestAddCleanupPath(_DatabasePath);
		NMib::NTest::fg_TestAddCleanupPath(_DatabasePath + "-wal");
		NMib::NTest::fg_TestAddCleanupPath(_DatabasePath + "-shm");

		if (NFile::CFile::fs_FileExists(_DatabasePath))
			NFile::CFile::fs_DeleteFile(_DatabasePath);

		if (NFile::CFile::fs_FileExists(_DatabasePath + "-wal"))
			NFile::CFile::fs_DeleteFile(_DatabasePath + "-wal");

		if (NFile::CFile::fs_FileExists(_DatabasePath + "-shm"))
			NFile::CFile::fs_DeleteFile(_DatabasePath + "-shm");
	}

	auto fg_CreateSqliteBenchmarkBackendFactory(CStr _Path)
	{
		return [Path = fg_Move(_Path)](ICSqlSchemaVersions const *_pSchemaVersions)
			{
				CSQLiteDatabaseBackendSettings Settings;
				Settings.m_Path = Path;
				Settings.m_nReadConnections = fg_Max((NSys::fg_Thread_GetVirtualCores() * 15) / 10, 4);

				return fg_CreateDatabaseBackendSQLite(_pSchemaVersions, fg_Move(Settings));
			}
		;
	}

	auto fg_CreatePostgresBenchmarkBackendFactory(CPostgresConnectionSettings _Settings)
	{
		return [Settings = fg_Move(_Settings)](ICSqlSchemaVersions const *_pSchemaVersions)
			{
				return fg_CreateDatabaseBackendPostgres
					(
						_pSchemaVersions
						, { .m_ConnectionSettings = CPostgresConnectionSettings(Settings) }
					)
				;
			}
		;
	}

	TCFuture<void> fg_RunCombinedBenchmarks(CPostgresConnectionSettings _Settings, umint _nRows)
	{
		auto CaptureScope = co_await g_CaptureExceptions;

		TCActor<CPostgresClientActor> ResetClient = fg_CreatePostgresClient();
		auto CleanupResetClient = co_await fg_AsyncDestroy(ResetClient);
		co_await ResetClient(&CPostgresClientActor::f_Connect, CPostgresConnectionSettings(_Settings)).f_Timeout(gc_Timeout, "Timed out connecting PostgreSQL benchmark reset client");

		NContainer::TCVector<CStr> TableNames;
		TableNames.f_InsertLast("profiles");
		TableNames.f_InsertLast("schema_migrations");
		for (CStr const &TableName : TableNames)
			co_await ResetClient(&CPostgresClientActor::f_Execute, "DROP TABLE IF EXISTS \"{}\" CASCADE"_f << TableName);

		CStr SqliteDatabasePath = NFile::CFile::fs_GetProgramDirectory() / "BenchmarkSQLite.sqlite";
		fg_ResetSqliteBenchmarkFiles(SqliteDatabasePath);

		auto SqliteBackend = fg_CreateSqliteBenchmarkBackendFactory(SqliteDatabasePath)(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions);
		CSqlDatabaseClient SqliteDatabase(SqliteBackend);
		co_await SqliteDatabase.f_Open();
		co_await SqliteDatabase.f_ApplySchema();

		auto PostgresBackend = fg_CreatePostgresBenchmarkBackendFactory(_Settings)(&NMib::NSQL::NTest::NDatabaseBackend::gc_SchemaVersions);
		CSqlDatabaseClient PostgresDatabase(PostgresBackend);
		co_await PostgresDatabase.f_Open();
		co_await PostgresDatabase.f_ApplySchema();

		NContainer::TCVector<NMib::NSQL::NTest::NDatabaseBackend::CSqlBenchmarkBackend> Backends;
		Backends.f_InsertLast({"SQLite", &SqliteDatabase});
		Backends.f_InsertLast({"Postgres", &PostgresDatabase});

		co_await NMib::NSQL::NTest::NDatabaseBackend::fg_TestSqlDatabaseBenchmarks(fg_Move(Backends), _nRows);

		co_await fg_Move(ResetClient).f_Destroy();
		CleanupResetClient.f_Clear();

		co_return {};
	}
}

struct CSqlBenchmarks_Tests : public NMib::NTest::CTest
{
	void f_DoTests()
	{
		DMibTestSuite(NMib::NTest::CTestCategory("SQL backend benchmarks") << NMib::NTest::CTestGroup("Performance")) -> TCFuture<void>
		{
#if defined(DMibDebug) || defined(DMibSanitizerEnabled)
			constexpr umint c_nRows = 200;
#else
			constexpr umint c_nRows = 20000;
#endif

			co_await fg_WithPostgresTestServer
				(
					{
						.m_DirectoryName = "PostgresDatabase_Benchmarks"
						, .m_Port = uint16(24022)
						, .m_bTLS = false
						, .m_MissingExecutableWarning = "Warning: Failed to find postgres executables, running SQL benchmarks against SQLite only\n"
					}
					, g_ActorFunctor / [](CPostgresConnectionSettings Settings) -> TCFuture<void>
					{
						co_await fg_RunCombinedBenchmarks(fg_Move(Settings), c_nRows);
						co_return {};
					}
				)
			;

			co_return {};
		};
	}
};

DMibTestRegister(CSqlBenchmarks_Tests, Malterlib::SQL);
