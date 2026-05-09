// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

#include "../Malterlib_SQL_SQLite_Init.h"

namespace NMib::NSQL::NPrivate
{
	// Whether a statement starts with the keyword of an INSERT, UPDATE or DELETE, or with the WITH clause that can precede one. The text of a
	// statement includes the empty statements, comments and UTF-8 byte order marks that SQLite skipped before it, since its tokenizer reads
	// a byte order mark as whitespace.
	static bool fsg_SqliteIsDataModification(sqlite3_stmt *_pStatement)
	{
		ch8 const *pSql = sqlite3_sql(_pStatement);
		if (!pSql)
			return true;

		while (*pSql)
		{
			if (*pSql == ' ' || *pSql == '\t' || *pSql == '\n' || *pSql == '\r' || *pSql == '\f' || *pSql == '\v' || *pSql == ';')
				++pSql;
			else if (uint8(pSql[0]) == 0xEF && uint8(pSql[1]) == 0xBB && uint8(pSql[2]) == 0xBF)
				pSql += 3;
			else if (pSql[0] == '-' && pSql[1] == '-')
			{
				while (*pSql && *pSql != '\n')
					++pSql;
			}
			else if (pSql[0] == '/' && pSql[1] == '*')
			{
				pSql += 2;
				while (*pSql && !(pSql[0] == '*' && pSql[1] == '/'))
					++pSql;

				if (*pSql)
					pSql += 2;
			}
			else
				break;
		}

		// One letter longer than the longest keyword, so a longer word does not match
		ch8 Keyword[8];
		umint nKeyword = 0;
		for (; nKeyword < 8; ++nKeyword)
		{
			ch8 Char = pSql[nKeyword];
			if (Char >= 'A' && Char <= 'Z')
				Char = ch8(Char - 'A' + 'a');
			else if (Char < 'a' || Char > 'z')
				break;

			Keyword[nKeyword] = Char;
		}

		auto fIsKeyword = [&](ch8 const *_pKeyword)
			{
				umint iChar = 0;
				for (; iChar < nKeyword; ++iChar)
				{
					if (_pKeyword[iChar] != Keyword[iChar])
						return false;
				}

				return _pKeyword[iChar] == 0;
			}
		;

		return fIsKeyword("insert") || fIsKeyword("update") || fIsKeyword("delete") || fIsKeyword("replace") || fIsKeyword("with");
	}

	static umint fsg_SqliteAffectedRows(sqlite3 *_pDatabase, sqlite3_stmt *_pStatement, int64 _TotalChangesBefore)
	{
		// Most non-DML doesn't set sqlite3_changes64(), which keeps an earlier statement's count; an unchanged total means the statement counted no rows.
		if (sqlite3_total_changes64(_pDatabase) == _TotalChangesBefore)
			return umint(0);

		// With foreign keys enforced, DROP TABLE of a referenced table deletes its rows first, and counts them and their cascades
		if (!fsg_SqliteIsDataModification(_pStatement))
			return umint(0);

		return umint(sqlite3_changes64(_pDatabase));
	}

	// A mutation with RETURNING yields its rows before it completes, and SQLite sets sqlite3_changes64() only then
	static int fsg_SqliteStepToDone(sqlite3_stmt *_pStatement)
	{
		int Result = sqlite3_step(_pStatement);
		while (Result == SQLITE_ROW)
			Result = sqlite3_step(_pStatement);

		return Result;
	}

	CSqliteStatement::CSqliteStatement(sqlite3_stmt *_pStatement)
		: m_pStatement(_pStatement)
	{
	}

	CSqliteStatement::CSqliteStatement(CSqliteStatement &&_Other)
		: m_pStatement(_Other.m_pStatement)
	{
		_Other.m_pStatement = nullptr;
	}

	CSqliteStatement::~CSqliteStatement()
	{
		if (m_pStatement)
			sqlite3_finalize(m_pStatement);
	}

	CSqliteStatement &CSqliteStatement::operator = (CSqliteStatement &&_Other)
	{
		if (this == &_Other)
			return *this;

		if (m_pStatement)
			sqlite3_finalize(m_pStatement);

		m_pStatement = _Other.m_pStatement;
		_Other.m_pStatement = nullptr;

		return *this;
	}

	CSqliteCachedStatement::CSqliteCachedStatement(sqlite3_stmt *_pStatement)
		: m_pStatement(_pStatement)
	{
	}

	CSqliteCachedStatement::CSqliteCachedStatement(CSqliteCachedStatement &&_Other)
		: m_pStatement(_Other.m_pStatement)
	{
		_Other.m_pStatement = nullptr;
	}

	CSqliteCachedStatement::~CSqliteCachedStatement()
	{
		if (m_pStatement)
		{
			sqlite3_reset(m_pStatement);
			sqlite3_clear_bindings(m_pStatement);
		}
	}

	CSqliteCachedStatement &CSqliteCachedStatement::operator = (CSqliteCachedStatement &&_Other)
	{
		if (this == &_Other)
			return *this;

		if (m_pStatement)
		{
			sqlite3_reset(m_pStatement);
			sqlite3_clear_bindings(m_pStatement);
		}

		m_pStatement = _Other.m_pStatement;
		_Other.m_pStatement = nullptr;

		return *this;
	}

	CSQLiteDatabaseHandle::~CSQLiteDatabaseHandle()
	{
		fp_Close();
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_Open(NStr::CStr const &_Path)
	{
		DMibCheck(!mp_pDatabase);

		fg_SqliteEnsureInitialized();

		if (sqlite3_open(_Path.f_GetStr(), &mp_pDatabase) != SQLITE_OK)
		{
			auto Error = fg_SqliteError(mp_pDatabase, "Failed to open SQLite database");
			fp_Close();

			co_return Error;
		}

		// A double-quoted identifier that names no column would otherwise read as a string literal, hiding schema mismatches
		sqlite3_db_config(mp_pDatabase, SQLITE_DBCONFIG_DQS_DML, 0, nullptr);
		sqlite3_db_config(mp_pDatabase, SQLITE_DBCONFIG_DQS_DDL, 0, nullptr);

		co_await f_Execute("PRAGMA foreign_keys=ON");

		sqlite3_busy_timeout(mp_pDatabase, gc_SqliteBusyTimeoutMilliseconds);

		co_await f_RecordJournalMode();
		m_MaxPageCount = co_await f_SelectInteger(NStr::CStr("PRAGMA max_page_count"), {});
		m_bTrustedSchema = co_await f_SelectInteger(NStr::CStr("PRAGMA trusted_schema"), {}) != 0;

		m_RestorePragmaSql.f_Clear();
		for (ch8 const *pPragma : gc_SqliteRestoredPragmas)
		{
			// A setting that does not apply to the database, such as mmap_size for an in-memory one, reports no value
			auto Reported = co_await f_SelectRaw(NStr::CStr::CFormat("PRAGMA {}") << pPragma, {});
			if (Reported.m_Rows.f_IsEmpty())
				continue;

			if (Reported.m_Rows.f_GetLen() != 1 || Reported.m_Rows[0].m_Values.f_GetLen() != 1 || Reported.m_Rows[0].m_Values[0].f_GetTypeID() != ESqlValueType::mc_Integer64)
				co_return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite did not report its {} setting as one integer") << pPragma);

			int64 Value = Reported.m_Rows[0].m_Values[0].f_GetAsType<int64>();
			// secure_delete reports FAST as 2, which setting it reads as ON
			if (NStr::CStr(pPragma) == "secure_delete" && Value == 2)
				m_RestorePragmaSql.f_InsertLast(NStr::CStr("PRAGMA secure_delete=FAST"));
			else
				m_RestorePragmaSql.f_InsertLast(NStr::CStr::CFormat("PRAGMA {}={}") << pPragma << Value);
		}

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_RecordJournalMode()
	{
		auto Result = co_await f_SelectRaw(NStr::CStr("PRAGMA journal_mode"), {});
		if (Result.m_Rows.f_GetLen() != 1 || Result.m_Rows[0].m_Values.f_GetLen() != 1)
			co_return DMibErrorDatabaseInstance("SQLite did not report its journal mode");

		m_JournalMode = Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>().f_LowerCase();

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_Execute(NStr::CStr const &_Sql)
	{
		auto Statement = co_await fp_Prepare(_Sql);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	NConcurrency::TCWrapped<int64> CSQLiteDatabaseHandle::f_ExecuteReturningLastInsertID(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		// Stepped to the end, since with count_changes the change counter is updated only then
		auto Result = fsg_SqliteStepToDone(Statement.m_pStatement);

		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite insert");

		// A trigger can skip the insert with RAISE(IGNORE), which leaves the previous insert's rowid in place
		if (sqlite3_changes64(mp_pDatabase) == 0)
			co_return DMibErrorDatabaseInstance("SQLite insert did not insert a row");

		co_return int64(sqlite3_last_insert_rowid(mp_pDatabase));
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(mp_pDatabase);
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = fsg_SqliteStepToDone(Statement.m_pStatement);

		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(mp_pDatabase, Statement.m_pStatement, TotalChangesBefore);
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(mp_pDatabase);
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = fsg_SqliteStepToDone(Statement.m_pStatement);

		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(mp_pDatabase, Statement.m_pStatement, TotalChangesBefore);
	}

	// False while a BEGIN or SAVEPOINT holds a transaction open; the implicit transactions of single statements leave it true
	bool CSQLiteDatabaseHandle::f_IsAutocommit() const
	{
		return sqlite3_get_autocommit(mp_pDatabase) != 0;
	}

	bool CSQLiteDatabaseHandle::f_IsOpen() const
	{
		return mp_pDatabase != nullptr;
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteBatchAffected(NStr::CStr const &_Sql, NContainer::TCVector<NContainer::TCVector<CSqlColumnValue>> const &_Rows)
	{
		if (_Rows.f_IsEmpty())
			co_return umint(0);

		auto Statement = co_await fp_Prepare(_Sql);

		umint nTotalAffected = 0;
		for (auto const &Row : _Rows)
		{
			int64 TotalChangesBefore = sqlite3_total_changes64(mp_pDatabase);
			co_await fp_BindValues(*Statement.m_pStatement, Row);

			auto StepResult = fsg_SqliteStepToDone(Statement.m_pStatement);
			if (StepResult != SQLITE_DONE)
				co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite batch insert statement");

			nTotalAffected += fsg_SqliteAffectedRows(mp_pDatabase, Statement.m_pStatement, TotalChangesBefore);

			auto ResetResult = sqlite3_reset(Statement.m_pStatement);
			if (ResetResult != SQLITE_OK)
				co_return fg_SqliteError(mp_pDatabase, "Failed to reset SQLite batch insert statement");

			sqlite3_clear_bindings(Statement.m_pStatement);
		}

		co_return nTotalAffected;
	}

	NConcurrency::TCWrapped<CSqliteStatement> CSQLiteDatabaseHandle::f_Prepare(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await fp_Prepare(_Sql);
		co_await fp_BindValues(*Statement.m_pStatement, _Values);

		co_return fg_Move(Statement);
	}

	NConcurrency::TCWrapped<CSqliteStatement> CSQLiteDatabaseHandle::f_Prepare(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		auto Statement = co_await fp_Prepare(_Sql);
		co_await fp_BindValues(*Statement.m_pStatement, _Values);

		co_return fg_Move(Statement);
	}

	NConcurrency::TCWrapped<CSqliteCachedStatement> CSQLiteDatabaseHandle::f_PrepareCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto *pStatement = co_await fp_LookupOrPrepare(_Key, _Sql);
		CSqliteCachedStatement Borrowed(pStatement);
		co_await fp_BindValues(*pStatement, _Values);

		co_return fg_Move(Borrowed);
	}

	NConcurrency::TCWrapped<CSqliteCachedStatement> CSQLiteDatabaseHandle::f_PrepareCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		auto *pStatement = co_await fp_LookupOrPrepare(_Key, _Sql);
		CSqliteCachedStatement Borrowed(pStatement);
		co_await fp_BindValues(*pStatement, _Values);

		co_return fg_Move(Borrowed);
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_ExecuteCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	auto CSQLiteDatabaseHandle::f_ExecuteReturningLastInsertIDCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
		-> NConcurrency::TCWrapped<int64>
	{
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		// Stepped to the end, since with count_changes the change counter is updated only then
		auto Result = fsg_SqliteStepToDone(Statement.m_pStatement);

		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite insert");

		// A trigger can skip the insert with RAISE(IGNORE), which leaves the previous insert's rowid in place
		if (sqlite3_changes64(mp_pDatabase) == 0)
			co_return DMibErrorDatabaseInstance("SQLite insert did not insert a row");

		co_return int64(sqlite3_last_insert_rowid(mp_pDatabase));
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(mp_pDatabase);
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = fsg_SqliteStepToDone(Statement.m_pStatement);

		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(mp_pDatabase, Statement.m_pStatement, TotalChangesBefore);
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(mp_pDatabase);
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = fsg_SqliteStepToDone(Statement.m_pStatement);

		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(mp_pDatabase, Statement.m_pStatement, TotalChangesBefore);
	}

	auto CSQLiteDatabaseHandle::f_ExecuteBatchAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<NContainer::TCVector<CSqlColumnValue>> const &_Rows)
		-> NConcurrency::TCWrapped<umint>
	{
		if (_Rows.f_IsEmpty())
			co_return umint(0);

		auto *pStatement = co_await fp_LookupOrPrepare(_Key, _Sql);
		CSqliteCachedStatement Borrowed(pStatement);

		umint nTotalAffected = 0;
		for (auto const &Row : _Rows)
		{
			int64 TotalChangesBefore = sqlite3_total_changes64(mp_pDatabase);
			co_await fp_BindValues(*pStatement, Row);

			auto StepResult = fsg_SqliteStepToDone(pStatement);
			if (StepResult != SQLITE_DONE)
				co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite batch insert statement");

			nTotalAffected += fsg_SqliteAffectedRows(mp_pDatabase, pStatement, TotalChangesBefore);

			auto ResetResult = sqlite3_reset(pStatement);
			if (ResetResult != SQLITE_OK)
				co_return fg_SqliteError(mp_pDatabase, "Failed to reset SQLite batch insert statement");

			sqlite3_clear_bindings(pStatement);
		}

		co_return nTotalAffected;
	}

	NConcurrency::TCWrapped<int64> CSQLiteDatabaseHandle::f_SelectIntegerCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite scalar select");

		int64 Value = sqlite3_column_int64(Statement.m_pStatement, 0);

		Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "SQLite scalar select returned unexpected extra rows");

		co_return Value;
	}

	NConcurrency::TCWrapped<bool> CSQLiteDatabaseHandle::f_HasRows(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result == SQLITE_ROW)
			co_return true;

		if (Result == SQLITE_DONE)
			co_return false;

		co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite row check");
	}

	NConcurrency::TCWrapped<bool> CSQLiteDatabaseHandle::f_HasSchemaVersion(NStr::CStr const &_SchemaVersionID)
	{
		NContainer::TCVector<CSqlValue> VersionValues;
		VersionValues.f_InsertLast(_SchemaVersionID);

		co_return f_HasRows(fg_SqliteHasSchemaVersionSql(), VersionValues);
	}

	NConcurrency::TCWrapped<NContainer::TCVector<CSqliteAppliedSchemaVersion>> CSQLiteDatabaseHandle::f_ReadAppliedSchemaVersions()
	{
		NContainer::TCVector<CSqliteAppliedSchemaVersion> Versions;
		auto Statement = co_await fp_Prepare(fg_SqliteReadSchemaVersionsSql());

		for (;;)
		{
			auto Result = sqlite3_step(Statement.m_pStatement);

			if (Result == SQLITE_DONE)
				co_return Versions;

			if (Result != SQLITE_ROW)
				co_return fg_SqliteError(mp_pDatabase, "Failed to read SQLite schema versions");

			CSqliteAppliedSchemaVersion Version;
			auto const *pID = sqlite3_column_text(Statement.m_pStatement, 0);
			auto const *pChecksum = sqlite3_column_text(Statement.m_pStatement, 1);
			Version.m_ID = pID ? reinterpret_cast<ch8 const *>(pID) : "";
			Version.m_Checksum = pChecksum ? reinterpret_cast<ch8 const *>(pChecksum) : "";

			Versions.f_InsertLast(fg_Move(Version));
		}
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_ValidateAppliedSchemaVersions(NContainer::TCVector<CSqlSchemaVersionDescription> const &_ExpectedVersions)
	{
		using namespace NStr;

		auto AppliedVersions = co_await f_ReadAppliedSchemaVersions();
		for (auto const &AppliedVersion : AppliedVersions)
		{
			auto const *pExpectedVersion = (CSqlSchemaVersionDescription const *)nullptr;

			for (auto const &ExpectedVersion : _ExpectedVersions)
			{
				if (ExpectedVersion.f_ID() == AppliedVersion.m_ID)
				{
					pExpectedVersion = &ExpectedVersion;

					break;
				}
			}

			if (!pExpectedVersion)
				co_return DMibErrorDatabaseInstance("SQLite database has unknown applied schema version '{}'"_f << AppliedVersion.m_ID);

			if (pExpectedVersion->m_Checksum != AppliedVersion.m_Checksum)
				co_return DMibErrorDatabaseInstance("SQLite schema version '{}' checksum mismatch"_f << AppliedVersion.m_ID);
		}

		co_return {};
	}

	NConcurrency::TCWrapped<bool> CSQLiteDatabaseHandle::f_HasTable(NStr::CStr const &_TableName)
	{
		NContainer::TCVector<CSqlValue> Values;
		Values.f_InsertLast(_TableName);

		co_return f_HasRows(fg_SqliteHasTableSql(), Values);
	}

	// Raw SQL can leave a transaction open or change the connection's pragmas, which would carry over to the pooled
	// connection's next user. A plain BEGIN reports as read-only, so it can run on a read connection.
	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_RestoreAfterRawSql(bool _bReadConnection)
	{
		// A failed reset does not stop the later ones, and the first failure is returned
		NException::CExceptionPointer pFirstException;
		auto fExecute = [&](NStr::CStr const &_Sql)
			{
				auto Result = f_Execute(_Sql);
				if (!Result && !pFirstException)
					pFirstException = fg_Move(Result).f_GetException();
			}
		;

		if (!f_IsAutocommit())
			fExecute(NStr::CStr("ROLLBACK"));

		// Dropping the temporary objects below is a write, and an enforced foreign key between temporary tables can fail the
		// drop of its parent
		fExecute(NStr::CStr("PRAGMA query_only=OFF"));
		fExecute(NStr::CStr("PRAGMA foreign_keys=OFF"));

		// A temporary table is found before main's table of the same name, which would take the ORM's writes, and a
		// temporary trigger can act on main's tables. SQLite's own objects, such as sqlite_sequence, cannot be dropped.
		auto TemporaryObjects = f_SelectRaw
			(
				NStr::CStr
				(
					"SELECT type, name FROM temp.sqlite_master WHERE type IN ('trigger', 'view', 'table') AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
					"ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 ELSE 2 END"
				)
				, {}
			)
		;
		if (!TemporaryObjects)
		{
			if (!pFirstException)
				pFirstException = fg_Move(TemporaryObjects).f_GetException();
		}
		else
		{
			for (auto const &Object : TemporaryObjects->m_Rows)
			{
				NStr::CStr const &Type = Object.m_Values[0].f_GetAsType<NStr::CStr>();
				NStr::CStr DropSql;
				{
					NStr::CStr::CAppender Appender(DropSql);
					Appender += Type == "trigger" ? "DROP TRIGGER IF EXISTS temp." : Type == "view" ? "DROP VIEW IF EXISTS temp." : "DROP TABLE IF EXISTS temp.";
					fg_SqliteAppendQuotedIdentifier(Appender, Object.m_Values[1].f_GetAsType<NStr::CStr>());
				}
				fExecute(DropSql);
			}
		}

		auto Databases = f_SelectRaw(NStr::CStr("SELECT name FROM pragma_database_list WHERE name NOT IN ('main', 'temp')"), {});
		if (!Databases)
		{
			if (!pFirstException)
				pFirstException = fg_Move(Databases).f_GetException();
		}
		else
		{
			for (auto const &Database : Databases->m_Rows)
			{
				NStr::CStr DetachSql;
				{
					NStr::CStr::CAppender Appender(DetachSql);
					Appender += "DETACH DATABASE ";
					fg_SqliteAppendQuotedIdentifier(Appender, Database.m_Values[0].f_GetAsType<NStr::CStr>());
				}
				fExecute(DetachSql);
			}
		}

		fExecute(NStr::CStr("PRAGMA foreign_keys=ON"));
		fExecute(NStr::CStr("PRAGMA count_changes=OFF"));
		fExecute(NStr::CStr("PRAGMA case_sensitive_like=OFF"));
		fExecute(NStr::CStr("PRAGMA recursive_triggers=OFF"));
		fExecute(NStr::CStr("PRAGMA reverse_unordered_selects=OFF"));
		fExecute(NStr::CStr("PRAGMA defer_foreign_keys=OFF"));
		fExecute(NStr::CStr("PRAGMA ignore_check_constraints=OFF"));
		fExecute(NStr::CStr("PRAGMA legacy_alter_table=OFF"));
		// A writable schema lets statements edit sqlite_master directly and skips some of SQLite's schema checks. RESET also
		// reloads a schema edited that way.
		fExecute(NStr::CStr("PRAGMA writable_schema=RESET"));
		// Views and triggers that call functions not marked innocuous fail while the schema is untrusted
		fExecute(NStr::CStr(m_bTrustedSchema ? "PRAGMA trusted_schema=ON" : "PRAGMA trusted_schema=OFF"));
		// The column names raw results report
		fExecute(NStr::CStr("PRAGMA full_column_names=OFF"));
		fExecute(NStr::CStr("PRAGMA short_column_names=ON"));

		for (auto const &Sql : m_RestorePragmaSql)
			fExecute(Sql);

		// An exclusive locking mode keeps the lock after commits, blocking the other connections, and the lock it holds is
		// released by the next access once the mode is normal
		fExecute(NStr::CStr("PRAGMA locking_mode=NORMAL"));
		fExecute(NStr::CStr("SELECT 1 FROM sqlite_master LIMIT 1"));

		// SQLite does not guarantee rollback with journaling disabled
		{
			auto JournalMode = f_SelectRaw(NStr::CStr("PRAGMA journal_mode"), {});
			if (!JournalMode)
			{
				if (!pFirstException)
					pFirstException = fg_Move(JournalMode).f_GetException();
			}
			else if (JournalMode->m_Rows.f_GetLen() != 1 || JournalMode->m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>().f_LowerCase() != m_JournalMode)
			{
				auto Restored = f_SelectRaw(NStr::CStr::CFormat("PRAGMA journal_mode={}") << m_JournalMode, {});
				if (!Restored)
				{
					if (!pFirstException)
						pFirstException = fg_Move(Restored).f_GetException();
				}
				else if (Restored->m_Rows.f_GetLen() != 1 || Restored->m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>().f_LowerCase() != m_JournalMode)
				{
					if (!pFirstException)
						pFirstException = DMibErrorDatabaseInstance("Failed to restore the SQLite journal mode raw SQL changed");
				}
			}
		}

		// A lower page limit fails the next write that needs a new page with SQLITE_FULL
		fExecute(NStr::CStr::CFormat("PRAGMA max_page_count={}") << m_MaxPageCount);

		sqlite3_busy_timeout(mp_pDatabase, gc_SqliteBusyTimeoutMilliseconds);
		fExecute(NStr::CStr(m_bSynchronousNormal ? "PRAGMA synchronous=NORMAL" : "PRAGMA synchronous=FULL"));
		if (_bReadConnection)
			fExecute(NStr::CStr("PRAGMA query_only=ON"));

		if (pFirstException)
			co_return fg_Move(pFirstException);

		co_return {};
	}

	NConcurrency::TCWrapped<bool> CSQLiteDatabaseHandle::f_HasColumn(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName)
	{
		auto ColumnNames = co_await f_ColumnNames(_TableName);
		// SQLite matches column names case-insensitively for ASCII letters
		for (auto const &ColumnName : ColumnNames)
		{
			if (NPrivate::fg_SqlIdentifiersEqualIgnoringAsciiCase(ColumnName, _ColumnName))
				co_return true;
		}

		co_return false;
	}

	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> CSQLiteDatabaseHandle::f_ColumnNames(NStr::CStr const &_TableName)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			// table_xinfo lists generated (and other hidden) columns that table_info omits
			Appender += "PRAGMA table_xinfo(";
			fg_SqliteAppendQuotedIdentifier(Appender, _TableName);
			Appender += ")";
		}

		NContainer::TCVector<NStr::CStr> ColumnNames;
		auto Statement = co_await fp_Prepare(Sql);
		for (;;)
		{
			auto Result = sqlite3_step(Statement.m_pStatement);

			if (Result == SQLITE_DONE)
				co_return fg_Move(ColumnNames);

			if (Result != SQLITE_ROW)
				co_return fg_SqliteError(mp_pDatabase, "Failed to inspect SQLite table columns");

			auto const *pText = reinterpret_cast<ch8 const *>(sqlite3_column_text(Statement.m_pStatement, 1));
			ColumnNames.f_InsertLast(NStr::CStr(pText, sqlite3_column_bytes(Statement.m_pStatement, 1)));
		}
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_MarkSchemaVersionApplied(NStr::CStr const &_SchemaVersionID)
	{
		co_return DMibErrorDatabaseInstance("Internal SQLite schema version application requires checksum");
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_MarkSchemaVersionApplied(CSqlSchemaVersionDescription const &_SchemaVersion)
	{
		NContainer::TCVector<CSqlValue> VersionValues;
		VersionValues.f_InsertLast(_SchemaVersion.f_ID());
		VersionValues.f_InsertLast(_SchemaVersion.f_DatabaseName());
		VersionValues.f_InsertLast(_SchemaVersion.m_Checksum);
		VersionValues.f_InsertLast(NStr::CStr("Malterlib"));

		co_return f_Execute(fg_SqliteInsertSchemaVersionSql(), VersionValues);
	}

	NConcurrency::TCWrapped<CSqlRowDataPointer> CSQLiteDatabaseHandle::f_SelectNext(sqlite3_stmt &_Statement, CSqlRowMapping const &_Mapping)
	{
		auto Result = sqlite3_step(&_Statement);

		if (Result == SQLITE_DONE)
			co_return nullptr;

		if (Result != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite select");

		auto pRow = _Mapping.f_CreateRow();
		if (auto pException = fp_MapRow(_Statement, _Mapping, *pRow))
			co_return pException;

		co_return pRow;
	}

	NConcurrency::TCWrapped<CSqlRowDataBatch> CSQLiteDatabaseHandle::f_Select(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values, CSqlRowMapping const &_Mapping)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);

		CSqlRowDataBatch Batch;
		for (;;)
		{
			auto pRow = co_await f_SelectNext(*Statement.m_pStatement, _Mapping);
			if (!pRow)
				break;
			Batch.f_InsertLast(fg_Move(pRow));
		}

		co_return Batch;
	}

	CSqlValue fg_SqliteRawColumnValue(sqlite3_stmt &_Statement, int _iColumn)
	{
		int ColumnType = sqlite3_column_type(&_Statement, _iColumn);
		switch (ColumnType)
		{
		case SQLITE_NULL: return {};
		case SQLITE_INTEGER: return int64(sqlite3_column_int64(&_Statement, _iColumn));
		case SQLITE_FLOAT: return fp64(sqlite3_column_double(&_Statement, _iColumn));
		case SQLITE_TEXT:
			{
				auto const *pText = sqlite3_column_text(&_Statement, _iColumn);
				if (!pText)
					return NStr::CStr();

				return NStr::CStr(reinterpret_cast<ch8 const *>(pText), umint(sqlite3_column_bytes(&_Statement, _iColumn)));
			}
		case SQLITE_BLOB:
			{
				auto const *pData = static_cast<uint8 const *>(sqlite3_column_blob(&_Statement, _iColumn));
				auto nBytes = sqlite3_column_bytes(&_Statement, _iColumn);
				return NContainer::CIOByteVector(pData, umint(nBytes));
			}
		}

		return {};
	}

	ESqlValueType fg_SqliteRawColumnValueType(int _SqliteType)
	{
		switch (_SqliteType)
		{
		case SQLITE_NULL: return ESqlValueType::mc_Null;
		case SQLITE_INTEGER: return ESqlValueType::mc_Integer64;
		case SQLITE_FLOAT: return ESqlValueType::mc_Float64;
		case SQLITE_TEXT: return ESqlValueType::mc_Text;
		case SQLITE_BLOB: return ESqlValueType::mc_Blob;
		}

		return ESqlValueType::mc_Null;
	}

	ESqlValueType fg_SqliteRawDeclaredColumnValueType(ch8 const *_pDeclaredType)
	{
		if (!_pDeclaredType)
			return ESqlValueType::mc_Null;

		NStr::CStr DeclaredType(_pDeclaredType);
		if (DeclaredType.f_FindNoCase("INT") >= 0)
			return ESqlValueType::mc_Integer64;
		if (DeclaredType.f_FindNoCase("CHAR") >= 0 || DeclaredType.f_FindNoCase("CLOB") >= 0 || DeclaredType.f_FindNoCase("TEXT") >= 0)
			return ESqlValueType::mc_Text;
		if (DeclaredType.f_FindNoCase("BLOB") >= 0)
			return ESqlValueType::mc_Blob;
		if (DeclaredType.f_FindNoCase("REAL") >= 0 || DeclaredType.f_FindNoCase("FLOA") >= 0 || DeclaredType.f_FindNoCase("DOUB") >= 0)
			return ESqlValueType::mc_Float64;

		return ESqlValueType::mc_Null;
	}

	NContainer::TCVector<CSqlRawColumnDescription> fg_SqliteRawColumns(sqlite3_stmt &_Statement)
	{
		NContainer::TCVector<CSqlRawColumnDescription> Columns;
		int nColumns = sqlite3_column_count(&_Statement);
		Columns.f_Reserve(umint(nColumns));
		for (int iColumn = 0; iColumn < nColumns; ++iColumn)
		{
			CSqlRawColumnDescription Column;
			auto const *pName = sqlite3_column_name(&_Statement, iColumn);
			Column.m_Name = pName ? NStr::CStr(pName) : NStr::CStr();
			Column.m_BackendTypeID = 0;
			Column.m_ValueType = fg_SqliteRawDeclaredColumnValueType(sqlite3_column_decltype(&_Statement, iColumn));
			Columns.f_InsertLast(fg_Move(Column));
		}

		return Columns;
	}

	void fg_SqliteUpdateRawColumnTypes(NContainer::TCVector<CSqlRawColumnDescription> &_Columns, CSqlRawRow const &_Row)
	{
		umint nColumns = fg_Min(_Columns.f_GetLen(), _Row.m_Values.f_GetLen());
		for (umint iColumn = 0; iColumn < nColumns; ++iColumn)
		{
			if (_Columns[iColumn].m_ValueType == ESqlValueType::mc_Null && _Row.m_Values[iColumn].f_GetTypeID() != ESqlValueType::mc_Null)
				_Columns[iColumn].m_ValueType = _Row.m_Values[iColumn].f_GetTypeID();
		}
	}

	bool fg_SqliteRawColumnsHaveUnknownType(NContainer::TCVector<CSqlRawColumnDescription> const &_Columns)
	{
		for (auto const &Column : _Columns)
		{
			if (Column.m_ValueType == ESqlValueType::mc_Null)
				return true;
		}

		return false;
	}

	NConcurrency::TCWrapped<CSqlRawResult> CSQLiteDatabaseHandle::f_SelectRaw(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);

		co_return co_await f_SelectRaw(fg_Move(Statement));
	}

	// The first step prepares the statement again when another connection changed the schema, which can change its columns, such as
	// those of SELECT *, so they are read after it
	NConcurrency::TCWrapped<CSqlRawResult> CSQLiteDatabaseHandle::f_SelectRaw(CSqliteStatement _Statement)
	{
		CSqlRawResult Result;
		int StepResult = sqlite3_step(_Statement.m_pStatement);
		Result.m_Columns = fg_SqliteRawColumns(*_Statement.m_pStatement);
		int nColumns = int(Result.m_Columns.f_GetLen());

		for (;; StepResult = sqlite3_step(_Statement.m_pStatement))
		{
			if (StepResult == SQLITE_DONE)
				break;

			if (StepResult != SQLITE_ROW)
				co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite raw select");

			CSqlRawRow Row;
			Row.m_Values.f_Reserve(umint(nColumns));
			for (int iColumn = 0; iColumn < nColumns; ++iColumn)
				Row.m_Values.f_InsertLast(fg_SqliteRawColumnValue(*_Statement.m_pStatement, iColumn));

			fg_SqliteUpdateRawColumnTypes(Result.m_Columns, Row);

			Result.m_Rows.f_InsertLast(fg_Move(Row));
		}

		co_return Result;
	}

	// The columns are read after the first step, as in f_SelectRaw
	auto fg_SqliteRawStreamStart(CSQLiteDatabaseHandle &_Database, sqlite3_stmt &_Statement) -> NConcurrency::TCWrapped<CSqliteRawStreamStart>
	{
		CSqliteRawStreamStart Start;

		auto FirstRow = co_await _Database.f_SelectRawNext(_Statement);
		Start.m_Columns = fg_SqliteRawColumns(_Statement);
		if (!FirstRow)
		{
			Start.m_bExhausted = true;

			co_return fg_Move(Start);
		}

		fg_SqliteUpdateRawColumnTypes(Start.m_Columns, *FirstRow);
		Start.m_FirstRows.f_InsertLast(fg_Move(*FirstRow));

		while (fg_SqliteRawColumnsHaveUnknownType(Start.m_Columns) && Start.m_FirstRows.f_GetLen() < gc_nSqliteRawTypeInferenceRowLimit)
		{
			auto NextRow = co_await _Database.f_SelectRawNext(_Statement);
			if (!NextRow)
			{
				Start.m_bExhausted = true;
				break;
			}

			fg_SqliteUpdateRawColumnTypes(Start.m_Columns, *NextRow);
			Start.m_FirstRows.f_InsertLast(fg_Move(*NextRow));
		}

		co_return fg_Move(Start);
	}

	NConcurrency::TCWrapped<CSqliteStatement> CSQLiteDatabaseHandle::f_PrepareRaw(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		return f_Prepare(_Sql, _Values);
	}

	bool CSQLiteDatabaseHandle::f_RawStatementIsReadOnly(CSqliteStatement const &_Statement) const
	{
		return sqlite3_stmt_readonly(_Statement.m_pStatement) != 0;
	}

	NConcurrency::TCWrapped<NStorage::TCOptional<CSqlRawRow>> CSQLiteDatabaseHandle::f_SelectRawNext(sqlite3_stmt &_Statement)
	{
		int StepResult = sqlite3_step(&_Statement);
		if (StepResult == SQLITE_DONE)
			co_return NStorage::TCOptional<CSqlRawRow>();

		if (StepResult != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite raw select");

		int nColumns = sqlite3_column_count(&_Statement);
		CSqlRawRow Row;
		Row.m_Values.f_Reserve(umint(nColumns));
		for (int iColumn = 0; iColumn < nColumns; ++iColumn)
			Row.m_Values.f_InsertLast(fg_SqliteRawColumnValue(_Statement, iColumn));

		co_return NStorage::TCOptional<CSqlRawRow>(fg_Move(Row));
	}

	NConcurrency::TCWrapped<int64> CSQLiteDatabaseHandle::f_SelectInteger(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_ROW)
			co_return fg_SqliteError(mp_pDatabase, "Failed to execute SQLite scalar select");

		int64 Value = sqlite3_column_int64(Statement.m_pStatement, 0);

		Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(mp_pDatabase, "SQLite scalar select returned unexpected extra rows");

		co_return Value;
	}

	void CSQLiteDatabaseHandle::f_Close()
	{
		fp_Close();
		m_bNeedsRawSqlReset = false;
		m_bSynchronousNormal = false;
		m_JournalMode.f_Clear();
		m_MaxPageCount = 0;
	}

	void CSQLiteDatabaseHandle::fp_Close()
	{
		if (!mp_pDatabase)
			return;

		for (auto iEntry = mp_StatementCache.f_Entries().f_GetIterator(); iEntry; ++iEntry)
		{
			if (iEntry->f_Value())
				sqlite3_finalize(iEntry->f_Value());
		}
		mp_StatementCache.f_Clear();

		auto Result = sqlite3_close(mp_pDatabase);
		(void)Result;
		DMibSafeCheck(Result == SQLITE_OK, "Failed to close SQLite database");
		mp_pDatabase = nullptr;
	}

	// Times are stored as "YYYY-MM-DD HH:MM:SS" followed by the fraction in up to nine digits without trailing zeros. A
	// whole second matches CURRENT_TIMESTAMP, and the text orders as time since the fraction follows the seconds. Years
	// outside 0-9999 would break that order and SQLite's date functions, so they are rejected.
	static bool fsg_SqliteFormatTime(NTime::CTime const &_Time, NStr::CStr &o_Text)
	{
		using namespace NStr;

		// A default-constructed time is invalid, and formatting it would store a valid time instead
		if (!_Time.f_IsValid())
			return false;

		NTime::CTimeConvert::CDateTime DateTime;
		NTime::CTimeConvert(NTime::CTime::fs_Create(_Time.f_GetSeconds())).f_ExtractDateTime(DateTime);
		if (DateTime.m_Year < 0 || DateTime.m_Year > 9999)
			return false;

		o_Text
			= "{sj4,sf0}-{sj2,sf0}-{sj2,sf0} {sj2,sf0}:{sj2,sf0}:{sj2,sf0}"_f
			<< DateTime.m_Year
			<< DateTime.m_Month
			<< DateTime.m_DayOfMonth
			<< DateTime.m_Hour
			<< DateTime.m_Minute
			<< DateTime.m_Second
		;

		int32 Nanoseconds = (_Time.f_GetFraction() * 1000000000.0).f_ToIntRound();
		if (Nanoseconds > 999999999)
			Nanoseconds = 999999999;

		if (Nanoseconds == 0)
			return true;

		ch8 Digits[10];
		for (umint iDigit = 9; iDigit-- > 0;)
		{
			Digits[iDigit] = ch8('0' + Nanoseconds % 10);
			Nanoseconds /= 10;
		}

		umint nDigits = 9;
		while (Digits[nDigits - 1] == '0')
			--nDigits;
		Digits[nDigits] = 0;

		o_Text += ".";
		o_Text += Digits;

		return true;
	}

	// "YYYY-MM-DD HH:MM:SS", with the fields checked since raw SQL and defaults can store any text
	static bool fsg_SqliteParseTimeSeconds(NTime::CTime &o_Time, NStr::CStr const &_Text)
	{
		ch8 const *pChar = _Text.f_GetStr();
		auto fParseNumber = [&](umint _nDigits, int32 &o_Value)
			{
				o_Value = 0;
				for (umint iDigit = 0; iDigit < _nDigits; ++iDigit)
				{
					if (*pChar < '0' || *pChar > '9')
						return false;

					o_Value = o_Value * 10 + (*pChar++ - '0');
				}

				return true;
			}
		;
		auto fParseSeparator = [&](ch8 _Separator)
			{
				if (*pChar != _Separator)
					return false;

				++pChar;
				return true;
			}
		;

		int32 Year;
		int32 Month;
		int32 Day;
		int32 Hour;
		int32 Minute;
		int32 Second;
		if
		(
			!fParseNumber(4, Year)
			|| !fParseSeparator('-')
			|| !fParseNumber(2, Month)
			|| !fParseSeparator('-')
			|| !fParseNumber(2, Day)
			|| !fParseSeparator(' ')
			|| !fParseNumber(2, Hour)
			|| !fParseSeparator(':')
			|| !fParseNumber(2, Minute)
			|| !fParseSeparator(':')
			|| !fParseNumber(2, Second)
			|| *pChar
		)
		{
			return false;
		}

		constexpr int32 c_DaysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
		if (Month < 1 || Month > 12 || Hour > 23 || Minute > 59 || Second > 59)
			return false;

		bool bLeapYear = (Year % 4 == 0 && Year % 100 != 0) || Year % 400 == 0;
		int32 nDays = c_DaysInMonth[Month - 1] + (Month == 2 && bLeapYear ? 1 : 0);
		if (Day < 1 || Day > nDays)
			return false;

		o_Time = NTime::CTimeConvert::fs_CreateTime(Year, Month, Day, Hour, Minute, Second, 0.0);

		return true;
	}

	// Accepts any number of fraction digits, as defaults and raw SQL can write them. Comparisons in SQL use the stored text,
	// so values that should match bound times are expected in the form fsg_SqliteFormatTime writes.
	static bool fsg_SqliteParseTime(NTime::CTime &o_Time, NStr::CStr const &_Text)
	{
		aint iFraction = _Text.f_FindChar('.');
		if (!fsg_SqliteParseTimeSeconds(o_Time, iFraction >= 0 ? _Text.f_Left(umint(iFraction)) : _Text))
			return false;

		if (iFraction < 0)
			return true;

		ch8 const *pFraction = _Text.f_GetStr() + iFraction;
		if (!pFraction[1])
			return false;

		int64 Nanoseconds = 0;
		umint nDigits = 0;
		for (ch8 const *pChar = pFraction + 1; *pChar; ++pChar)
		{
			if (*pChar < '0' || *pChar > '9')
				return false;

			if (nDigits < 9)
			{
				Nanoseconds = Nanoseconds * 10 + (*pChar - '0');
				++nDigits;
			}
		}

		for (; nDigits < 9; ++nDigits)
			Nanoseconds *= 10;

		o_Time.f_SetFraction(fp64(Nanoseconds) / 1000000000.0);

		return true;
	}

	static bool fsg_SqliteIsOnlyWhitespaceAndComments(ch8 const *_pSql)
	{
		ch8 const *pChar = _pSql;
		while (*pChar)
		{
			if (*pChar == ' ' || *pChar == '\t' || *pChar == '\n' || *pChar == '\r' || *pChar == '\f' || *pChar == '\v' || *pChar == ';')
				++pChar;
			else if (pChar[0] == '-' && pChar[1] == '-')
			{
				while (*pChar && *pChar != '\n')
					++pChar;
			}
			else if (pChar[0] == '/' && pChar[1] == '*')
			{
				pChar += 2;
				while (*pChar && !(pChar[0] == '*' && pChar[1] == '/'))
					++pChar;
				if (*pChar)
					pChar += 2;
			}
			else
				return false;
		}

		return true;
	}

	// Preparing some PRAGMAs already changes the connection, so trailing SQL is found without preparing. sqlite3_complete tells
	// semicolons in trigger bodies from statement ends, and is asked only at semicolons outside literals, quotes and comments.
	static bool fsg_SqliteHasTrailingStatement(NStr::CStr const &_Sql)
	{
		ch8 const *pSql = _Sql.f_GetStr();
		aint Length = _Sql.f_GetLen();
		aint iChar = 0;
		auto fSkipQuoted = [&](ch8 _Close)
			{
				for (++iChar; iChar < Length; ++iChar)
				{
					if (pSql[iChar] == _Close)
					{
						if (_Close != ']' && iChar + 1 < Length && pSql[iChar + 1] == _Close)
							++iChar;
						else
							break;
					}
				}
				++iChar;
			}
		;

		// sqlite3_complete also accepts empty statements, which sqlite3_prepare_v2 skips before the first statement
		bool bStatementStarted = false;
		while (iChar < Length)
		{
			ch8 Char = pSql[iChar];
			if (Char == '\'' || Char == '"' || Char == '`')
			{
				bStatementStarted = true;
				fSkipQuoted(Char);
			}
			else if (Char == '[')
			{
				bStatementStarted = true;
				fSkipQuoted(']');
			}
			else if (Char == '-' && iChar + 1 < Length && pSql[iChar + 1] == '-')
			{
				while (iChar < Length && pSql[iChar] != '\n')
					++iChar;
			}
			else if (Char == '/' && iChar + 1 < Length && pSql[iChar + 1] == '*')
			{
				iChar += 2;
				while (iChar < Length && !(pSql[iChar] == '*' && iChar + 1 < Length && pSql[iChar + 1] == '/'))
					++iChar;
				iChar += 2;
			}
			else if (Char == ';')
			{
				if (bStatementStarted)
				{
					NStr::CStr Prefix = _Sql.f_Left(umint(iChar + 1));
					if (sqlite3_complete(Prefix.f_GetStr()))
						return !fsg_SqliteIsOnlyWhitespaceAndComments(pSql + iChar + 1);
				}

				++iChar;
			}
			else
			{
				if (Char != ' ' && Char != '\t' && Char != '\n' && Char != '\r' && Char != '\f' && Char != '\v')
					bStatementStarted = true;

				++iChar;
			}
		}

		return false;
	}

	// SQLite compiles text only up to a NUL, so a string holding one would silently lose what follows it
	static bool fsg_SqliteHasEmbeddedNul(NStr::CStr const &_Sql)
	{
		ch8 const *pSql = _Sql.f_GetStr();
		for (aint iChar = 0; iChar < _Sql.f_GetLen(); ++iChar)
		{
			if (pSql[iChar] == 0)
				return true;
		}

		return false;
	}

	NConcurrency::TCWrapped<CSqliteStatement> CSQLiteDatabaseHandle::fp_Prepare(NStr::CStr const &_Sql)
	{
		if (fsg_SqliteHasEmbeddedNul(_Sql))
			co_return DMibErrorDatabaseInstance("SQLite SQL must not contain NUL characters");

		if (fsg_SqliteHasTrailingStatement(_Sql))
			co_return DMibErrorDatabaseInstance("SQLite SQL must contain a single statement; trailing statements or tokens are not executed");

		sqlite3_stmt *pStatement = nullptr;
		char const *pTail = nullptr;
		auto Result = sqlite3_prepare_v2(mp_pDatabase, _Sql.f_GetStr(), -1, &pStatement, &pTail);

		if (Result != SQLITE_OK)
			co_return fg_SqliteError(mp_pDatabase, "Failed to prepare SQLite statement");

		// sqlite3_prepare_v2 compiles only the first statement, so the remainder may hold only whitespace, comments and
		// semicolons. It is scanned rather than prepared: preparing some PRAGMAs already changes the connection.
		if (pTail && !fsg_SqliteIsOnlyWhitespaceAndComments(pTail))
		{
			if (pStatement)
				sqlite3_finalize(pStatement);

			co_return DMibErrorDatabaseInstance("SQLite SQL must contain a single statement; trailing statements or tokens are not executed");
		}

		// sqlite3_prepare_v2 can return SQLITE_OK without producing a statement
		if (!pStatement)
			co_return DMibErrorDatabaseInstance("SQLite SQL contains no statement to execute");

		co_return CSqliteStatement(pStatement);
	}

	NConcurrency::TCWrapped<sqlite3_stmt *> CSQLiteDatabaseHandle::fp_LookupOrPrepare(CSqliteCacheKey _Key, NStr::CStr const &_Sql)
	{
		if (auto *pExisting = mp_StatementCache.f_FindEqual(_Key))
			co_return *pExisting;

		sqlite3_stmt *pStatement = nullptr;
		auto Result = sqlite3_prepare_v2(mp_pDatabase, _Sql.f_GetStr(), -1, &pStatement, nullptr);

		if (Result != SQLITE_OK)
			co_return fg_SqliteError(mp_pDatabase, "Failed to prepare SQLite statement");

		mp_StatementCache[_Key] = pStatement;
		co_return pStatement;
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlValue> const &_Values)
	{
		// SQLite binds NULL to unbound parameters, so a value is required for every index up to the largest, also those ?NNN skips
		if (umint(sqlite3_bind_parameter_count(&_Statement)) != _Values.f_GetLen())
		{
			co_return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("SQLite statement expects {} bound parameter(s) but {} were provided") << umint(sqlite3_bind_parameter_count(&_Statement)) << _Values.f_GetLen()
				)
			;
		}

		for (umint iValue = 0; iValue < _Values.f_GetLen(); ++iValue)
			co_await fp_BindValue(_Statement, iValue + 1, _Values[iValue]);

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		if (umint(sqlite3_bind_parameter_count(&_Statement)) != _Values.f_GetLen())
		{
			co_return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("SQLite statement expects {} bound parameter(s) but {} were provided") << umint(sqlite3_bind_parameter_count(&_Statement)) << _Values.f_GetLen()
				)
			;
		}

		for (umint iValue = 0; iValue < _Values.f_GetLen(); ++iValue)
			co_await fp_BindValue(_Statement, iValue + 1, _Values[iValue].m_Value);

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::fp_BindValue(sqlite3_stmt &_Statement, umint _iParam, CSqlValue const &_Value)
	{
		auto Result = SQLITE_OK;
		switch (_Value.f_GetTypeID())
		{
		case ESqlValueType::mc_Null: Result = sqlite3_bind_null(&_Statement, int(_iParam)); break;
		case ESqlValueType::mc_Integer8: Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int8>()); break;
		case ESqlValueType::mc_Integer16: Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int16>()); break;
		case ESqlValueType::mc_Integer32: Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int32>()); break;
		case ESqlValueType::mc_Integer64: Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int64>()); break;
		case ESqlValueType::mc_UnsignedInteger8: Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<uint8>()); break;
		case ESqlValueType::mc_UnsignedInteger16: Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<uint16>()); break;
		case ESqlValueType::mc_UnsignedInteger32: Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<uint32>()); break;
		case ESqlValueType::mc_UnsignedInteger64:
			{
				uint64 Value = _Value.f_GetAsType<uint64>();
				if (Value > uint64(TCLimitsInt<int64>::mc_Max))
					co_return DMibErrorDatabaseInstance("SQLite unsigned integer parameter exceeds signed 64-bit database range");

				Result = sqlite3_bind_int64(&_Statement, int(_iParam), int64(Value));
				break;
			}
		case ESqlValueType::mc_Float32:
		case ESqlValueType::mc_Float64:
			{
				fp64 Value = _Value.f_GetTypeID() == ESqlValueType::mc_Float32 ? fp64(_Value.f_GetAsType<fp32>()) : _Value.f_GetAsType<fp64>();
				// SQLite stores NaN as NULL, which would read back as a missing value
				if (Value.f_IsNan())
					co_return DMibErrorDatabaseInstance("SQLite cannot store a NaN floating point value");

				Result = sqlite3_bind_double(&_Statement, int(_iParam), Value.f_Get());
				break;
			}
		case ESqlValueType::mc_Text:
			{
				auto const &Value = _Value.f_GetAsType<NStr::CStr>();
				Result = sqlite3_bind_text64(&_Statement, int(_iParam), Value.f_GetStr(), sqlite3_uint64(Value.f_GetLen()), SQLITE_TRANSIENT, SQLITE_UTF8);
				break;
			}
		case ESqlValueType::mc_Blob:
			{
				auto const &Value = _Value.f_GetAsType<NContainer::CIOByteVector>();
				// A null data pointer binds NULL instead of an empty blob
				if (Value.f_IsEmpty())
					Result = sqlite3_bind_zeroblob(&_Statement, int(_iParam), 0);
				else
					Result = sqlite3_bind_blob64(&_Statement, int(_iParam), Value.f_GetArray(), sqlite3_uint64(Value.f_GetLen()), SQLITE_TRANSIENT);
				break;
			}
		case ESqlValueType::mc_Boolean: Result = sqlite3_bind_int(&_Statement, int(_iParam), _Value.f_GetAsType<bool>() ? 1 : 0); break;
		case ESqlValueType::mc_Time:
			{
				NStr::CStr Value;
				if (!fsg_SqliteFormatTime(_Value.f_GetAsType<NTime::CTime>(), Value))
					co_return DMibErrorDatabaseInstance("SQLite time values must be valid and have a year between 0 and 9999");

				Result = sqlite3_bind_text(&_Statement, int(_iParam), Value.f_GetStr(), -1, SQLITE_TRANSIENT);
				break;
			}
		case ESqlValueType::mc_UUID:
		case ESqlValueType::mc_Date:
		case ESqlValueType::mc_TimeOfDay:
		case ESqlValueType::mc_Timestamp:
		case ESqlValueType::mc_TimestampTz:
		case ESqlValueType::mc_Interval:
		case ESqlValueType::mc_Json:
		case ESqlValueType::mc_Jsonb:
		case ESqlValueType::mc_UnrecognizedBackend:
		case ESqlValueType::mc_Array_Integer16:
		case ESqlValueType::mc_Array_Integer32:
		case ESqlValueType::mc_Array_Integer64:
		case ESqlValueType::mc_Array_Float32:
		case ESqlValueType::mc_Array_Float64:
		case ESqlValueType::mc_Array_Text:
		case ESqlValueType::mc_Array_Boolean:
		case ESqlValueType::mc_Array_Bytes:
		case ESqlValueType::mc_Array_Date:
		case ESqlValueType::mc_Array_TimeOfDay:
		case ESqlValueType::mc_Array_Timestamp:
		case ESqlValueType::mc_Array_TimestampTz:
		case ESqlValueType::mc_Array_UUID:
		case ESqlValueType::mc_Array_Json:
		case ESqlValueType::mc_Array_Jsonb:
		case ESqlValueType::mc_Array_Interval:
			co_return fg_SqliteUnsupportedValueTypeError(_Value.f_GetTypeID());
		}

		if (Result != SQLITE_OK)
			co_return fg_SqliteError(mp_pDatabase, "Failed to bind SQLite parameter");

		co_return {};
	}

	NConcurrency::TCWrapped<CSqlValue> fg_SqliteColumnValue(sqlite3_stmt &_Statement, int _iColumn, CSqlRowFieldMapping const &_Field)
	{
		int StorageClass = sqlite3_column_type(&_Statement, _iColumn);
		if (StorageClass == SQLITE_NULL)
			return CSqlValue();

		switch (_Field.m_ValueType)
		{
		case ESqlValueType::mc_Integer8:
		case ESqlValueType::mc_Integer16:
		case ESqlValueType::mc_Integer32:
		case ESqlValueType::mc_Integer64:
		case ESqlValueType::mc_UnsignedInteger8:
		case ESqlValueType::mc_UnsignedInteger16:
		case ESqlValueType::mc_UnsignedInteger32:
		case ESqlValueType::mc_UnsignedInteger64:
		case ESqlValueType::mc_Boolean:
			// sqlite3_column_int64 clamps a REAL (integer arithmetic that overflowed) and turns TEXT into its integer prefix or 0
			if (StorageClass != SQLITE_INTEGER)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains a non-integer value for an integer field") << _Field.m_ColumnName);
			break;
		case ESqlValueType::mc_Float32:
		case ESqlValueType::mc_Float64:
			// sqlite3_column_double turns TEXT and BLOB values into 0 or their numeric prefix
			if (StorageClass != SQLITE_INTEGER && StorageClass != SQLITE_FLOAT)
				return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains a non-numeric value for a floating point field") << _Field.m_ColumnName);
			break;
		default: break;
		}

		switch (_Field.m_ValueType)
		{
		case ESqlValueType::mc_Integer8:
			{
				int64 Value = sqlite3_column_int64(&_Statement, _iColumn);
				if (Value < fg_SqlSignedIntegerMin<int8>() || Value > fg_SqlSignedIntegerMax<int8>())
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an integer outside target signed type range") << _Field.m_ColumnName);

				return int8(Value);
			}
		case ESqlValueType::mc_Integer16:
			{
				int64 Value = sqlite3_column_int64(&_Statement, _iColumn);
				if (Value < fg_SqlSignedIntegerMin<int16>() || Value > fg_SqlSignedIntegerMax<int16>())
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an integer outside target signed type range") << _Field.m_ColumnName);

				return int16(Value);
			}
		case ESqlValueType::mc_Integer32:
			{
				int64 Value = sqlite3_column_int64(&_Statement, _iColumn);
				if (Value < fg_SqlSignedIntegerMin<int32>() || Value > fg_SqlSignedIntegerMax<int32>())
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an integer outside target signed type range") << _Field.m_ColumnName);

				return int32(Value);
			}
		case ESqlValueType::mc_Integer64: return CSqlValue(int64(sqlite3_column_int64(&_Statement, _iColumn)));
		case ESqlValueType::mc_UnsignedInteger8:
			{
				int64 Value = sqlite3_column_int64(&_Statement, _iColumn);
				if (Value < 0 || uint64(Value) > fg_SqlUnsignedIntegerMax<uint8>())
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an integer outside target unsigned type range") << _Field.m_ColumnName);

				return uint8(Value);
			}
		case ESqlValueType::mc_UnsignedInteger16:
			{
				int64 Value = sqlite3_column_int64(&_Statement, _iColumn);
				if (Value < 0 || uint64(Value) > fg_SqlUnsignedIntegerMax<uint16>())
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an integer outside target unsigned type range") << _Field.m_ColumnName);

				return uint16(Value);
			}
		case ESqlValueType::mc_UnsignedInteger32:
			{
				int64 Value = sqlite3_column_int64(&_Statement, _iColumn);
				if (Value < 0 || uint64(Value) > fg_SqlUnsignedIntegerMax<uint32>())
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an integer outside target unsigned type range") << _Field.m_ColumnName);

				return uint32(Value);
			}
		case ESqlValueType::mc_UnsignedInteger64:
			{
				int64 Value = sqlite3_column_int64(&_Statement, _iColumn);
				if (Value < 0)
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains a negative value for an unsigned field") << _Field.m_ColumnName);

				return uint64(Value);
			}
		case ESqlValueType::mc_Float32:
			{
				fp64 Value = fp64(sqlite3_column_double(&_Statement, _iColumn));
				if (!Value.f_IsNan() && !Value.f_IsInfinity() && (Value < fp64(fp32::fs_LimitMin()) || Value > fp64(fp32::fs_LimitMax())))
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains a floating point value outside target fp32 range") << _Field.m_ColumnName);

				return fp32(Value);
			}
		case ESqlValueType::mc_Float64: return fp64(sqlite3_column_double(&_Statement, _iColumn));
		case ESqlValueType::mc_Text:
			{
				auto const *pText = sqlite3_column_text(&_Statement, _iColumn);
				if (!pText)
					return NStr::CStr();

				return NStr::CStr(reinterpret_cast<ch8 const *>(pText), umint(sqlite3_column_bytes(&_Statement, _iColumn)));
			}
		case ESqlValueType::mc_Blob:
			{
				auto const *pData = static_cast<uint8 const *>(sqlite3_column_blob(&_Statement, _iColumn));
				auto nBytes = sqlite3_column_bytes(&_Statement, _iColumn);
				return NContainer::CIOByteVector(pData, umint(nBytes));
			}
		case ESqlValueType::mc_Boolean: return sqlite3_column_int64(&_Statement, _iColumn) != 0;
		case ESqlValueType::mc_Time:
			{
				auto const *pText = sqlite3_column_text(&_Statement, _iColumn);
				NTime::CTime Time;
				if (!pText)
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an invalid time value") << _Field.m_ColumnName);

				if (!fsg_SqliteParseTime(Time, NStr::CStr(reinterpret_cast<ch8 const *>(pText))))
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an invalid time value") << _Field.m_ColumnName);

				return Time;
			}
		case ESqlValueType::mc_UUID: return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' uses unsupported UUID ORM mapping") << _Field.m_ColumnName);
		case ESqlValueType::mc_Date:
		case ESqlValueType::mc_TimeOfDay:
		case ESqlValueType::mc_Timestamp:
		case ESqlValueType::mc_TimestampTz:
		case ESqlValueType::mc_Interval:
		case ESqlValueType::mc_Json:
		case ESqlValueType::mc_Jsonb:
		case ESqlValueType::mc_UnrecognizedBackend:
		case ESqlValueType::mc_Array_Integer16:
		case ESqlValueType::mc_Array_Integer32:
		case ESqlValueType::mc_Array_Integer64:
		case ESqlValueType::mc_Array_Float32:
		case ESqlValueType::mc_Array_Float64:
		case ESqlValueType::mc_Array_Text:
		case ESqlValueType::mc_Array_Boolean:
		case ESqlValueType::mc_Array_Bytes:
		case ESqlValueType::mc_Array_Date:
		case ESqlValueType::mc_Array_TimeOfDay:
		case ESqlValueType::mc_Array_Timestamp:
		case ESqlValueType::mc_Array_TimestampTz:
		case ESqlValueType::mc_Array_UUID:
		case ESqlValueType::mc_Array_Json:
		case ESqlValueType::mc_Array_Jsonb:
		case ESqlValueType::mc_Array_Interval:
			return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' uses unsupported PostgreSQL-specific ORM mapping") << _Field.m_ColumnName);
		case ESqlValueType::mc_Null: return CSqlValue();
		}

		DMibNeverGetHere;
		return CSqlValue();
	}

	NException::CExceptionPointer CSQLiteDatabaseHandle::fp_MapRow(sqlite3_stmt &_Statement, CSqlRowMapping const &_Mapping, ICRowData &_Row)
	{
		for (umint iField = 0; iField < _Mapping.m_Fields.f_GetLen(); ++iField)
		{
			auto const &Field = _Mapping.m_Fields[iField];
			auto *pMember = reinterpret_cast<uint8 *>(&_Row) + Field.m_Offset;
			auto WrappedValue = fg_SqliteColumnValue(_Statement, int(iField), Field);
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			if (auto pException = Field.m_fSetValue(pMember, fg_Move(*WrappedValue), Field.m_ColumnName))
				return pException;
		}

		return {};
	}
}
