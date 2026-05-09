// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

#include "../Malterlib_SQL_SQLite_Init.h"

namespace NMib::NSQL::NPrivate
{
	static umint fsg_SqliteAffectedRows(sqlite3 *_pDatabase, int64 _TotalChangesBefore)
	{
		// sqlite3_changes() reports the row count of the most recent INSERT/UPDATE/DELETE and is left untouched by any
		// statement that changes no rows (CREATE, DROP, PRAGMA, SELECT, ...). Connections are pooled and reused, so a
		// non-DML statement run after an earlier mutation would otherwise echo that mutation's stale count - which the
		// raw execute API surfaces as the affected-row result. sqlite3_total_changes64() only advances for row
		// mutations, so an unchanged total proves this statement affected no rows and the count must be reported as 0.
		if (sqlite3_total_changes64(_pDatabase) == _TotalChangesBefore)
			return umint(0);

		return umint(sqlite3_changes(_pDatabase));
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
		DMibCheck(!m_pDatabase);

		fg_SqliteEnsureInitialized();

		if (sqlite3_open(_Path.f_GetStr(), &m_pDatabase) != SQLITE_OK)
		{
			auto Error = fg_SqliteError(m_pDatabase, "Failed to open SQLite database");
			fp_Close();

			co_return Error;
		}

		co_await f_Execute("PRAGMA foreign_keys=ON");

		sqlite3_busy_timeout(m_pDatabase, 30000);

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_Execute(NStr::CStr const &_Sql)
	{
		auto Statement = co_await fp_Prepare(_Sql);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	NConcurrency::TCWrapped<int64> CSQLiteDatabaseHandle::f_ExecuteReturningLastInsertID(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite insert");

		co_return int64(sqlite3_last_insert_rowid(m_pDatabase));
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(m_pDatabase);
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(m_pDatabase, TotalChangesBefore);
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(m_pDatabase);
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(m_pDatabase, TotalChangesBefore);
	}

	bool CSQLiteDatabaseHandle::f_IsAutocommit() const
	{
		return sqlite3_get_autocommit(m_pDatabase) != 0;
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteBatchAffected(NStr::CStr const &_Sql, NContainer::TCVector<NContainer::TCVector<CSqlColumnValue>> const &_Rows)
	{
		if (_Rows.f_IsEmpty())
			co_return umint(0);

		auto Statement = co_await fp_Prepare(_Sql);

		umint nTotalAffected = 0;
		for (auto const &Row : _Rows)
		{
			int64 TotalChangesBefore = sqlite3_total_changes64(m_pDatabase);
			co_await fp_BindValues(*Statement.m_pStatement, Row);

			auto StepResult = sqlite3_step(Statement.m_pStatement);
			if (StepResult != SQLITE_DONE && StepResult != SQLITE_ROW)
				co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite batch insert statement");

			nTotalAffected += fsg_SqliteAffectedRows(m_pDatabase, TotalChangesBefore);

			auto ResetResult = sqlite3_reset(Statement.m_pStatement);
			if (ResetResult != SQLITE_OK)
				co_return fg_SqliteError(m_pDatabase, "Failed to reset SQLite batch insert statement");

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
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return {};
	}

	auto CSQLiteDatabaseHandle::f_ExecuteReturningLastInsertIDCached
		(
			CSqliteCacheKey _Key
			, NStr::CStr const &_Sql
			, NContainer::TCVector<CSqlColumnValue> const &_Values
		)
		-> NConcurrency::TCWrapped<int64>
	{
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite insert");

		co_return int64(sqlite3_last_insert_rowid(m_pDatabase));
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(m_pDatabase);
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(m_pDatabase, TotalChangesBefore);
	}

	NConcurrency::TCWrapped<umint> CSQLiteDatabaseHandle::f_ExecuteAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		int64 TotalChangesBefore = sqlite3_total_changes64(m_pDatabase);
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);

		if (Result != SQLITE_DONE && Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite statement");

		co_return fsg_SqliteAffectedRows(m_pDatabase, TotalChangesBefore);
	}

	auto CSQLiteDatabaseHandle::f_ExecuteBatchAffectedCached
		(
			CSqliteCacheKey _Key
			, NStr::CStr const &_Sql
			, NContainer::TCVector<NContainer::TCVector<CSqlColumnValue>> const &_Rows
		)
		-> NConcurrency::TCWrapped<umint>
	{
		if (_Rows.f_IsEmpty())
			co_return umint(0);

		auto *pStatement = co_await fp_LookupOrPrepare(_Key, _Sql);
		CSqliteCachedStatement Borrowed(pStatement);

		umint nTotalAffected = 0;
		for (auto const &Row : _Rows)
		{
			int64 TotalChangesBefore = sqlite3_total_changes64(m_pDatabase);
			co_await fp_BindValues(*pStatement, Row);

			auto StepResult = sqlite3_step(pStatement);
			if (StepResult != SQLITE_DONE && StepResult != SQLITE_ROW)
				co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite batch insert statement");

			nTotalAffected += fsg_SqliteAffectedRows(m_pDatabase, TotalChangesBefore);

			auto ResetResult = sqlite3_reset(pStatement);
			if (ResetResult != SQLITE_OK)
				co_return fg_SqliteError(m_pDatabase, "Failed to reset SQLite batch insert statement");

			sqlite3_clear_bindings(pStatement);
		}

		co_return nTotalAffected;
	}

	NConcurrency::TCWrapped<int64> CSQLiteDatabaseHandle::f_SelectIntegerCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_PrepareCached(_Key, _Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite scalar select");

		int64 Value = sqlite3_column_int64(Statement.m_pStatement, 0);

		Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(m_pDatabase, "SQLite scalar select returned unexpected extra rows");

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

		co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite row check");
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
				co_return fg_SqliteError(m_pDatabase, "Failed to read SQLite schema versions");

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

	NConcurrency::TCWrapped<bool> CSQLiteDatabaseHandle::f_HasColumn(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName)
	{
		NStr::CStr Sql;
		{
			NStr::CStr::CAppender Appender(Sql);
			// table_xinfo lists generated (and other hidden) columns that table_info omits, so an existing generated
			// column is detected as present rather than reported missing and re-added by a migration.
			Appender += "PRAGMA table_xinfo(";
			fg_SqliteAppendQuotedIdentifier(Appender, _TableName);
			Appender += ")";
		}

		auto Statement = co_await fp_Prepare(Sql);
		for (;;)
		{
			auto Result = sqlite3_step(Statement.m_pStatement);

			if (Result == SQLITE_DONE)
				co_return false;

			if (Result != SQLITE_ROW)
				co_return fg_SqliteError(m_pDatabase, "Failed to inspect SQLite table columns");

			auto const *pText = sqlite3_column_text(Statement.m_pStatement, 1);

			if (pText && _ColumnName == reinterpret_cast<ch8 const *>(pText))
				co_return true;
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
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite select");

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
		case SQLITE_NULL:
			return {};
		case SQLITE_INTEGER:
			return int64(sqlite3_column_int64(&_Statement, _iColumn));
		case SQLITE_FLOAT:
			return fp64(sqlite3_column_double(&_Statement, _iColumn));
		case SQLITE_TEXT:
			{
				auto const *pText = sqlite3_column_text(&_Statement, _iColumn);
				return NStr::CStr(pText ? reinterpret_cast<ch8 const *>(pText) : "");
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
		case SQLITE_NULL:
			return ESqlValueType::mc_Null;
		case SQLITE_INTEGER:
			return ESqlValueType::mc_Integer64;
		case SQLITE_FLOAT:
			return ESqlValueType::mc_Float64;
		case SQLITE_TEXT:
			return ESqlValueType::mc_Text;
		case SQLITE_BLOB:
			return ESqlValueType::mc_Blob;
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
		for (int i = 0; i < nColumns; ++i)
		{
			CSqlRawColumnDescription Column;
			auto const *pName = sqlite3_column_name(&_Statement, i);
			Column.m_Name = pName ? NStr::CStr(pName) : NStr::CStr();
			Column.m_BackendTypeID = 0;
			Column.m_ValueType = fg_SqliteRawDeclaredColumnValueType(sqlite3_column_decltype(&_Statement, i));
			Columns.f_InsertLast(fg_Move(Column));
		}

		return Columns;
	}

	void fg_SqliteUpdateRawColumnTypes(NContainer::TCVector<CSqlRawColumnDescription> &_Columns, CSqlRawRow const &_Row)
	{
		umint nColumns = fg_Min(_Columns.f_GetLen(), _Row.m_Values.f_GetLen());
		for (umint i = 0; i < nColumns; ++i)
		{
			if (_Columns[i].m_ValueType == ESqlValueType::mc_Null && _Row.m_Values[i].f_GetTypeID() != ESqlValueType::mc_Null)
				_Columns[i].m_ValueType = _Row.m_Values[i].f_GetTypeID();
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

	NConcurrency::TCWrapped<CSqlRawResult> CSQLiteDatabaseHandle::f_SelectRaw(CSqliteStatement _Statement)
	{
		CSqlRawResult Result;
		int nColumns = sqlite3_column_count(_Statement.m_pStatement);
		Result.m_Columns.f_Reserve(umint(nColumns));
		for (int i = 0; i < nColumns; ++i)
		{
			CSqlRawColumnDescription Column;
			auto const *pName = sqlite3_column_name(_Statement.m_pStatement, i);
			Column.m_Name = pName ? NStr::CStr(pName) : NStr::CStr();
			Column.m_BackendTypeID = 0;
			Column.m_ValueType = fg_SqliteRawDeclaredColumnValueType(sqlite3_column_decltype(_Statement.m_pStatement, i));
			Result.m_Columns.f_InsertLast(fg_Move(Column));
		}

		for (;;)
		{
			int StepResult = sqlite3_step(_Statement.m_pStatement);
			if (StepResult == SQLITE_DONE)
				break;

			if (StepResult != SQLITE_ROW)
				co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite raw select");

			CSqlRawRow Row;
			Row.m_Values.f_Reserve(umint(nColumns));
			for (int i = 0; i < nColumns; ++i)
				Row.m_Values.f_InsertLast(fg_SqliteRawColumnValue(*_Statement.m_pStatement, i));

			fg_SqliteUpdateRawColumnTypes(Result.m_Columns, Row);

			Result.m_Rows.f_InsertLast(fg_Move(Row));
		}

		co_return Result;
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
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite raw select");

		int nColumns = sqlite3_column_count(&_Statement);
		CSqlRawRow Row;
		Row.m_Values.f_Reserve(umint(nColumns));
		for (int i = 0; i < nColumns; ++i)
			Row.m_Values.f_InsertLast(fg_SqliteRawColumnValue(_Statement, i));

		co_return NStorage::TCOptional<CSqlRawRow>(fg_Move(Row));
	}

	NConcurrency::TCWrapped<int64> CSQLiteDatabaseHandle::f_SelectInteger(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values)
	{
		auto Statement = co_await f_Prepare(_Sql, _Values);
		auto Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_ROW)
			co_return fg_SqliteError(m_pDatabase, "Failed to execute SQLite scalar select");

		int64 Value = sqlite3_column_int64(Statement.m_pStatement, 0);

		Result = sqlite3_step(Statement.m_pStatement);
		if (Result != SQLITE_DONE)
			co_return fg_SqliteError(m_pDatabase, "SQLite scalar select returned unexpected extra rows");

		co_return Value;
	}

	void CSQLiteDatabaseHandle::fp_Close()
	{
		if (!m_pDatabase)
			return;

		for (auto iEntry = m_StatementCache.f_Entries().f_GetIterator(); iEntry; ++iEntry)
		{
			if (iEntry->f_Value())
				sqlite3_finalize(iEntry->f_Value());
		}
		m_StatementCache.f_Clear();

		auto Result = sqlite3_close(m_pDatabase);
		(void)Result;
		DMibSafeCheck(Result == SQLITE_OK, "Failed to close SQLite database");
		m_pDatabase = nullptr;
	}

	NConcurrency::TCWrapped<CSqliteStatement> CSQLiteDatabaseHandle::fp_Prepare(NStr::CStr const &_Sql)
	{
		sqlite3_stmt *pStatement = nullptr;
		char const *pTail = nullptr;
		auto Result = sqlite3_prepare_v2(m_pDatabase, _Sql.f_GetStr(), -1, &pStatement, &pTail);

		if (Result != SQLITE_OK)
			co_return fg_SqliteError(m_pDatabase, "Failed to prepare SQLite statement");

		// sqlite3_prepare_v2 compiles only the first statement and points pTail at the remainder. A non-whitespace
		// tail means the caller passed more than one statement (or trailing tokens); executing just the first and
		// reporting success would silently drop the rest. Generated ORM/DDL SQL is always a single statement, so this
		// only ever rejects a raw call that smuggled several statements into one string.
		if (pTail)
		{
			char const *pRemainder = pTail;
			while (*pRemainder == ' ' || *pRemainder == '\t' || *pRemainder == '\n' || *pRemainder == '\r' || *pRemainder == '\v' || *pRemainder == '\f')
				++pRemainder;

			if (*pRemainder)
			{
				if (pStatement)
					sqlite3_finalize(pStatement);

				co_return DMibErrorDatabaseInstance("SQLite SQL must contain a single statement; trailing statements or tokens are not executed");
			}
		}

		// sqlite3_prepare_v2 returns SQLITE_OK with a null statement for input that compiles to nothing - an empty,
		// whitespace-only, or comment-only string. Returning that null wrapper would crash bind/query callers, which
		// dereference m_pStatement; reject it as invalid SQL instead.
		if (!pStatement)
			co_return DMibErrorDatabaseInstance("SQLite SQL contains no statement to execute");

		co_return CSqliteStatement(pStatement);
	}

	NConcurrency::TCWrapped<sqlite3_stmt *> CSQLiteDatabaseHandle::fp_LookupOrPrepare(CSqliteCacheKey _Key, NStr::CStr const &_Sql)
	{
		if (auto *pExisting = m_StatementCache.f_FindEqual(_Key))
			co_return *pExisting;

		sqlite3_stmt *pStatement = nullptr;
		auto Result = sqlite3_prepare_v2(m_pDatabase, _Sql.f_GetStr(), -1, &pStatement, nullptr);

		if (Result != SQLITE_OK)
			co_return fg_SqliteError(m_pDatabase, "Failed to prepare SQLite statement");

		m_StatementCache[_Key] = pStatement;
		co_return pStatement;
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlValue> const &_Values)
	{
		// SQLite leaves any unbound placeholder as NULL, so a statement with more placeholders than supplied values
		// would silently run with NULLs. Require an exact match so a raw call like f_QueryRaw("SELECT ?", {}) is rejected.
		if (umint(sqlite3_bind_parameter_count(&_Statement)) != _Values.f_GetLen())
		{
			co_return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("SQLite statement expects {} bound parameter(s) but {} were provided")
					<< umint(sqlite3_bind_parameter_count(&_Statement))
					<< _Values.f_GetLen()
				)
			;
		}

		for (umint i = 0; i < _Values.f_GetLen(); ++i)
			co_await fp_BindValue(_Statement, i + 1, _Values[i]);

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlColumnValue> const &_Values)
	{
		if (umint(sqlite3_bind_parameter_count(&_Statement)) != _Values.f_GetLen())
		{
			co_return DMibErrorDatabaseInstance
				(
					NStr::CStr::CFormat("SQLite statement expects {} bound parameter(s) but {} were provided")
					<< umint(sqlite3_bind_parameter_count(&_Statement))
					<< _Values.f_GetLen()
				)
			;
		}

		for (umint i = 0; i < _Values.f_GetLen(); ++i)
			co_await fp_BindValue(_Statement, i + 1, _Values[i].m_Value);

		co_return {};
	}

	NConcurrency::TCWrapped<void> CSQLiteDatabaseHandle::fp_BindValue(sqlite3_stmt &_Statement, umint _iParam, CSqlValue const &_Value)
	{
		auto Result = SQLITE_OK;
		switch (_Value.f_GetTypeID())
		{
		case ESqlValueType::mc_Null:
			Result = sqlite3_bind_null(&_Statement, int(_iParam));
			break;
		case ESqlValueType::mc_Integer8:
			Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int8>());
			break;
		case ESqlValueType::mc_Integer16:
			Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int16>());
			break;
		case ESqlValueType::mc_Integer32:
			Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int32>());
			break;
		case ESqlValueType::mc_Integer64:
			Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<int64>());
			break;
		case ESqlValueType::mc_UnsignedInteger8:
			Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<uint8>());
			break;
		case ESqlValueType::mc_UnsignedInteger16:
			Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<uint16>());
			break;
		case ESqlValueType::mc_UnsignedInteger32:
			Result = sqlite3_bind_int64(&_Statement, int(_iParam), _Value.f_GetAsType<uint32>());
			break;
		case ESqlValueType::mc_UnsignedInteger64:
			{
				uint64 Value = _Value.f_GetAsType<uint64>();
				if (Value > uint64(TCLimitsInt<int64>::mc_Max))
					co_return DMibErrorDatabaseInstance("SQLite unsigned integer parameter exceeds signed 64-bit database range");

				Result = sqlite3_bind_int64(&_Statement, int(_iParam), int64(Value));
				break;
			}
		case ESqlValueType::mc_Float32:
			Result = sqlite3_bind_double(&_Statement, int(_iParam), _Value.f_GetAsType<fp32>().f_Get());
			break;
		case ESqlValueType::mc_Float64:
			Result = sqlite3_bind_double(&_Statement, int(_iParam), _Value.f_GetAsType<fp64>().f_Get());
			break;
		case ESqlValueType::mc_Text:
			Result = sqlite3_bind_text(&_Statement, int(_iParam), _Value.f_GetAsType<NStr::CStr>().f_GetStr(), -1, SQLITE_TRANSIENT);
			break;
		case ESqlValueType::mc_Blob:
			{
				auto const &Value = _Value.f_GetAsType<NContainer::CIOByteVector>();
				Result = sqlite3_bind_blob(&_Statement, int(_iParam), Value.f_IsEmpty() ? nullptr : Value.f_GetArray(), int(Value.f_GetLen()), SQLITE_TRANSIENT);
				break;
			}
		case ESqlValueType::mc_Boolean:
			Result = sqlite3_bind_int(&_Statement, int(_iParam), _Value.f_GetAsType<bool>() ? 1 : 0);
			break;
		case ESqlValueType::mc_Time:
			{
				NStr::CStr Value = NTime::fg_GetFullTimeStr(_Value.f_GetAsType<NTime::CTime>());
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
			co_return fg_SqliteError(m_pDatabase, "Failed to bind SQLite parameter");

		co_return {};
	}

	NConcurrency::TCWrapped<CSqlValue> fg_SqliteColumnValue(sqlite3_stmt &_Statement, int _iColumn, CSqlRowFieldMapping const &_Field)
	{
		if (sqlite3_column_type(&_Statement, _iColumn) == SQLITE_NULL)
			return CSqlValue();

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
		case ESqlValueType::mc_Integer64:
			return CSqlValue(int64(sqlite3_column_int64(&_Statement, _iColumn)));
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
		case ESqlValueType::mc_Float64:
			return fp64(sqlite3_column_double(&_Statement, _iColumn));
		case ESqlValueType::mc_Text:
			{
				auto const *pText = sqlite3_column_text(&_Statement, _iColumn);
				return NStr::CStr(pText ? reinterpret_cast<ch8 const *>(pText) : "");
			}
		case ESqlValueType::mc_Blob:
			{
				auto const *pData = static_cast<uint8 const *>(sqlite3_column_blob(&_Statement, _iColumn));
				auto nBytes = sqlite3_column_bytes(&_Statement, _iColumn);
				return NContainer::CIOByteVector(pData, umint(nBytes));
			}
		case ESqlValueType::mc_Boolean:
			return sqlite3_column_int64(&_Statement, _iColumn) != 0;
		case ESqlValueType::mc_Time:
			{
				auto const *pText = sqlite3_column_text(&_Statement, _iColumn);
				NTime::CTime Time;
				if (!pText || !NTime::fg_ParseFullTimeStr(Time, reinterpret_cast<ch8 const *>(pText)))
					return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' contains an invalid time value") << _Field.m_ColumnName);

				return Time;
			}
		case ESqlValueType::mc_UUID:
			return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQLite column '{}' uses unsupported UUID ORM mapping") << _Field.m_ColumnName);
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
		case ESqlValueType::mc_Null:
			return CSqlValue();
		}

		DMibNeverGetHere;
		return CSqlValue();
	}

	NException::CExceptionPointer CSQLiteDatabaseHandle::fp_MapRow(sqlite3_stmt &_Statement, CSqlRowMapping const &_Mapping, ICRowData &_Row)
	{
		for (umint i = 0; i < _Mapping.m_Fields.f_GetLen(); ++i)
		{
			auto const &Field = _Mapping.m_Fields[i];
			auto *pMember = reinterpret_cast<uint8 *>(&_Row) + Field.m_Offset;
			auto WrappedValue = fg_SqliteColumnValue(_Statement, int(i), Field);
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			if (auto pException = Field.m_fSetValue(pMember, fg_Move(*WrappedValue), Field.m_ColumnName))
				return pException;
		}

		return {};
	}
}
