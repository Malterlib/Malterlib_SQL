// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/SQLiteDatabase>
#include <Mib/SQL/SQL>
#include <Mib/Concurrency/ActorSequencerActor>
#include <Mib/Concurrency/LogError>
#include <Mib/Container/LinkedList>
#include <Mib/Intrusive/DoublyLinkedList>
#include <Mib/Storage/Pointer>

#include "../../SourceGenerated/SQLite/sqlite3.h"

namespace NMib::NSQL::NPrivate
{
	using CPreparedSelectStatementKey = CSqlSelectOperationDescription const *;
	using CPreparedInsertStatementKey = CSqlInsertOperationDescription const *;
	using CPreparedUpdateStatementKey = CSqlUpdateOperationDescription const *;
	using CPreparedDeleteStatementKey = CSqlDeleteOperationDescription const *;
	using CPreparedUpsertStatementKey = CSqlUpsertOperationDescription const *;

	struct CPreparedSelectCacheEntry
	{
		CSqlPreparedSelectStatementDescription m_Description;
		NStr::CStr m_Sql;
		NStr::CStr m_CountSql;
		NStr::CStr m_ExistsSql;
	};

	struct CPreparedInsertCacheEntry
	{
		CSqlPreparedInsertStatementDescription m_Description;
		NStr::CStr m_Sql;
	};

	struct CPreparedUpdateCacheEntry
	{
		CSqlPreparedUpdateStatementDescription m_Description;
		NStr::CStr m_Sql;
	};

	struct CPreparedDeleteCacheEntry
	{
		CSqlPreparedDeleteStatementDescription m_Description;
		NStr::CStr m_Sql;
	};

	struct CPreparedUpsertCacheEntry
	{
		CSqlPreparedUpsertStatementDescription m_Description;
		NStr::CStr m_Sql;
	};

	struct CSqliteStatement
	{
		CSqliteStatement() = default;
		CSqliteStatement(sqlite3_stmt *_pStatement);
		CSqliteStatement(CSqliteStatement &&_Other);
		~CSqliteStatement();
		CSqliteStatement &operator = (CSqliteStatement &&_Other);

		sqlite3_stmt &f_Statement() const;

		sqlite3_stmt *m_pStatement = nullptr;
	};

	enum class ESqliteCacheBucket : uint8
	{
		mc_SelectMain
		, mc_SelectCount
		, mc_SelectExists
		, mc_Insert
		, mc_Update
		, mc_Delete
		, mc_Upsert
	};

	struct CSqliteCacheKey
	{
		constexpr auto operator <=> (CSqliteCacheKey const &_Other) const noexcept = default;

		void const *m_pDescription = nullptr;
		ESqliteCacheBucket m_Bucket = ESqliteCacheBucket::mc_SelectMain;
	};

	struct CSqliteCachedStatement
	{
		CSqliteCachedStatement() = default;
		CSqliteCachedStatement(sqlite3_stmt *_pStatement);
		CSqliteCachedStatement(CSqliteCachedStatement &&_Other);
		~CSqliteCachedStatement();
		CSqliteCachedStatement &operator = (CSqliteCachedStatement &&_Other);

		sqlite3_stmt *m_pStatement = nullptr;
	};

	struct CSqliteRawStreamStart
	{
		NContainer::TCVector<CSqlRawColumnDescription> m_Columns;
		CSqlRawRowBatch m_FirstRows; // Read to infer the types of columns without a known declared type, and delivered first
		bool m_bExhausted = false;
	};

	struct CSqliteAppliedSchemaVersion
	{
		NStr::CStr m_ID;
		NStr::CStr m_Checksum;
	};

	constexpr int gc_SqliteBusyTimeoutMilliseconds = 30000;

	// Connection settings that raw SQL can change and that restoring the connection sets back to what it opened with. They
	// change how statements perform, such as automatic_index for joins, or what deleting leaves in the file.
	inline constexpr ch8 const *gc_SqliteRestoredPragmas[] =
		{
			"analysis_limit"
			, "automatic_index"
			, "cache_size"
			, "cache_spill"
			, "cell_size_check"
			, "checkpoint_fullfsync"
			, "fullfsync"
			, "journal_size_limit"
			, "mmap_size"
			, "read_uncommitted"
			, "secure_delete"
			, "temp_store"
			, "threads"
			, "wal_autocheckpoint"
		}
	;

	struct CSQLiteDatabaseHandle
	{
		~CSQLiteDatabaseHandle();

		NConcurrency::TCWrapped<void> f_Open(NStr::CStr const &_Path);
		void f_Close();
		NConcurrency::TCWrapped<void> f_Execute(NStr::CStr const &_Sql);
		NConcurrency::TCWrapped<void> f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<void> f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<int64> f_ExecuteReturningLastInsertID(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteBatchAffected(NStr::CStr const &_Sql, NContainer::TCVector<NContainer::TCVector<CSqlColumnValue>> const &_Rows);
		bool f_IsAutocommit() const;
		bool f_IsOpen() const;
		NConcurrency::TCWrapped<CSqliteStatement> f_Prepare(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<CSqliteStatement> f_Prepare(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<CSqliteCachedStatement> f_PrepareCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<CSqliteCachedStatement> f_PrepareCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<void> f_ExecuteCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<int64> f_ExecuteReturningLastInsertIDCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteBatchAffectedCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<NContainer::TCVector<CSqlColumnValue>> const &_Rows);
		NConcurrency::TCWrapped<int64> f_SelectIntegerCached(CSqliteCacheKey _Key, NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<int64> f_SelectInteger(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<bool> f_HasRows(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<bool> f_HasSchemaVersion(NStr::CStr const &_SchemaVersionID);
		NConcurrency::TCWrapped<NContainer::TCVector<CSqliteAppliedSchemaVersion>> f_ReadAppliedSchemaVersions();
		NConcurrency::TCWrapped<void> f_ValidateAppliedSchemaVersions(NContainer::TCVector<CSqlSchemaVersionDescription> const &_ExpectedVersions);
		NConcurrency::TCWrapped<bool> f_HasTable(NStr::CStr const &_TableName);
		NConcurrency::TCWrapped<bool> f_HasColumn(NStr::CStr const &_TableName, NStr::CStr const &_ColumnName);
		NConcurrency::TCWrapped<void> f_RestoreAfterRawSql(bool _bReadConnection);

		NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> f_ColumnNames(NStr::CStr const &_TableName);
		NConcurrency::TCWrapped<void> f_MarkSchemaVersionApplied(NStr::CStr const &_SchemaVersionID);
		NConcurrency::TCWrapped<void> f_MarkSchemaVersionApplied(CSqlSchemaVersionDescription const &_SchemaVersion);
		NConcurrency::TCWrapped<CSqlRowDataPointer> f_SelectNext(sqlite3_stmt &_Statement, CSqlRowMapping const &_Mapping);
		NConcurrency::TCWrapped<CSqlRowDataBatch> f_Select(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values, CSqlRowMapping const &_Mapping);
		NConcurrency::TCWrapped<CSqlRawResult> f_SelectRaw(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<CSqlRawResult> f_SelectRaw(CSqliteStatement _Statement);
		NConcurrency::TCWrapped<NStorage::TCOptional<CSqlRawRow>> f_SelectRawNext(sqlite3_stmt &_Statement);
		NConcurrency::TCWrapped<CSqliteStatement> f_PrepareRaw(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		bool f_RawStatementIsReadOnly(CSqliteStatement const &_Statement) const;
		NConcurrency::TCWrapped<void> f_RecordJournalMode();

		bool m_bNeedsRawSqlReset = false;                    // Raw SQL or a failed ROLLBACK can have left state behind, and restoring the connection has not yet succeeded
		bool m_bSynchronousNormal = false;                   // Opened with synchronous=NORMAL instead of SQLite's default FULL
		NStr::CStr m_JournalMode;                            // The journal mode SQLite reported after the backend set it up, in lower case
		int64 m_MaxPageCount = 0;                            // The page limit SQLite reported when the connection opened
		bool m_bTrustedSchema = true;                        // The trusted_schema setting the connection opened with
		NContainer::TCVector<NStr::CStr> m_RestorePragmaSql; // Statements that set the settings of gc_SqliteRestoredPragmas back to what the connection opened with

	private:
		void fp_Close();
		NConcurrency::TCWrapped<CSqliteStatement> fp_Prepare(NStr::CStr const &_Sql);
		NConcurrency::TCWrapped<sqlite3_stmt *> fp_LookupOrPrepare(CSqliteCacheKey _Key, NStr::CStr const &_Sql);
		NConcurrency::TCWrapped<void> fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<void> fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<void> fp_BindValue(sqlite3_stmt &_Statement, umint _iParam, CSqlValue const &_Value);
		NException::CExceptionPointer fp_MapRow(sqlite3_stmt &_Statement, CSqlRowMapping const &_Mapping, ICRowData &_Row);

		sqlite3 *mp_pDatabase = nullptr;
		NContainer::TCMap<CSqliteCacheKey, sqlite3_stmt *> mp_StatementCache;
	};

	struct CSQLiteTransactionActor;
	struct CSQLiteDatabaseBackendActor;

	struct CSQLiteDatabaseHandleEntry
	{
		DMibListLinkDS_Link(CSQLiteDatabaseHandleEntry, m_FreeLink);

		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> m_pDatabase;
	};

	struct CSQLiteDatabaseCheckout
	{
		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> m_pDatabase;
		NConcurrency::CActorSubscription m_ReleaseSubscription;
	};

	enum class ESQLiteDatabasePool : uint8
	{
		mc_Write
		, mc_Read
	};

	struct CSQLiteDatabasePoolWaiter
	{
		NConcurrency::TCPromise<void> m_Promise;
		CSQLiteDatabasePoolWaiter *m_pOtherPoolWaiter = nullptr; // The same wait in the other pool's list, which waking this one removes
	};

	struct CSQLiteDatabasePool
	{
		NContainer::TCLinkedList<CSQLiteDatabaseHandleEntry> m_Entries;
		DMibListLinkDS_List(CSQLiteDatabaseHandleEntry, m_FreeLink) m_FreeEntries;
		NContainer::TCLinkedList<CSQLiteDatabasePoolWaiter> m_Waiters;
	};

	NConcurrency::TCFuture<void> fg_SqliteWaitForEitherPool(CSQLiteDatabasePool &_ReadPool, CSQLiteDatabasePool &_WritePool);
	void fg_SqliteWakePoolWaiter(CSQLiteDatabasePool &_Pool, CSQLiteDatabasePool &_OtherPool);

	struct CSQLiteDatabaseBackendActor : public ICSqlDatabaseBackendActor
	{
		CSQLiteDatabaseBackendActor(ICSqlSchemaVersions const *_pSchemaVersions, CSQLiteDatabaseBackendSettings _Settings);

		static NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> fs_Select(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<umint> fs_Count(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<bool> fs_Exists(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static auto fs_QueryRawRowStream
			(
				CSQLiteDatabaseCheckout _Checkout
				, NStorage::TCSharedPointer<CSqliteStatement> _pStatement
				, CSqlRawRowBatch _FirstRows
				, bool _bExhausted
				, umint _nRowsPerBatch
			)
			-> NConcurrency::TCAsyncGenerator<CSqlRawRowBatch>
		;

		CSqlDatabaseBackendCapabilities f_Capabilities() const override;
		NConcurrency::TCFuture<void> f_Open() override;
		NConcurrency::TCFuture<void> f_ApplySchema() override;
		NConcurrency::TCFuture<void> f_Insert(CSqlInsertOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_InsertMany(CSqlBulkInsertOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_InsertReturning(CSqlInsertOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Upsert(CSqlUpsertOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_UpsertReturning(CSqlUpsertOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Update(CSqlUpdateOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_UpdateReturning(CSqlUpdateOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Delete(CSqlDeleteOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_DeleteReturning(CSqlDeleteOperation _Operation) override;
		NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> f_Select(CSqlSelectOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Count(CSqlSelectOperation _Operation) override;
		NConcurrency::TCFuture<bool> f_Exists(CSqlSelectOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_ExecuteRaw(CSqlRawOperation _Operation) override;
		NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(CSqlRawOperation _Operation) override;
		NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(CSqlRawOperation _Operation) override;
		NConcurrency::TCFuture<CSqlTransactionInterface> f_BeginTransaction(CSqlTransactionSettings _Settings = {}) override;
		NConcurrency::TCFuture<CSqlTransactionInterface> f_BeginReadTransaction(CSqlTransactionSettings _Settings = {}) override;

	private:
		friend struct CSQLiteTransactionActor;

		NConcurrency::TCFuture<void> fp_Destroy() override;
		CPreparedSelectCacheEntry const &fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		CPreparedInsertCacheEntry const &fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		CPreparedUpdateCacheEntry const &fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		CPreparedDeleteCacheEntry const &fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		CPreparedUpsertCacheEntry const &fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);
		NConcurrency::TCFuture<CSQLiteDatabaseCheckout> fp_CheckoutWriteDatabase(bool _bOpening = false);
		NConcurrency::TCFuture<CSQLiteDatabaseCheckout> fp_CheckoutReadDatabase();
		NConcurrency::TCFuture<void> fp_WaitForOpen();
		NConcurrency::TCFuture<void> fp_WaitForEitherPool();
		void fp_PassOnWriteWake();
		CSQLiteDatabasePool &fp_GetPool(ESQLiteDatabasePool _Pool);
		void fp_ReleaseEntry(ESQLiteDatabasePool _Pool, CSQLiteDatabaseHandleEntry *_pEntry);
		CSQLiteDatabaseCheckout fp_MakeCheckout(ESQLiteDatabasePool _Pool, CSQLiteDatabaseHandleEntry *_pEntry);

		ICSqlSchemaVersions const *mp_pSchemaVersions = nullptr;
		CSqlSchemaVersionDescription mp_Schema;
		NContainer::TCVector<CSqlSchemaVersionDescription> mp_SchemaVersions;
		NContainer::TCVector<CSqlSchemaMigrationDescription> mp_SchemaMigrations;
		CSQLiteDatabaseBackendSettings mp_Settings;
		bool mp_bSingleConnection = false;
		umint mp_nOpening = 0; // f_Open calls in progress, which reads wait for while the read pool has no connections
		CSQLiteDatabasePool mp_WritePool;
		CSQLiteDatabasePool mp_ReadPool;
		NContainer::TCMap<CPreparedSelectStatementKey, CPreparedSelectCacheEntry> mp_PreparedSelectCache;
		NContainer::TCMap<CPreparedInsertStatementKey, CPreparedInsertCacheEntry> mp_PreparedInsertCache;
		NContainer::TCMap<CPreparedUpdateStatementKey, CPreparedUpdateCacheEntry> mp_PreparedUpdateCache;
		NContainer::TCMap<CPreparedDeleteStatementKey, CPreparedDeleteCacheEntry> mp_PreparedDeleteCache;
		NContainer::TCMap<CPreparedUpsertStatementKey, CPreparedUpsertCacheEntry> mp_PreparedUpsertCache;
	};

	struct CSQLiteTransactionActor : public ICSqlTransactionActor
	{
		CSQLiteTransactionActor
			(
				CSqlSchemaVersionDescription _Schema
				, CSQLiteDatabaseBackendSettings _Settings
				, CSqlTransactionSettings _TransactionSettings
				, bool _bReadOnly
				, CSQLiteDatabaseCheckout _Checkout
				, NStorage::TCSharedPointer<CSQLiteDatabaseHandle> _pOwnedDatabase
			)
		;

		static NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> fs_Select(CSQLiteTransactionActor *_pTransaction, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<umint> fs_Count(CSQLiteTransactionActor *_pTransaction, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<bool> fs_Exists(CSQLiteTransactionActor *_pTransaction, CSqlSelectOperation _Operation);

		NConcurrency::TCFuture<void> f_OpenBegin();
		NConcurrency::TCFuture<void> f_Insert(CSqlInsertOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_InsertMany(CSqlBulkInsertOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_InsertReturning(CSqlInsertOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Upsert(CSqlUpsertOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_UpsertReturning(CSqlUpsertOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Update(CSqlUpdateOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_UpdateReturning(CSqlUpdateOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Delete(CSqlDeleteOperation _Operation) override;
		NConcurrency::TCFuture<CSqlValue> f_DeleteReturning(CSqlDeleteOperation _Operation) override;
		NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> f_Select(CSqlSelectOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_Count(CSqlSelectOperation _Operation) override;
		NConcurrency::TCFuture<bool> f_Exists(CSqlSelectOperation _Operation) override;
		NConcurrency::TCFuture<umint> f_ExecuteRaw(CSqlRawOperation _Operation) override;
		NConcurrency::TCFuture<CSqlRawResult> f_QueryRaw(CSqlRawOperation _Operation) override;
		NConcurrency::TCFuture<CSqlRawStream> f_QueryRawStream(CSqlRawOperation _Operation) override;
		NConcurrency::TCFuture<NStr::CStr> f_CreateSavepoint() override;
		NConcurrency::TCFuture<void> f_ReleaseSavepoint(NStr::CStr _Name) override;
		NConcurrency::TCFuture<void> f_RollbackToSavepoint(NStr::CStr _Name) override;
		NConcurrency::TCFuture<NStr::CStr> f_CreateScopeSavepoint() override;
		NConcurrency::TCFuture<void> f_EndScopeSavepoint(NStr::CStr _Name, ESqlTransactionScopeEnd _End) override;
		NConcurrency::TCFuture<void> f_CommitTransaction() override;
		NConcurrency::TCFuture<void> f_RollbackTransaction() override;

	private:
		NConcurrency::TCFuture<void> fp_Destroy() override;
		bool fp_EndedBySqlite();
		NConcurrency::TCFuture<void> fp_CloseOwnedDatabase();
		NException::CExceptionPointer fp_CheckNotFinished();
		NConcurrency::TCFuture<NStr::CStr> fp_CreateSavepoint(NPrivate::CSqlSavepointStack::EKind _Kind);
		NConcurrency::TCFuture<void> fp_ExecuteSavepointSql(NStr::CStr _Sql);
		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> fp_DatabasePointer();
		CPreparedSelectCacheEntry const &fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		CPreparedInsertCacheEntry const &fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		CPreparedUpdateCacheEntry const &fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		CPreparedDeleteCacheEntry const &fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		CPreparedUpsertCacheEntry const &fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);

		CSqlSchemaVersionDescription mp_Schema;
		CSQLiteDatabaseBackendSettings mp_Settings;
		CSqlTransactionSettings mp_TransactionSettings;
		bool mp_bReadOnly = false;
		CSQLiteDatabaseCheckout mp_Checkout;
		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> mp_pOwnedDatabase;
		NConcurrency::CSequencer mp_Sequencer;
		NContainer::TCMap<CPreparedSelectStatementKey, CPreparedSelectCacheEntry> mp_PreparedSelectCache;
		NContainer::TCMap<CPreparedInsertStatementKey, CPreparedInsertCacheEntry> mp_PreparedInsertCache;
		NContainer::TCMap<CPreparedUpdateStatementKey, CPreparedUpdateCacheEntry> mp_PreparedUpdateCache;
		NContainer::TCMap<CPreparedDeleteStatementKey, CPreparedDeleteCacheEntry> mp_PreparedDeleteCache;
		NContainer::TCMap<CPreparedUpsertStatementKey, CPreparedUpsertCacheEntry> mp_PreparedUpsertCache;
		umint mp_iNextSavepoint = 0;
		NPrivate::CSqlSavepointStack mp_Savepoints;
		bool mp_bFinished = false;
		bool mp_bCommitted = false;
		bool mp_bEndedBySqlite = false;
	};

	CSqlErrorData fg_SqliteErrorData(sqlite3 *_pDatabase);
	CExceptionSql fg_SqliteError(sqlite3 *_pDatabase, NStr::CStr const &_Message);

	NException::CExceptionPointer fg_SqliteUnsupportedColumnTypeError(ESqlColumnType _Type);
	NException::CExceptionPointer fg_SqliteUnsupportedValueTypeError(ESqlValueType _Type);
	CSqlDatabaseBackendCapabilities fg_SqliteCapabilities();
	NContainer::TCVector<CSqlRawColumnDescription> fg_SqliteRawColumns(sqlite3_stmt &_Statement);
	void fg_SqliteUpdateRawColumnTypes(NContainer::TCVector<CSqlRawColumnDescription> &_Columns, CSqlRawRow const &_Row);
	bool fg_SqliteRawColumnsHaveUnknownType(NContainer::TCVector<CSqlRawColumnDescription> const &_Columns);
	inline constexpr umint gc_nSqliteRawTypeInferenceRowLimit = 64; // Rows a raw stream buffers at most to infer the types of columns without a recognized declared type
	auto fg_SqliteRawStreamStart(CSQLiteDatabaseHandle &_Database, sqlite3_stmt &_Statement) -> NConcurrency::TCWrapped<CSqliteRawStreamStart>;
	CSqlValue fg_SqliteRawColumnValue(sqlite3_stmt &_Statement, int _iColumn);
	ESqlValueType fg_SqliteRawColumnValueType(int _SqliteType);
	void fg_SqliteAppendQuotedIdentifier(NStr::CStr::CAppender &_Appender, NStr::CStr const &_Identifier);
	void fg_SqliteAppendStringLiteral(NStr::CStr::CAppender &_Appender, NStr::CStr const &_String);
	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteCreateTable(CSqlTableDescription const &_Table, bool _bIfNotExists);
	NStr::CStr fg_SqliteColumnType(ESqlColumnType _Type, NException::CExceptionPointer &o_pError);
	void fg_SqliteAppendConstraintDefinition(NStr::CStr::CAppender &_Appender, CSqlConstraintDescription const &_Constraint);
	NConcurrency::TCWrapped<void> fg_SqliteAppendColumnDefinition(NStr::CStr::CAppender &_Appender, CSqlColumnDescription const &_Column);
	NStr::CStr fg_SqliteConstraintDefinitionSql(CSqlConstraintDescription const &_Constraint);
	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteColumnDefinitionSql(CSqlColumnDescription const &_Column);
	NStr::CStr fg_SqliteCreateIndex(CSqlTableDescription const &_Table, CSqlIndexDescription const &_Index, bool _bIfNotExists = false);
	NStr::CStr fg_SqliteCollationName(NStr::CStr const &_CollationSql);
	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteAlterTableAddColumn(CSqlTableDescription const &_Table, CSqlColumnDescription const &_Column);
	NContainer::TCVector<NStr::CStr> fg_SqliteRenameTable(CSqlSchemaMigrationOperationDescription const &_Operation);
	NStr::CStr fg_SqliteRenameColumn(CSqlSchemaMigrationOperationDescription const &_Operation);
	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_SqliteCreateSchemaStatements(CSqlSchemaVersionDescription const &_Schema);
	NStr::CStr fg_SqliteCreateSchemaVersionTableSql();
	NStr::CStr fg_SqliteHasSchemaVersionSql();
	NStr::CStr fg_SqliteReadSchemaVersionsSql();
	NStr::CStr fg_SqliteInsertSchemaVersionSql();
	NStr::CStr fg_SqliteHasTableSql();
	NStr::CStr fg_SqliteDropTableSql(NStr::CStr const &_TableName);
	NStr::CStr fg_SqliteUpdateColumnSql(CSqlSchemaMigrationOperationDescription const &_Operation);
	NStr::CStr fg_SqliteInsertSql(CSqlInsertOperation const &_Operation);
	NStr::CStr fg_SqliteUpsertSql(CSqlUpsertOperation const &_Operation);
	NStr::CStr fg_SqliteUpsertSql(CSqlPreparedUpsertStatementDescription const &_Description);
	NStr::CStr fg_SqliteUpdateSql(CSqlUpdateOperation const &_Operation);
	NStr::CStr fg_SqliteUpdateSql(CSqlPreparedUpdateStatementDescription const &_Description);
	NStr::CStr fg_SqliteDeleteSql(CSqlDeleteOperation const &_Operation);
	NStr::CStr fg_SqliteDeleteSql(CSqlPreparedDeleteStatementDescription const &_Description);
	NStr::CStr fg_SqliteSelectSql(CSqlPreparedSelectStatementDescription const &_Statement);
	NStr::CStr fg_SqliteSelectCountSql(CSqlPreparedSelectStatementDescription const &_Statement);
	NStr::CStr fg_SqliteSelectExistsSql(CSqlPreparedSelectStatementDescription const &_Statement);
	NException::CExceptionPointer fg_SqliteValidateSelectStatement(CSqlPreparedSelectStatementDescription const &_Statement);
	bool fg_SqliteSelectHasSetOperandModifiers(CSqlPreparedSelectStatementDescription const &_Statement);
	bool fg_SqliteUsesSingleConnection(NStr::CStr const &_Path);
	auto fg_SqliteSyncAdditiveSchema(CSQLiteDatabaseHandle &_Database, CSqlSchemaVersionDescription const &_Schema, CSqlSchemaVersionDescription const *_pPreviousSchema = nullptr)
		-> NConcurrency::TCWrapped<void>
	;
	umint fg_SqliteFindMigration(NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations, NStr::CStr const &_FromVersionID, NStr::CStr const &_ToVersionID);
	// Owns the rewritten strings and column options the planned schema points at
	struct CSqlitePlannedSchemaStorage
	{
		NContainer::TCVector<NStorage::TCUniquePointer<NStr::CStr>> m_Strings;
		NContainer::TCVector<NStorage::TCUniquePointer<NContainer::TCVector<CNonPortableColumnOptions>>> m_Options;
	};

	void fg_SqliteApplyMigrationOperationToPlannedSchema
		(
			CSqlSchemaVersionDescription &_Schema
			, CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlitePlannedSchemaStorage &o_Storage
		)
	;
	auto fg_SqliteApplyMigrationOperations
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription &o_PlannedSchema
			, CSqlitePlannedSchemaStorage &o_PlannedSchemaStorage
		)
		-> NConcurrency::TCWrapped<void>
	;
	auto fg_SqliteCheckNoReferencesToDroppedTables(CSQLiteDatabaseHandle &_Database, NContainer::TCVector<NStr::CStr> const &_DroppedTables) -> NConcurrency::TCWrapped<void>;
	NException::CExceptionPointer fg_SqliteValidateSchemaVersions(NContainer::TCVector<CSqlSchemaVersionDescription> const &_Versions);

	struct CSqliteCheckConstraint
	{
		NStr::CStr m_Name;
		NStr::CStr m_Expression; // Encoded tokens, for comparison
		NStr::CStr m_Sql;        // As written, for messages
	};

	enum class ESqliteTableOption : uint8
	{
		mc_None           = 0
		, mc_Strict       = DMibBit(0)
		, mc_WithoutRowid = DMibBit(1)
	};

	NContainer::TCVector<CSqliteCheckConstraint> fg_SqliteTableChecks(NStr::CStr const &_CreateTableSql);
	auto fg_SqliteRebuildTableStatements
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
			, ch8 const *_pCopiedRowid
			, ESqliteTableOption _Options
		)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	;
	auto fg_SqliteRebuildCopiedRowid
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
			, NContainer::TCVector<NStr::CStr> const &_OldColumns
		)
		-> ch8 const *
	;
	auto fg_SqliteMigrationOperationStatements
		(
			CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	;
	auto fg_SqliteSyncAdditiveSchemaStatements(CSqlSchemaVersionDescription const &_Schema, CSqlSchemaVersionDescription const *_pPreviousSchema)
		-> NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>>
	;
}

#include "Malterlib_SQL_SQLiteDatabase_Internal.hpp"
