// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/SQLiteDatabase>
#include <Mib/SQL/SQL>
#include <Mib/Concurrency/ActorSequencerActor>
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
		void const *m_pDescription = nullptr;
		ESqliteCacheBucket m_Bucket = ESqliteCacheBucket::mc_SelectMain;

		constexpr auto operator <=> (CSqliteCacheKey const &_Other) const noexcept = default;
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

	struct CSqliteAppliedSchemaVersion
	{
		NStr::CStr m_ID;
		NStr::CStr m_Checksum;
	};

	struct CSQLiteDatabaseHandle
	{
		~CSQLiteDatabaseHandle();

		NConcurrency::TCWrapped<void> f_Open(NStr::CStr const &_Path);
		NConcurrency::TCWrapped<void> f_Execute(NStr::CStr const &_Sql);
		NConcurrency::TCWrapped<void> f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<void> f_Execute(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<int64> f_ExecuteReturningLastInsertID(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteAffected(NStr::CStr const &_Sql, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<umint> f_ExecuteBatchAffected(NStr::CStr const &_Sql, NContainer::TCVector<NContainer::TCVector<CSqlColumnValue>> const &_Rows);
		// False when a transaction is open on the connection (autocommit disabled), e.g. after a raw BEGIN/SAVEPOINT.
		bool f_IsAutocommit() const;
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
		NConcurrency::TCWrapped<void> f_MarkSchemaVersionApplied(NStr::CStr const &_SchemaVersionID);
		NConcurrency::TCWrapped<void> f_MarkSchemaVersionApplied(CSqlSchemaVersionDescription const &_SchemaVersion);
		NConcurrency::TCWrapped<CSqlRowDataPointer> f_SelectNext(sqlite3_stmt &_Statement, CSqlRowMapping const &_Mapping);
		NConcurrency::TCWrapped<CSqlRowDataBatch> f_Select(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values, CSqlRowMapping const &_Mapping);
		NConcurrency::TCWrapped<CSqlRawResult> f_SelectRaw(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<CSqlRawResult> f_SelectRaw(CSqliteStatement _Statement);
		NConcurrency::TCWrapped<NStorage::TCOptional<CSqlRawRow>> f_SelectRawNext(sqlite3_stmt &_Statement);
		NConcurrency::TCWrapped<CSqliteStatement> f_PrepareRaw(NStr::CStr const &_Sql, NContainer::TCVector<CSqlValue> const &_Values);
		bool f_RawStatementIsReadOnly(CSqliteStatement const &_Statement) const;

	private:
		void fp_Close();
		NConcurrency::TCWrapped<CSqliteStatement> fp_Prepare(NStr::CStr const &_Sql);
		NConcurrency::TCWrapped<sqlite3_stmt *> fp_LookupOrPrepare(CSqliteCacheKey _Key, NStr::CStr const &_Sql);
		NConcurrency::TCWrapped<void> fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlValue> const &_Values);
		NConcurrency::TCWrapped<void> fp_BindValues(sqlite3_stmt &_Statement, NContainer::TCVector<CSqlColumnValue> const &_Values);
		NConcurrency::TCWrapped<void> fp_BindValue(sqlite3_stmt &_Statement, umint _iParam, CSqlValue const &_Value);
		NException::CExceptionPointer fp_MapRow(sqlite3_stmt &_Statement, CSqlRowMapping const &_Mapping, ICRowData &_Row);

		sqlite3 *m_pDatabase = nullptr;
		NContainer::TCMap<CSqliteCacheKey, sqlite3_stmt *> m_StatementCache;
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

	struct CSQLiteDatabasePool
	{
		NContainer::TCLinkedList<CSQLiteDatabaseHandleEntry> m_Entries;
		DMibListLinkDS_List(CSQLiteDatabaseHandleEntry, m_FreeLink) m_FreeEntries;
		NContainer::TCLinkedList<NConcurrency::TCPromise<void>> m_Waiters;
	};

	struct CSQLiteDatabaseBackendActor : public ICSqlDatabaseBackendActor
	{
		CSQLiteDatabaseBackendActor(ICSqlSchemaVersions const *_pSchemaVersions, CSQLiteDatabaseBackendSettings _Settings);

		static NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> fs_Select(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<umint> fs_Count(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<bool> fs_Exists(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static auto fs_QueryRawRowStream
			(
				CSQLiteDatabaseBackendActor *_pBackend
				, CSQLiteDatabaseCheckout _Checkout
				, NStorage::TCSharedPointer<CSqliteStatement> _pStatement
				, CSqlRawRowBatch _FirstRows
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

		CPreparedSelectCacheEntry const &fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		CPreparedInsertCacheEntry const &fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		CPreparedUpdateCacheEntry const &fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		CPreparedDeleteCacheEntry const &fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		CPreparedUpsertCacheEntry const &fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);
		NConcurrency::TCFuture<CSQLiteDatabaseCheckout> fp_CheckoutWriteDatabase();
		NConcurrency::TCFuture<CSQLiteDatabaseCheckout> fp_CheckoutReadDatabase();
		void fp_ReleaseEntry(CSQLiteDatabasePool &_Pool, CSQLiteDatabaseHandleEntry *_pEntry);
		CSQLiteDatabaseCheckout fp_MakeCheckout(CSQLiteDatabasePool &_Pool, CSQLiteDatabaseHandleEntry *_pEntry);

		ICSqlSchemaVersions const *m_pSchemaVersions = nullptr;
		CSqlSchemaVersionDescription m_Schema;
		NContainer::TCVector<CSqlSchemaVersionDescription> m_SchemaVersions;
		NContainer::TCVector<CSqlSchemaMigrationDescription> m_SchemaMigrations;
		CSQLiteDatabaseBackendSettings m_Settings;
		bool m_bSingleConnection = false;
		CSQLiteDatabasePool m_WritePool;
		CSQLiteDatabasePool m_ReadPool;
		NContainer::TCMap<CPreparedSelectStatementKey, CPreparedSelectCacheEntry> m_PreparedSelectCache;
		NContainer::TCMap<CPreparedInsertStatementKey, CPreparedInsertCacheEntry> m_PreparedInsertCache;
		NContainer::TCMap<CPreparedUpdateStatementKey, CPreparedUpdateCacheEntry> m_PreparedUpdateCache;
		NContainer::TCMap<CPreparedDeleteStatementKey, CPreparedDeleteCacheEntry> m_PreparedDeleteCache;
		NContainer::TCMap<CPreparedUpsertStatementKey, CPreparedUpsertCacheEntry> m_PreparedUpsertCache;
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
		NConcurrency::TCFuture<void> f_CommitTransaction() override;
		NConcurrency::TCFuture<void> f_RollbackTransaction() override;

	private:
		NException::CExceptionPointer fp_CheckNotFinished();
		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> fp_DatabasePointer();
		CPreparedSelectCacheEntry const &fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		CPreparedInsertCacheEntry const &fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		CPreparedUpdateCacheEntry const &fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		CPreparedDeleteCacheEntry const &fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		CPreparedUpsertCacheEntry const &fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);

		CSqlSchemaVersionDescription m_Schema;
		CSQLiteDatabaseBackendSettings m_Settings;
		CSqlTransactionSettings m_TransactionSettings;
		bool m_bReadOnly = false;
		CSQLiteDatabaseCheckout m_Checkout;
		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> m_pOwnedDatabase;
		NConcurrency::CSequencer m_Sequencer;
		NContainer::TCMap<CPreparedSelectStatementKey, CPreparedSelectCacheEntry> m_PreparedSelectCache;
		NContainer::TCMap<CPreparedInsertStatementKey, CPreparedInsertCacheEntry> m_PreparedInsertCache;
		NContainer::TCMap<CPreparedUpdateStatementKey, CPreparedUpdateCacheEntry> m_PreparedUpdateCache;
		NContainer::TCMap<CPreparedDeleteStatementKey, CPreparedDeleteCacheEntry> m_PreparedDeleteCache;
		NContainer::TCMap<CPreparedUpsertStatementKey, CPreparedUpsertCacheEntry> m_PreparedUpsertCache;
		umint m_iNextSavepoint = 0;
		bool m_bFinished = false;
	};


	CSqlErrorData fg_SqliteErrorData(sqlite3 *_pDatabase);
	CExceptionSql fg_SqliteError(sqlite3 *_pDatabase, NStr::CStr const &_Message);

	NException::CExceptionPointer fg_SqliteUnsupportedColumnTypeError(ESqlColumnType _Type);
	NException::CExceptionPointer fg_SqliteUnsupportedValueTypeError(ESqlValueType _Type);
	CSqlDatabaseBackendCapabilities fg_SqliteCapabilities();
	NContainer::TCVector<CSqlRawColumnDescription> fg_SqliteRawColumns(sqlite3_stmt &_Statement);
	void fg_SqliteUpdateRawColumnTypes(NContainer::TCVector<CSqlRawColumnDescription> &_Columns, CSqlRawRow const &_Row);
	bool fg_SqliteRawColumnsHaveUnknownType(NContainer::TCVector<CSqlRawColumnDescription> const &_Columns);
	// SQLite reports no declared type for expression columns, so a raw stream infers each column's value type from the
	// first non-NULL row. A column that is NULL for every row (e.g. SELECT NULL AS value) can never be resolved, so the
	// inference pass is capped at this many leading rows to keep the stream from buffering the entire result before it
	// starts flowing; any column still unresolved after the cap keeps its NULL type.
	inline constexpr umint gc_nSqliteRawTypeInferenceRowLimit = 64;
	CSqlValue fg_SqliteRawColumnValue(sqlite3_stmt &_Statement, int _iColumn);
	ESqlValueType fg_SqliteRawColumnValueType(int _SqliteType);
	void fg_SqliteAppendQuotedIdentifier(NStr::CStr::CAppender &_Appender, NStr::CStr const &_Identifier);
	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteCreateTable(CSqlTableDescription const &_Table, bool _bIfNotExists);
	NStr::CStr fg_SqliteCreateIndex(CSqlTableDescription const &_Table, CSqlIndexDescription const &_Index, bool _bIfNotExists = false);
	NConcurrency::TCWrapped<NStr::CStr> fg_SqliteAlterTableAddColumn(CSqlTableDescription const &_Table, CSqlColumnDescription const &_Column);
	NStr::CStr fg_SqliteRenameTable(CSqlSchemaMigrationOperationDescription const &_Operation);
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
	bool fg_SqliteUsesSingleConnection(NStr::CStr const &_Path);
	NConcurrency::TCWrapped<void> fg_SqliteApplyMigrationOperations
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaMigrationDescription const &_Migration
			, CSqlSchemaVersionDescription const &_TargetSchema
		)
	;
	NConcurrency::TCWrapped<void> fg_SqliteSyncAdditiveSchema
		(
			CSQLiteDatabaseHandle &_Database
			, CSqlSchemaVersionDescription const &_Schema
			, CSqlSchemaVersionDescription const *_pPreviousSchema = nullptr
		)
	;
	CSqlSchemaMigrationDescription const *fg_SqliteFindMigration
		(
			NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations
			, NStr::CStr const &_FromVersionID
			, NStr::CStr const &_ToVersionID
		)
	;
	void fg_SqliteApplyMigrationOperationToPlannedSchema
		(
			CSqlSchemaVersionDescription &_Schema
			, CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
		)
	;
	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_SqliteRebuildTableStatements
		(
			CSqlTableDescription const &_Table
			, NContainer::TCVector<NStr::CStr const *> const &_ExistingColumns
		)
	;
	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_SqliteMigrationOperationStatements
		(
			CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
	;
	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_SqliteSyncAdditiveSchemaStatements
		(
			CSqlSchemaVersionDescription const &_Schema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
	;
}
