// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/PostgresDatabase>
#include <Mib/Container/LinkedList>
#include <Mib/Container/Map>
#include <Mib/Concurrency/AsyncDestroy>
#include <Mib/Concurrency/ActorSequencerActor>
#include <Mib/Concurrency/LogError>
#include <Mib/Intrusive/AVLTree>
#include <Mib/Intrusive/DoublyLinkedList>
#include <Mib/Storage/Pointer>

namespace NMib::NSQL::NPrivate
{
	struct CPostgresPreparedCacheEntryBase
	{
		CSqlQueryID m_QueryID;
		NStr::CStr m_Sql;
		NStr::CStr m_Name;
		NIntrusive::TCAVLLink<> m_QueryIDLink;

		CPostgresPreparedCacheEntryBase() = default;
		CPostgresPreparedCacheEntryBase(CPostgresPreparedCacheEntryBase const &_Other)
			: m_QueryID(_Other.m_QueryID)
			, m_Sql(_Other.m_Sql)
			, m_Name(_Other.m_Name)
		{
		}
		CPostgresPreparedCacheEntryBase &operator =(CPostgresPreparedCacheEntryBase const &_Other)
		{
			m_QueryID = _Other.m_QueryID;
			m_Sql = _Other.m_Sql;
			m_Name = _Other.m_Name;

			return *this;
		}
	};

	struct CPostgresPreparedCacheQueryIDCompare
	{
		CSqlQueryID operator () (CPostgresPreparedCacheEntryBase const &_Node) const
		{
			return _Node.m_QueryID;
		}
	};

	using CPostgresPreparedCacheByQueryID = NIntrusive::TCAVLTree
		<
			&CPostgresPreparedCacheEntryBase::m_QueryIDLink
			, CPostgresPreparedCacheQueryIDCompare
		>
	;

	struct CPostgresPreparedSelectCacheEntry : public CPostgresPreparedCacheEntryBase
	{
		CSqlPreparedSelectStatementDescription m_Description;
		NStr::CStr m_CountSql;
		NStr::CStr m_ExistsSql;
	};

	struct CPostgresPreparedInsertCacheEntry : public CPostgresPreparedCacheEntryBase
	{
		CSqlPreparedInsertStatementDescription m_Description;
	};

	struct CPostgresPreparedUpdateCacheEntry : public CPostgresPreparedCacheEntryBase
	{
		CSqlPreparedUpdateStatementDescription m_Description;
	};

	struct CPostgresPreparedDeleteCacheEntry : public CPostgresPreparedCacheEntryBase
	{
		CSqlPreparedDeleteStatementDescription m_Description;
	};

	struct CPostgresPreparedUpsertCacheEntry : public CPostgresPreparedCacheEntryBase
	{
		CSqlPreparedUpsertStatementDescription m_Description;
	};

	template <typename t_CEntry, typename t_CDescription>
	struct TCPostgresPreparedCache
	{
		using CEntry = t_CEntry;
		using CDescription = t_CDescription;

		NContainer::TCMap<CDescription const *, CEntry> m_ByPointer;
		CPostgresPreparedCacheByQueryID m_ByQueryID;

		TCPostgresPreparedCache() = default;
		TCPostgresPreparedCache(TCPostgresPreparedCache const &_Other)
			: m_ByPointer(_Other.m_ByPointer)
		{
			for (auto iEntry = m_ByPointer.f_Entries().f_GetIterator(); iEntry; ++iEntry)
				m_ByQueryID.f_Insert(static_cast<CPostgresPreparedCacheEntryBase &>(iEntry->f_Value()));
		}
		TCPostgresPreparedCache &operator =(TCPostgresPreparedCache const &_Other)
		{
			if (this == &_Other)
				return *this;

			f_Clear();
			m_ByPointer = _Other.m_ByPointer;
			for (auto iEntry = m_ByPointer.f_Entries().f_GetIterator(); iEntry; ++iEntry)
				m_ByQueryID.f_Insert(static_cast<CPostgresPreparedCacheEntryBase &>(iEntry->f_Value()));

			return *this;
		}
		~TCPostgresPreparedCache()
		{
			m_ByQueryID.f_RemoveAll();
		}

		// Look up by the QueryID-derived content key. Used to answer "is a server-side prepared statement with this
		// statement's SQL already prepared on this connection?" - distinct description globals that hash to the same
		// QueryID share one server-side prepared statement (same name, same SQL), so this must match across them. Do
		// NOT use this to obtain a statement's row mapping: see f_FindByPointer.
		CEntry const *f_Find(CDescription const *_pDescription) const
		{
			if (auto const *pEntry = m_ByPointer.f_FindEqual(_pDescription))
				return pEntry;

			if (auto *pBase = m_ByQueryID.f_FindEqual(_pDescription->m_QueryID))
				return static_cast<CEntry const *>(pBase);

			return nullptr;
		}

		// Look up by the compile-time-unique description pointer only. Used to obtain the cached row mapping for THIS
		// statement. An aliased expression selection carries a client-side row mapping (which result-struct member each
		// decoded value lands in) that does not change the SQL and so cannot be folded into the SQL-derived QueryID -
		// f_Select(fg_SqlAlias<&A::m_X>(e)) and f_Select(fg_SqlAlias<&A::m_Y>(e)) produce the same SQL/QueryID but
		// different mappings. Reusing one's entry for the other (as the QueryID fallback would) would decode rows into
		// the wrong member, so mapping lookups go through the pointer key, which never conflates distinct statements.
		CEntry const *f_FindByPointer(CDescription const *_pDescription) const
		{
			return m_ByPointer.f_FindEqual(_pDescription);
		}

		CEntry &f_Insert(CDescription const *_pDescription, CEntry const &_Entry)
		{
			fp_CheckCollision(_pDescription, _Entry);
			m_ByPointer[_pDescription] = _Entry;
			auto *pEntry = m_ByPointer.f_FindEqual(_pDescription);
			m_ByQueryID.f_Insert(static_cast<CPostgresPreparedCacheEntryBase &>(*pEntry));

			return *pEntry;
		}

		void f_Clear()
		{
			m_ByQueryID.f_RemoveAll();
			m_ByPointer.f_Clear();
		}

		bool f_IsEmpty() const
		{
			return m_ByPointer.f_IsEmpty();
		}

	private:
		void fp_CheckCollision(CDescription const *_pDescription, CEntry const &_Entry) const
		{
#if defined DMibContractConfigure_CheckEnabled
			if (auto *pBase = m_ByQueryID.f_FindEqual(_pDescription->m_QueryID))
			{
				DMibCheck(pBase->m_Name == _Entry.m_Name);
			}
#else
			(void)_pDescription;
			(void)_Entry;
#endif
		}
	};

	using CPostgresPreparedSelectCache = TCPostgresPreparedCache<CPostgresPreparedSelectCacheEntry, CSqlSelectOperationDescription>;
	using CPostgresPreparedInsertCache = TCPostgresPreparedCache<CPostgresPreparedInsertCacheEntry, CSqlInsertOperationDescription>;
	using CPostgresPreparedUpdateCache = TCPostgresPreparedCache<CPostgresPreparedUpdateCacheEntry, CSqlUpdateOperationDescription>;
	using CPostgresPreparedDeleteCache = TCPostgresPreparedCache<CPostgresPreparedDeleteCacheEntry, CSqlDeleteOperationDescription>;
	using CPostgresPreparedUpsertCache = TCPostgresPreparedCache<CPostgresPreparedUpsertCacheEntry, CSqlUpsertOperationDescription>;

	struct CPostgresAppliedSchemaVersion
	{
		NStr::CStr m_ID;
		NStr::CStr m_Checksum;
	};

	struct CPostgresTransactionActor;
	struct CPostgresConnectionPoolEntry;

	struct CPostgresConnectionCheckoutState
	{
		bool m_bReusable = false;
	};

	struct CPostgresConnectionCheckout
	{
		NConcurrency::TCActor<CPostgresClientActor> m_Client;
		CPostgresPreparedSelectCache *m_pPreparedSelectCache = nullptr;
		CPostgresPreparedInsertCache *m_pPreparedInsertCache = nullptr;
		CPostgresPreparedUpdateCache *m_pPreparedUpdateCache = nullptr;
		CPostgresPreparedDeleteCache *m_pPreparedDeleteCache = nullptr;
		CPostgresPreparedUpsertCache *m_pPreparedUpsertCache = nullptr;
		NStorage::TCSharedPointer<CPostgresConnectionCheckoutState> m_pState;
		NConcurrency::CActorSubscription m_ReleaseSubscription;

		void f_MarkReusable(bool _bReusable = true);
	};

	struct CPostgresConnectionPoolEntry
	{
		DMibListLinkDS_Link(CPostgresConnectionPoolEntry, m_FreeLink);

		NConcurrency::TCActor<CPostgresClientActor> m_Client;
		CPostgresPreparedSelectCache m_PreparedSelectCache;
		CPostgresPreparedInsertCache m_PreparedInsertCache;
		CPostgresPreparedUpdateCache m_PreparedUpdateCache;
		CPostgresPreparedDeleteCache m_PreparedDeleteCache;
		CPostgresPreparedUpsertCache m_PreparedUpsertCache;
		bool m_bConnected = false;
	};

	struct CPostgresDatabaseBackendActor : public ICSqlDatabaseBackendActor
	{
		CPostgresDatabaseBackendActor(ICSqlSchemaVersions const *_pSchemaVersions, CPostgresDatabaseBackendSettings _Settings);

		static NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> fs_Select(CPostgresDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<umint> fs_Count(CPostgresDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<bool> fs_Exists(CPostgresDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation);

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
		friend struct CPostgresTransactionActor;

		NConcurrency::TCFuture<void> fp_Destroy() override;
		NConcurrency::TCFuture<CPostgresConnectionCheckout> fp_CheckoutConnection();
		NConcurrency::TCFuture<void> fp_ReleaseConnection(CPostgresConnectionPoolEntry *_pEntry, NStorage::TCSharedPointer<CPostgresConnectionCheckoutState> _pState);
		void fp_NotifyConnectionAvailable();
		CPostgresPreparedSelectCacheEntry fp_EnsurePreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		CPostgresPreparedSelectCacheEntry fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		CPostgresPreparedInsertCacheEntry fp_EnsurePreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		CPostgresPreparedInsertCacheEntry fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		CPostgresPreparedUpdateCacheEntry fp_EnsurePreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		CPostgresPreparedUpdateCacheEntry fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		CPostgresPreparedDeleteCacheEntry fp_EnsurePreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		CPostgresPreparedDeleteCacheEntry fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		CPostgresPreparedUpsertCacheEntry fp_EnsurePreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);
		CPostgresPreparedUpsertCacheEntry fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);

		ICSqlSchemaVersions const *m_pSchemaVersions = nullptr;
		CSqlSchemaVersionDescription m_Schema;
		NContainer::TCVector<CSqlSchemaVersionDescription> m_SchemaVersions;
		NContainer::TCVector<CSqlSchemaMigrationDescription> m_SchemaMigrations;
		CPostgresDatabaseBackendSettings m_Settings;
		NContainer::TCLinkedList<CPostgresConnectionPoolEntry> m_ConnectionPool;
		DMibListLinkDS_List(CPostgresConnectionPoolEntry, m_FreeLink) m_FreeConnectionPoolEntries;
		NContainer::TCLinkedList<NConcurrency::TCPromise<void>> m_ConnectionPoolWaiters;
		NStorage::TCSharedPointer<CPostgresPreparedSelectCache> m_pPreparedSelectCache;
		NStorage::TCSharedPointer<CPostgresPreparedInsertCache> m_pPreparedInsertCache;
		NStorage::TCSharedPointer<CPostgresPreparedUpdateCache> m_pPreparedUpdateCache;
		NStorage::TCSharedPointer<CPostgresPreparedDeleteCache> m_pPreparedDeleteCache;
		NStorage::TCSharedPointer<CPostgresPreparedUpsertCache> m_pPreparedUpsertCache;
	};

	struct CPostgresTransactionActor : public ICSqlTransactionActor
	{
		CPostgresTransactionActor
			(
				bool _bReadOnly
				, CSqlTransactionSettings _TransactionSettings
				, CPostgresConnectionCheckout _ConnectionCheckout
				, umint _nSelectRowsPerBatch
				, uint32 _nPipelineLength
				, NConcurrency::TCActor<CPostgresDatabaseBackendActor> _Backend
				, NStorage::TCSharedPointer<CPostgresPreparedSelectCache> _pPreparedSelectCache
				, NStorage::TCSharedPointer<CPostgresPreparedInsertCache> _pPreparedInsertCache
				, NStorage::TCSharedPointer<CPostgresPreparedUpdateCache> _pPreparedUpdateCache
				, NStorage::TCSharedPointer<CPostgresPreparedDeleteCache> _pPreparedDeleteCache
				, NStorage::TCSharedPointer<CPostgresPreparedUpsertCache> _pPreparedUpsertCache
			)
		;

		static NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> fs_Select(CPostgresTransactionActor *_pTransaction, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<umint> fs_Count(CPostgresTransactionActor *_pTransaction, CSqlSelectOperation _Operation);
		static NConcurrency::TCFuture<bool> fs_Exists(CPostgresTransactionActor *_pTransaction, CSqlSelectOperation _Operation);

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
		NConcurrency::TCFuture<void> fp_Destroy() override;
		NException::CExceptionPointer fp_CheckNotFinished();
		CPostgresPreparedSelectCacheEntry fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		NConcurrency::TCFuture<void> fp_WarmPreparedSelect(CSqlSelectOperationDescription const *_pDescription);
		CPostgresPreparedInsertCacheEntry fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		NConcurrency::TCFuture<void> fp_WarmPreparedInsert(CSqlInsertOperationDescription const *_pDescription);
		CPostgresPreparedUpdateCacheEntry fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		NConcurrency::TCFuture<void> fp_WarmPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription);
		CPostgresPreparedDeleteCacheEntry fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		NConcurrency::TCFuture<void> fp_WarmPreparedDelete(CSqlDeleteOperationDescription const *_pDescription);
		CPostgresPreparedUpsertCacheEntry fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);
		NConcurrency::TCFuture<void> fp_WarmPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription);

		bool m_bReadOnly = false;
		CSqlTransactionSettings m_TransactionSettings;
		CPostgresConnectionCheckout m_ConnectionCheckout;
		umint m_nSelectRowsPerBatch = 0;
		uint32 m_nPipelineLength = 5;
		NConcurrency::TCActor<CPostgresDatabaseBackendActor> m_Backend;
		NConcurrency::CSequencer m_Sequencer;
		NStorage::TCSharedPointer<CPostgresPreparedSelectCache> m_pPreparedSelectCache;
		NStorage::TCSharedPointer<CPostgresPreparedInsertCache> m_pPreparedInsertCache;
		NStorage::TCSharedPointer<CPostgresPreparedUpdateCache> m_pPreparedUpdateCache;
		NStorage::TCSharedPointer<CPostgresPreparedDeleteCache> m_pPreparedDeleteCache;
		NStorage::TCSharedPointer<CPostgresPreparedUpsertCache> m_pPreparedUpsertCache;
		umint m_iNextSavepoint = 0;
		bool m_bFinished = false;
	};

	void fg_PostgresAppendQuotedIdentifier(NStr::CStr::CAppender &_Appender, NStr::CStr const &_Identifier);
	NStr::CStr const &fg_PostgresColumnDefaultSql(CSqlColumnDescription const &_Column);
	bool fg_PostgresColumnIsGenerated(CSqlColumnDescription const &_Column);
	NStr::CStr fg_PostgresCreateTable(CSqlTableDescription const &_Table, bool _bIfNotExists = false, bool _bIncludeForeignKeys = true);
	NStr::CStr fg_PostgresCreateIndex(CSqlTableDescription const &_Table, CSqlIndexDescription const &_Index, bool _bIfNotExists = false);
	NStr::CStr fg_PostgresAlterTableAddColumn(CSqlTableDescription const &_Table, CSqlColumnDescription const &_Column);
	NStr::CStr fg_PostgresAlterTableAddConstraint(CSqlTableDescription const &_Table, CSqlConstraintDescription const &_Constraint);
	NStr::CStr fg_PostgresRenameTable(CSqlSchemaMigrationOperationDescription const &_Operation);
	NStr::CStr fg_PostgresRenameColumn(CSqlSchemaMigrationOperationDescription const &_Operation);
	NStr::CStr fg_PostgresDropTable(NStr::CStr const &_TableName);
	NStr::CStr fg_PostgresDropColumn(CSqlSchemaMigrationOperationDescription const &_Operation);
	NStr::CStr fg_PostgresUpdateColumnSql(CSqlSchemaMigrationOperationDescription const &_Operation);
	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_PostgresMigrationOperationStatements
		(
			CSqlSchemaMigrationOperationDescription const &_Operation
			, CSqlSchemaVersionDescription const &_TargetSchema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
	;
	NConcurrency::TCWrapped<NContainer::TCVector<NStr::CStr>> fg_PostgresSyncAdditiveSchemaStatements
		(
			CSqlSchemaVersionDescription const &_Schema
			, CSqlSchemaVersionDescription const *_pPreviousSchema
		)
	;
	NContainer::TCVector<NStr::CStr> fg_PostgresCreateSchemaStatements(CSqlSchemaVersionDescription const &_Schema);
	NStr::CStr fg_PostgresCreateSchemaVersionTableSql();
	NStr::CStr fg_PostgresReadSchemaVersionsSql();
	NStr::CStr fg_PostgresHasTableSql();
	NStr::CStr fg_PostgresHasColumnSql();
	NStr::CStr fg_PostgresHasConstraintSql();
	NStr::CStr fg_PostgresInsertSchemaVersionSql();
	NStr::CStr fg_PostgresInsertSql(CSqlInsertOperation const &_Operation);
	NStr::CStr fg_PostgresUpsertSql(CSqlUpsertOperation const &_Operation);
	NStr::CStr fg_PostgresUpsertSql(CSqlPreparedUpsertStatementDescription const &_Description);
	NStr::CStr fg_PostgresUpdateSql(CSqlUpdateOperation const &_Operation);
	NStr::CStr fg_PostgresUpdateSql(CSqlPreparedUpdateStatementDescription const &_Description);
	NStr::CStr fg_PostgresDeleteSql(CSqlDeleteOperation const &_Operation);
	NStr::CStr fg_PostgresDeleteSql(CSqlPreparedDeleteStatementDescription const &_Description);
	NStr::CStr fg_PostgresSelectSql(CSqlPreparedSelectStatementDescription const &_Statement, umint _nWhereParameterCount);
	NStr::CStr fg_PostgresSelectCountSql(CSqlPreparedSelectStatementDescription const &_Statement);
	NStr::CStr fg_PostgresSelectExistsSql(CSqlPreparedSelectStatementDescription const &_Statement);
	NException::CExceptionPointer fg_PostgresValidateSelectStatement(CSqlPreparedSelectStatementDescription const &_Statement);
	NStr::CStr fg_PostgresPreparedInsertName(CSqlQueryID _QueryID);
	NStr::CStr fg_PostgresPreparedSelectName(CSqlQueryID _QueryID);
	NStr::CStr fg_PostgresPreparedUpdateName(CSqlQueryID _QueryID);
	NStr::CStr fg_PostgresPreparedDeleteName(CSqlQueryID _QueryID);
	NStr::CStr fg_PostgresPreparedUpsertName(CSqlQueryID _QueryID);
	NConcurrency::TCFuture<umint> fg_PostgresExecuteCount(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values);
	NConcurrency::TCFuture<bool> fg_PostgresExecuteExists(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values);
	NConcurrency::TCFuture<umint> fg_PostgresExecuteAffected(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Values);
	NConcurrency::TCFuture<umint> fg_PostgresExecutePreparedAffected(NConcurrency::TCActor<CPostgresClientActor> _Client, NStr::CStr _Name, NContainer::TCVector<CPostgresValue> _Values);
	NException::CExceptionPointer fg_PostgresParseAffectedRows(CPostgresQueryResult const &_Result, umint &o_nRows);
	auto fg_PostgresParameterBatchGenerator(NConcurrency::TCAsyncGenerator<CSqlBulkInsertRowBatch> _RowBatches, uint32 _nPipelineLength)
		-> NConcurrency::TCAsyncGenerator<NContainer::TCVector<NContainer::TCVector<CPostgresValue>>>
	;
	NConcurrency::TCFuture<CSqlValue> fg_PostgresExecuteReturningValue
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, NStr::CStr _Sql
			, NContainer::TCVector<CPostgresValue> _Values
			, CSqlRowFieldMapping _Field
		)
	;
	NConcurrency::TCWrapped<CPostgresValue> fg_PostgresValue(CSqlValue &&_Value);
	NConcurrency::TCWrapped<NContainer::TCVector<CPostgresValue>> fg_PostgresValues(NContainer::TCVector<CSqlValue> &&_Values);
	NConcurrency::TCWrapped<NContainer::TCVector<CPostgresValue>> fg_PostgresValues(NContainer::TCVector<CSqlColumnValue> &&_Values);
	EPostgresValueType fg_PostgresValueType(ESqlValueType _ValueType);
	NContainer::TCVector<EPostgresValueType> fg_PostgresValueTypes(NContainer::TCVector<ESqlValueType> const &_ValueTypes);
	NContainer::TCVector<EPostgresValueType> fg_PostgresParameterTypes(CSqlParameterTypesDescription _ParameterTypes);
	NConcurrency::TCWrapped<CSqlValue> fg_PostgresSqlValue(CPostgresValue &&_Value, CSqlRowFieldMapping const &_Field);
	CSqlValue fg_PostgresSqlValueRaw(CPostgresValue &&_Value);
	ESqlValueType fg_PostgresSqlValueTypeForBackendType(EPostgresValueType _Type);
	NConcurrency::TCWrapped<CSqlRowDataPointer> fg_PostgresMapRow(CPostgresDataRow &&_Row, CSqlRowMapping const &_Mapping);
	CSqlRawResult fg_PostgresRawResult(CPostgresQueryResult &&_QueryResult);
	NContainer::TCVector<CSqlRawColumnDescription> fg_PostgresRawColumns(CPostgresRowDescription const &_Description);
	CSqlRawRow fg_PostgresRawRow(CPostgresDataRow &&_Row);
}
