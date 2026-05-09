// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

#include <Mib/Concurrency/AsyncDestroy>

namespace NMib::NSQL::NPrivate
{
	CSQLiteDatabaseBackendActor::CSQLiteDatabaseBackendActor(ICSqlSchemaVersions const *_pSchemaVersions, CSQLiteDatabaseBackendSettings _Settings)
		: m_pSchemaVersions(_pSchemaVersions)
		, m_Schema(_pSchemaVersions->f_Describe())
		, m_SchemaVersions(_pSchemaVersions->f_DescribeVersions())
		, m_SchemaMigrations(_pSchemaVersions->f_DescribeMigrations())
		, m_Settings(fg_Move(_Settings))
		, m_bSingleConnection(fg_SqliteUsesSingleConnection(m_Settings.m_Path))
	{
		auto &WriteEntry = m_WritePool.m_Entries.f_InsertLast();
		WriteEntry.m_pDatabase = fg_Construct();
		m_WritePool.m_FreeEntries.f_InsertLast(WriteEntry);
	}

	CSQLiteDatabaseCheckout CSQLiteDatabaseBackendActor::fp_MakeCheckout(CSQLiteDatabasePool &_Pool, CSQLiteDatabaseHandleEntry *_pEntry)
	{
		CSQLiteDatabaseCheckout Checkout;
		Checkout.m_pDatabase = _pEntry->m_pDatabase;
		Checkout.m_ReleaseSubscription = NConcurrency::g_ActorSubscription / [this, &_Pool, _pEntry]() -> NConcurrency::TCFuture<void>
			{
				fp_ReleaseEntry(_Pool, _pEntry);

				co_return {};
			}
		;

		return Checkout;
	}

	NConcurrency::TCFuture<CSQLiteDatabaseCheckout> CSQLiteDatabaseBackendActor::fp_CheckoutWriteDatabase()
	{
		for (;;)
		{
			if (auto *pEntry = m_WritePool.m_FreeEntries.f_Pop())
				co_return fp_MakeCheckout(m_WritePool, pEntry);

			NConcurrency::TCPromiseFuturePair<void> Waiter;
			m_WritePool.m_Waiters.f_InsertLast(fg_Move(Waiter.m_Promise));
			co_await fg_Move(Waiter.m_Future);
		}
	}

	NConcurrency::TCFuture<CSQLiteDatabaseCheckout> CSQLiteDatabaseBackendActor::fp_CheckoutReadDatabase()
	{
		if (m_bSingleConnection)
			co_return co_await fp_CheckoutWriteDatabase();

		for (;;)
		{
			if (auto *pEntry = m_ReadPool.m_FreeEntries.f_Pop())
				co_return fp_MakeCheckout(m_ReadPool, pEntry);

			NConcurrency::TCPromiseFuturePair<void> Waiter;
			m_ReadPool.m_Waiters.f_InsertLast(fg_Move(Waiter.m_Promise));
			co_await fg_Move(Waiter.m_Future);
		}
	}

	void CSQLiteDatabaseBackendActor::fp_ReleaseEntry(CSQLiteDatabasePool &_Pool, CSQLiteDatabaseHandleEntry *_pEntry)
	{
		_Pool.m_FreeEntries.f_InsertFirst(_pEntry);

		if (_Pool.m_Waiters.f_IsEmpty())
			return;

		_Pool.m_Waiters.f_PopFirst().f_SetResult();
	}

	CSqlDatabaseBackendCapabilities fg_SqliteCapabilities()
	{
		return
			{
				.m_Dialect = ESqlDialect::mc_SQL1999 | ESqlDialect::mc_SQLite
				, .m_bReadTransactions = true
				, .m_bTransactionalDDL = true
				, .m_bTableRename = true
				, .m_bColumnRename = true
				, .m_bTableRebuild = true
				, .m_bDropColumn = true
				, .m_bForeignKeyEnforcement = true
				, .m_bNumberedPlaceholders = false
				, .m_bUUID = false
				, .m_bDate = false
				, .m_bTimeOfDay = false
				, .m_bTimestamp = false
				, .m_bTimestampTz = false
				, .m_bInterval = false
				, .m_bJSON = false
				, .m_bJSONB = false
				, .m_bArrays = false
				, .m_bUnrecognizedBackend = false
				, .m_bIsolationSerializable = true
			}
		;
	}

	CSqlDatabaseBackendCapabilities CSQLiteDatabaseBackendActor::f_Capabilities() const
	{
		return fg_SqliteCapabilities();
	}

	bool fg_SqliteSelectHasModifiers(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		return !_Statement.m_OrderBy.f_IsEmpty() || _Statement.m_LimitOffset.m_bHasLimit || _Statement.m_LimitOffset.m_bHasOffset;
	}

	bool fg_SqliteSelectHasSetOperandModifiers(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		if (_Statement.m_SetOperations.f_IsEmpty())
			return false;

		// The top-level statement IS the left operand of the compound (f_Describe copies the left operand's
		// clauses up), so this check rejects a modified left operand. Do not remove it as redundant - the loop
		// below only inspects the right-hand m_SetOperations entries.
		if (fg_SqliteSelectHasModifiers(_Statement))
			return true;

		for (auto const &SetOperation : _Statement.m_SetOperations)
		{
			CSqlPreparedSelectStatementDescription OperandDescription = SetOperation.m_pStatement->f_Describe();
			if (fg_SqliteSelectHasModifiers(OperandDescription))
				return true;
			if (fg_SqliteSelectHasSetOperandModifiers(OperandDescription))
				return true;
		}

		return false;
	}

	NException::CExceptionPointer fg_SqliteValidateSelectStatement(CSqlPreparedSelectStatementDescription const &_Statement)
	{
		if (fg_SqliteSelectHasSetOperandModifiers(_Statement))
			return DMibErrorDatabaseInstance("SQLite set operation operands cannot have ORDER BY, LIMIT, or OFFSET modifiers");

		return {};
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CSQLiteDatabaseBackendActor::fs_Select(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto const &StatementEntry = _pBackend->fp_GetPreparedSelect(_Operation.m_pDescription);
		CSqlPreparedSelectStatementDescription const *pStatementDescription = &StatementEntry.m_Description;
		if (auto pException = fg_SqliteValidateSelectStatement(*pStatementDescription))
			co_return pException;

		if (_Operation.m_nResultRowLimit != 0 && !pStatementDescription->m_LimitOffset.m_bHasLimit)
			co_return DMibErrorDatabaseInstance("SELECT specifies a result-row limit but the prepared statement does not declare f_WithLimit()");
		if (_Operation.m_nResultRowOffset != 0 && !pStatementDescription->m_LimitOffset.m_bHasOffset)
			co_return DMibErrorDatabaseInstance("SELECT specifies a result-row offset but the prepared statement does not declare f_WithOffset()");
		auto Sql = StatementEntry.m_Sql;
		auto Mapping = pStatementDescription->m_RowMapping;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		if (pStatementDescription->m_LimitOffset.m_bHasLimit)
			Parameters.f_InsertLast(_Operation.m_nResultRowLimit != 0 ? int64(_Operation.m_nResultRowLimit) : int64(-1));
		if (pStatementDescription->m_LimitOffset.m_bHasOffset)
			Parameters.f_InsertLast(int64(_Operation.m_nResultRowOffset));
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : _pBackend->m_Settings.m_nSelectRowsPerBatch;
		auto pDescriptionKey = _Operation.m_pDescription;

		auto Checkout = co_await _pBackend->fp_CheckoutReadDatabase();
		auto pDatabase = Checkout.m_pDatabase;
		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		auto CachedStatement = co_await pDatabase->f_PrepareCached({pDescriptionKey, ESqliteCacheBucket::mc_SelectMain}, Sql, Parameters);
		CSqlRowDataBatch Batch;

		for (;;)
		{
			auto pRow = co_await pDatabase->f_SelectNext(*CachedStatement.m_pStatement, Mapping);
			if (!pRow)
				break;

			Batch.f_InsertLast(fg_Move(pRow));
			if (Batch.f_GetLen() >= nRowsPerBatch)
				co_yield fg_Move(Batch);
		}

		if (Batch.f_GetLen() != 0)
			co_yield fg_Move(Batch);

		co_return {};
	}

	NConcurrency::TCFuture<umint> CSQLiteDatabaseBackendActor::fs_Count(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto const &StatementEntry = _pBackend->fp_GetPreparedSelect(_Operation.m_pDescription);
		if (auto pException = fg_SqliteValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_CountSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto pDescriptionKey = _Operation.m_pDescription;

		auto Checkout = co_await _pBackend->fp_CheckoutReadDatabase();
		auto pDatabase = Checkout.m_pDatabase;
		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		co_return umint(co_await pDatabase->f_SelectIntegerCached({pDescriptionKey, ESqliteCacheBucket::mc_SelectCount}, Sql, Parameters));
	}

	NConcurrency::TCFuture<bool> CSQLiteDatabaseBackendActor::fs_Exists(CSQLiteDatabaseBackendActor *_pBackend, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto const &StatementEntry = _pBackend->fp_GetPreparedSelect(_Operation.m_pDescription);
		if (auto pException = fg_SqliteValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_ExistsSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto pDescriptionKey = _Operation.m_pDescription;

		auto Checkout = co_await _pBackend->fp_CheckoutReadDatabase();
		auto pDatabase = Checkout.m_pDatabase;
		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		co_return (co_await pDatabase->f_SelectIntegerCached({pDescriptionKey, ESqliteCacheBucket::mc_SelectExists}, Sql, Parameters)) != 0;
	}

	NConcurrency::TCFuture<void> CSQLiteDatabaseBackendActor::f_Open()
	{
		if (!m_bSingleConnection && m_Settings.m_nReadConnections == 0)
			co_return DMibErrorDatabaseInstance("SQLite read connection count must be at least one");

		if (m_Settings.m_nSelectRowsPerBatch == 0)
			co_return DMibErrorDatabaseInstance("SQLite select rows per batch must be at least one");

		auto Path = m_Settings.m_Path;
		auto bSingleConnection = m_bSingleConnection;

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pWriteDatabase, Path, bSingleConnection] -> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_await pWriteDatabase->f_Open(Path);

					if (bSingleConnection)
						co_return {};

					co_await pWriteDatabase->f_Execute("PRAGMA journal_mode=WAL");
					co_await pWriteDatabase->f_Execute("PRAGMA synchronous=NORMAL");

					co_return {};
				}
			)
		;

		if (!m_bSingleConnection)
		{
			// f_Open already rejected a non-single-connection database with m_nReadConnections == 0 above, so a
			// non-single-connection database always builds at least one read-pool entry here; reads can never wait on
			// an empty read pool.
			for (umint i = 0; i < m_Settings.m_nReadConnections; ++i)
			{
				NStorage::TCSharedPointer<CSQLiteDatabaseHandle> pDatabase = fg_Construct();
				co_await
					(
						NConcurrency::g_Dispatch(BlockingActorCheckout) / [pDatabase, Path] -> NConcurrency::TCFuture<void>
						{
							auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

							co_await pDatabase->f_Open(Path);
							co_await pDatabase->f_Execute("PRAGMA query_only=ON");

							co_return {};
						}
					)
				;

				auto &ReadEntry = m_ReadPool.m_Entries.f_InsertLast();
				ReadEntry.m_pDatabase = fg_Move(pDatabase);
				m_ReadPool.m_FreeEntries.f_InsertLast(ReadEntry);
			}
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CSQLiteDatabaseBackendActor::f_ApplySchema()
	{
		auto Checkout = co_await fp_CheckoutWriteDatabase();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) /
				[
					pWriteDatabase = Checkout.m_pDatabase
					, Schema = m_Schema
					, SchemaVersionID = m_Schema.f_ID()
					, SchemaVersions = m_SchemaVersions
					, SchemaMigrations = m_SchemaMigrations
				]
				-> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_await pWriteDatabase->f_Execute(fg_SqliteCreateSchemaVersionTableSql());

					enum class ESchemaTransactionState
					{
						mc_NotStarted
						, mc_Rollback
						, mc_Commit
					};
					ESchemaTransactionState TransactionState = ESchemaTransactionState::mc_NotStarted;

					co_await pWriteDatabase->f_Execute("PRAGMA foreign_keys=OFF");
					auto SchemaCleanup = co_await NConcurrency::fg_AsyncDestroy
						(
							[&]() -> NConcurrency::TCFuture<void>
							{
								auto pDatabase = pWriteDatabase;
								auto State = TransactionState;

								NConcurrency::TCAsyncResult<void> FinishTransactionResult;
								if (State == ESchemaTransactionState::mc_Commit)
								{
									FinishTransactionResult = pDatabase->f_Execute("COMMIT TRANSACTION");
									// A failed COMMIT (for example SQLITE_BUSY) leaves the transaction open in SQLite, so
									// discard it before this pooled write connection is reused; otherwise later work would
									// run inside the abandoned schema transaction.
									if (!FinishTransactionResult)
									{
										NConcurrency::TCAsyncResult<void> RollbackResult = pDatabase->f_Execute("ROLLBACK TRANSACTION");
										(void)RollbackResult;
									}
								}
								else if (State == ESchemaTransactionState::mc_Rollback)
									FinishTransactionResult = pDatabase->f_Execute("ROLLBACK TRANSACTION");
								else
									FinishTransactionResult.f_SetResult();

								co_await pDatabase->f_Execute("PRAGMA foreign_keys=ON");
								// Rebuilds scope legacy_alter_table=ON around their rename. If a rebuild fails between the
								// ON and the matching OFF (for example a rename onto a colliding scratch name), the
								// transaction rollback does not undo the pragma (it is connection-scoped, not
								// transactional), so the pooled write connection would keep legacy ALTER TABLE semantics
								// and later renames would stop rewriting foreign-key references. Always restore it here.
								co_await pDatabase->f_Execute("PRAGMA legacy_alter_table=OFF");

								if (!FinishTransactionResult)
									co_return fg_Move(FinishTransactionResult).f_GetException();

								co_return {};
							}
						)
					;

					co_await pWriteDatabase->f_Execute("BEGIN TRANSACTION");
					TransactionState = ESchemaTransactionState::mc_Rollback;

					co_await pWriteDatabase->f_ValidateAppliedSchemaVersions(SchemaVersions);

					NStr::CStr CurrentSchemaVersionID;
					umint iCurrentSchemaVersion = umint(-1);

					for (umint i = 0; i < SchemaVersions.f_GetLen(); ++i)
					{
						auto const &SchemaVersion = SchemaVersions[i];
						if (co_await pWriteDatabase->f_HasSchemaVersion(SchemaVersion.f_ID()))
						{
							CurrentSchemaVersionID = SchemaVersion.f_ID();
							iCurrentSchemaVersion = i;
						}
					}

					if (CurrentSchemaVersionID == SchemaVersionID)
					{
						TransactionState = ESchemaTransactionState::mc_Commit;

						co_return {};
					}

					if (!CurrentSchemaVersionID.f_IsEmpty())
					{
						for (umint i = iCurrentSchemaVersion + 1; i < SchemaVersions.f_GetLen(); ++i)
						{
							auto const &PreviousSchema = SchemaVersions[i - 1];
							auto const &NextSchema = SchemaVersions[i];
							CSqlSchemaVersionDescription PlannedPreviousSchema = PreviousSchema;

							if (auto pMigration = fg_SqliteFindMigration(SchemaMigrations, PreviousSchema.f_ID(), NextSchema.f_ID()))
							{
								co_await fg_SqliteApplyMigrationOperations(*pWriteDatabase, *pMigration, NextSchema);

								// Reflect the applied operations in the planned previous schema so the additive sync
								// compares against the post-operation state, not the stale previous version. Otherwise a
								// rename or rebuild can make the additive pass detect a phantom new constraint and rebuild
								// the table again, diverging from the previewed plan (which already does this).
								for (auto const &Operation : pMigration->m_Operations)
									fg_SqliteApplyMigrationOperationToPlannedSchema(PlannedPreviousSchema, Operation, NextSchema);
							}

							co_await fg_SqliteSyncAdditiveSchema(*pWriteDatabase, NextSchema, &PlannedPreviousSchema);
							co_await pWriteDatabase->f_MarkSchemaVersionApplied(NextSchema);
						}

						// Migrations run with foreign_keys=OFF (rebuilds rename and recreate tables), so SQLite does
						// not validate foreign keys as statements execute. Re-enabling the pragma afterwards does not
						// retroactively check existing rows, so a migration that introduces a violation would commit
						// and record the schema version. Validate explicitly before committing.
						if (co_await pWriteDatabase->f_HasRows("PRAGMA foreign_key_check", {}))
							co_return DMibErrorDatabaseInstance("Schema migration left foreign key constraint violations");

						TransactionState = ESchemaTransactionState::mc_Commit;

						co_return {};
					}

					// Sync additively rather than emitting plain CREATE TABLEs (mirroring the PostgreSQL backend). When
					// schema_migrations is empty but the database already contains one of the target tables - for
					// example a table created outside version tracking - unconditional creates abort with "table already
					// exists"; the additive sync creates only the missing tables (CREATE TABLE IF NOT EXISTS) and adds
					// missing columns to the ones already present before recording the versions as applied.
					co_await fg_SqliteSyncAdditiveSchema(*pWriteDatabase, Schema, nullptr);

					for (auto const &SchemaVersion : SchemaVersions)
						co_await pWriteDatabase->f_MarkSchemaVersionApplied(SchemaVersion);

					if (co_await pWriteDatabase->f_HasRows("PRAGMA foreign_key_check", {}))
						co_return DMibErrorDatabaseInstance("Schema setup left foreign key constraint violations");

					TransactionState = ESchemaTransactionState::mc_Commit;

					co_return {};
				}
			)
		;

		co_return {};
	}

	NConcurrency::TCFuture<void> CSQLiteDatabaseBackendActor::f_Insert(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedInsert(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteInsertSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) /
				[
					pWriteDatabase = fg_Move(pWriteDatabase)
					, Sql = fg_Move(Sql)
					, Values = fg_Move(Values)
					, pDescriptionKey
				]
				-> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_await pWriteDatabase->f_ExecuteCached({pDescriptionKey, ESqliteCacheBucket::mc_Insert}, Sql, Values);
					else
						co_await pWriteDatabase->f_Execute(Sql, Values);

					co_return {};
				}
			)
		;

		co_return {};
	}

	NConcurrency::TCFuture<umint> CSQLiteDatabaseBackendActor::f_InsertMany(CSqlBulkInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (!_Operation.m_pDescription)
			co_return DMibErrorDatabaseInstance("SQLite bulk insert requires a prepared insert description");

		auto *pDescriptionKey = _Operation.m_pDescription;
		auto Sql = fp_GetPreparedInsert(pDescriptionKey).m_Sql;
		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;
		auto iBatch = co_await fg_Move(_Operation.m_RowBatches).f_GetPipelinedIterator(m_Settings.m_nPipelineLength);

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		umint nTotalAffected = 0;
		for (; iBatch; co_await ++iBatch)
		{
			if ((*iBatch).f_IsEmpty())
				continue;

			nTotalAffected += co_await pWriteDatabase->f_ExecuteBatchAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Insert}, Sql, *iBatch);
		}

		co_return nTotalAffected;
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteDatabaseBackendActor::f_InsertReturning(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedInsert(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteInsertSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		int64 ID = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) /
				[
					pWriteDatabase = fg_Move(pWriteDatabase)
					, Sql = fg_Move(Sql)
					, Values = fg_Move(Values)
					, pDescriptionKey
				]
				-> NConcurrency::TCFuture<int64>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pWriteDatabase->f_ExecuteReturningLastInsertIDCached({pDescriptionKey, ESqliteCacheBucket::mc_Insert}, Sql, Values);
					else
						co_return co_await pWriteDatabase->f_ExecuteReturningLastInsertID(Sql, Values);
				}
			)
		;

		co_return ID;
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteDatabaseBackendActor::f_UpsertReturning(CSqlUpsertOperation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteDatabaseBackendActor::f_UpdateReturning(CSqlUpdateOperation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteDatabaseBackendActor::f_DeleteReturning(CSqlDeleteOperation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<umint> CSQLiteDatabaseBackendActor::f_Upsert(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedUpsert(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteUpsertSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) /
				[
					pWriteDatabase = fg_Move(pWriteDatabase)
					, Sql = fg_Move(Sql)
					, Values = fg_Move(Values)
					, pDescriptionKey
				]
				-> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pWriteDatabase->f_ExecuteAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Upsert}, Sql, Values);
					else
						co_return co_await pWriteDatabase->f_ExecuteAffected(Sql, Values);
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCFuture<umint> CSQLiteDatabaseBackendActor::f_Update(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedUpdate(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteUpdateSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) /
				[
					pWriteDatabase = fg_Move(pWriteDatabase)
					, Sql = fg_Move(Sql)
					, Values = fg_Move(Values)
					, pDescriptionKey
				]
				-> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pWriteDatabase->f_ExecuteAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Update}, Sql, Values);
					else
						co_return co_await pWriteDatabase->f_ExecuteAffected(Sql, Values);
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCFuture<umint> CSQLiteDatabaseBackendActor::f_Delete(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedDelete(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteDeleteSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) /
				[
					pWriteDatabase = fg_Move(pWriteDatabase)
					, Sql = fg_Move(Sql)
					, Values = fg_Move(Values)
					, pDescriptionKey
				]
				-> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pWriteDatabase->f_ExecuteAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Delete}, Sql, Values);
					else
						co_return co_await pWriteDatabase->f_ExecuteAffected(Sql, Values);
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CSQLiteDatabaseBackendActor::f_Select(CSqlSelectOperation _Operation)
	{
		return fs_Select(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CSQLiteDatabaseBackendActor::f_Count(CSqlSelectOperation _Operation)
	{
		return fs_Count(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<bool> CSQLiteDatabaseBackendActor::f_Exists(CSqlSelectOperation _Operation)
	{
		return fs_Exists(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CSQLiteDatabaseBackendActor::f_ExecuteRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_Postgres)
			co_return DMibErrorDatabaseInstance("PostgreSQL-specific raw SQL operation cannot run on the SQLite backend");

		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = fg_Move(_Operation.m_Parameters);

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pWriteDatabase = fg_Move(pWriteDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values)] -> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					umint nAffected = co_await pWriteDatabase->f_ExecuteAffected(Sql, Values);

					// Raw SQL may be transaction-control (BEGIN/SAVEPOINT) that leaves autocommit off; the write
					// connection is reused, so an open transaction would leak into the next operation (its writes
					// committing or rolling back together, or the next BEGIN failing). Roll it back to restore
					// autocommit before the connection is released, mirroring the PostgreSQL raw path which discards a
					// session left in a transaction.
					if (!pWriteDatabase->f_IsAutocommit())
						co_await pWriteDatabase->f_Execute(NStr::CStr("ROLLBACK"));

					co_return nAffected;
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCFuture<CSqlRawResult> CSQLiteDatabaseBackendActor::f_QueryRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_Postgres)
			co_return DMibErrorDatabaseInstance("PostgreSQL-specific raw SQL operation cannot run on the SQLite backend");

		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = fg_Move(_Operation.m_Parameters);

		// Read-pool connections are opened query_only=ON, so a row-returning mutation such as INSERT ... RETURNING
		// cannot run on one. Detect that on a read connection and, if the statement writes, fall through to the
		// write connection instead (matching the transaction and PostgreSQL raw paths). A read connection stays on
		// the read pool. Single-connection databases have no read pool, so they always use the write connection.
		if (!m_bSingleConnection)
		{
			auto ReadCheckout = co_await fp_CheckoutReadDatabase();
			auto pReadDatabase = ReadCheckout.m_pDatabase;
			auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
			NStorage::TCOptional<CSqlRawResult> ReadResult = co_await
				(
					NConcurrency::g_Dispatch(BlockingActorCheckout) / [pReadDatabase, Sql, Values] -> NConcurrency::TCFuture<NStorage::TCOptional<CSqlRawResult>>
					{
						auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

						auto Statement = co_await pReadDatabase->f_PrepareRaw(Sql, Values);
						if (!pReadDatabase->f_RawStatementIsReadOnly(Statement))
							co_return NStorage::TCOptional<CSqlRawResult>();

						auto Result = co_await pReadDatabase->f_SelectRaw(fg_Move(Statement));

						// Transaction-control raw SQL such as BEGIN/SAVEPOINT reports as read-only and runs here, yet
						// leaves the pooled read connection inside a transaction. Roll it back before the checkout is
						// released so it does not leak into the next operation, mirroring f_ExecuteRaw.
						if (!pReadDatabase->f_IsAutocommit())
							co_await pReadDatabase->f_Execute(NStr::CStr("ROLLBACK"));

						co_return NStorage::TCOptional<CSqlRawResult>(fg_Move(Result));
					}
				)
			;

			if (ReadResult)
				co_return fg_Move(*ReadResult);
		}

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pDatabase = Checkout.m_pDatabase;
		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		auto Result = co_await pDatabase->f_SelectRaw(Sql, Values);

		// As in the read path, transaction-control raw SQL would otherwise leave the reused write connection inside a
		// transaction (this is the only path single-connection databases take). Roll it back before releasing it.
		if (!pDatabase->f_IsAutocommit())
			co_await pDatabase->f_Execute(NStr::CStr("ROLLBACK"));

		co_return Result;
	}

	NConcurrency::TCAsyncGenerator<CSqlRawRowBatch> CSQLiteDatabaseBackendActor::fs_QueryRawRowStream
		(
			CSQLiteDatabaseBackendActor *_pBackend
			, CSQLiteDatabaseCheckout _Checkout
			, NStorage::TCSharedPointer<CSqliteStatement> _pStatement
			, CSqlRawRowBatch _FirstRows
			, umint _nRowsPerBatch
		)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto pDatabase = _Checkout.m_pDatabase;
		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		CSqlRawRowBatch Batch;
		for (auto &Row : _FirstRows)
		{
			Batch.f_InsertLast(fg_Move(Row));
			if (Batch.f_GetLen() >= _nRowsPerBatch)
				co_yield fg_Move(Batch);
		}

		for (;;)
		{
			auto pRow = co_await pDatabase->f_SelectRawNext(*_pStatement->m_pStatement);
			if (!pRow)
				break;

			Batch.f_InsertLast(fg_Move(*pRow));
			if (Batch.f_GetLen() >= _nRowsPerBatch)
				co_yield fg_Move(Batch);
		}

		if (Batch.f_GetLen() != 0)
			co_yield fg_Move(Batch);

		// Transaction-control raw SQL streamed here (e.g. BEGIN) leaves the pooled connection in a transaction once the
		// rows are exhausted; roll it back before _Checkout is released so it does not leak into the next operation. A
		// plain read-only SELECT ends its implicit transaction on exhaustion, so autocommit is already restored here.
		if (!pDatabase->f_IsAutocommit())
			co_await pDatabase->f_Execute(NStr::CStr("ROLLBACK"));

		co_return {};
	}

	NConcurrency::TCFuture<CSqlRawStream> CSQLiteDatabaseBackendActor::f_QueryRawStream(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_Postgres)
			co_return DMibErrorDatabaseInstance("PostgreSQL-specific raw SQL operation cannot run on the SQLite backend");

		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = fg_Move(_Operation.m_Parameters);
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : m_Settings.m_nSelectRowsPerBatch;

		// As in f_QueryRaw, a read-pool connection (query_only=ON) cannot stream a row-returning mutation, so decide
		// up front whether the statement writes and stream from the write connection if it does. A read-only stream
		// keeps using the read pool; single-connection databases always use the write connection.
		CSQLiteDatabaseCheckout Checkout;
		bool bUseWriteConnection = m_bSingleConnection;
		if (!m_bSingleConnection)
		{
			auto ReadCheckout = co_await fp_CheckoutReadDatabase();
			auto pReadDatabase = ReadCheckout.m_pDatabase;
			auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
			bool bMutates = co_await
				(
					NConcurrency::g_Dispatch(BlockingActorCheckout) / [pReadDatabase, Sql, Values] -> NConcurrency::TCFuture<bool>
					{
						auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

						auto Statement = co_await pReadDatabase->f_PrepareRaw(Sql, Values);
						co_return !pReadDatabase->f_RawStatementIsReadOnly(Statement);
					}
				)
			;

			if (bMutates)
				bUseWriteConnection = true;
			else
				Checkout = fg_Move(ReadCheckout);
		}

		if (bUseWriteConnection)
			Checkout = co_await fp_CheckoutWriteDatabase();

		auto pDatabase = Checkout.m_pDatabase;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		auto Statement = co_await pDatabase->f_PrepareRaw(Sql, Values);
		auto Columns = fg_SqliteRawColumns(*Statement.m_pStatement);
		CSqlRawRowBatch FirstRows;
		while (fg_SqliteRawColumnsHaveUnknownType(Columns) && FirstRows.f_GetLen() < gc_nSqliteRawTypeInferenceRowLimit)
		{
			auto pRow = co_await pDatabase->f_SelectRawNext(*Statement.m_pStatement);
			if (!pRow)
				break;

			fg_SqliteUpdateRawColumnTypes(Columns, *pRow);
			FirstRows.f_InsertLast(fg_Move(*pRow));
		}

		NStorage::TCSharedPointer<CSqliteStatement> pSharedStatement = fg_Construct();
		*pSharedStatement = fg_Move(Statement);

		CSqlRawStream Stream
			{
				.m_Columns = fg_Move(Columns)
				, .m_Rows = fs_QueryRawRowStream(this, fg_Move(Checkout), fg_Move(pSharedStatement), fg_Move(FirstRows), nRowsPerBatch)
			}
		;

		co_return Stream;
	}

	CPreparedSelectCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = m_PreparedSelectCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedSelectCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteSelectSql(Entry.m_Description);
		Entry.m_CountSql = fg_SqliteSelectCountSql(Entry.m_Description);
		Entry.m_ExistsSql = fg_SqliteSelectExistsSql(Entry.m_Description);

		m_PreparedSelectCache[_pDescription] = fg_Move(Entry);

		return *m_PreparedSelectCache.f_FindEqual(_pDescription);
	}

	CPreparedInsertCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = m_PreparedInsertCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedInsertCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		CSqlInsertOperation Operation;
		Operation.m_TableName = Entry.m_Description.m_TableName;
		Operation.m_pDescription = _pDescription;
		Entry.m_Sql = fg_SqliteInsertSql(Operation);

		m_PreparedInsertCache[_pDescription] = fg_Move(Entry);

		return *m_PreparedInsertCache.f_FindEqual(_pDescription);
	}

	CPreparedUpdateCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = m_PreparedUpdateCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedUpdateCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteUpdateSql(Entry.m_Description);

		m_PreparedUpdateCache[_pDescription] = fg_Move(Entry);

		return *m_PreparedUpdateCache.f_FindEqual(_pDescription);
	}

	CPreparedDeleteCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = m_PreparedDeleteCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedDeleteCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteDeleteSql(Entry.m_Description);

		m_PreparedDeleteCache[_pDescription] = fg_Move(Entry);

		return *m_PreparedDeleteCache.f_FindEqual(_pDescription);
	}

	CPreparedUpsertCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = m_PreparedUpsertCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedUpsertCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteUpsertSql(Entry.m_Description);

		m_PreparedUpsertCache[_pDescription] = fg_Move(Entry);

		return *m_PreparedUpsertCache.f_FindEqual(_pDescription);
	}
}
