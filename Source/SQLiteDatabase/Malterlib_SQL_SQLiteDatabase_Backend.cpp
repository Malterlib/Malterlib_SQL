// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

#include <Mib/Concurrency/AsyncDestroy>

namespace NMib::NSQL::NPrivate
{
	CSQLiteDatabaseBackendActor::CSQLiteDatabaseBackendActor(ICSqlSchemaVersions const *_pSchemaVersions, CSQLiteDatabaseBackendSettings _Settings)
		: mp_pSchemaVersions(_pSchemaVersions)
		, mp_Schema(_pSchemaVersions->f_Describe())
		, mp_SchemaVersions(_pSchemaVersions->f_DescribeVersions())
		, mp_SchemaMigrations(_pSchemaVersions->f_DescribeMigrations())
		, mp_Settings(fg_Move(_Settings))
		, mp_bSingleConnection(fg_SqliteUsesSingleConnection(mp_Settings.m_Path))
	{
		auto &WriteEntry = mp_WritePool.m_Entries.f_InsertLast();
		WriteEntry.m_pDatabase = fg_Construct();
		mp_WritePool.m_FreeEntries.f_InsertLast(WriteEntry);
	}

	// The connection stays marked until restoring succeeds, so a checkout of it retries instead of using it unrestored
	NConcurrency::TCFuture<void> fg_SqliteRestoreAfterRawSql(NStorage::TCSharedPointer<CSQLiteDatabaseHandle> _pDatabase, bool _bReadConnection)
	{
		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [_pDatabase, _bReadConnection]() -> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_await _pDatabase->f_RestoreAfterRawSql(_bReadConnection);
					_pDatabase->m_bNeedsRawSqlReset = false;

					co_return {};
				}
			)
		;

		co_return {};
	}

	CSQLiteDatabasePool &CSQLiteDatabaseBackendActor::fp_GetPool(ESQLiteDatabasePool _Pool)
	{
		return _Pool == ESQLiteDatabasePool::mc_Read ? mp_ReadPool : mp_WritePool;
	}

	CSQLiteDatabaseCheckout CSQLiteDatabaseBackendActor::fp_MakeCheckout(ESQLiteDatabasePool _Pool, CSQLiteDatabaseHandleEntry *_pEntry)
	{
		CSQLiteDatabaseCheckout Checkout;
		Checkout.m_pDatabase = _pEntry->m_pDatabase;
		Checkout.m_ReleaseSubscription = NConcurrency::g_ActorSubscription / [this, _Pool, _pEntry]() -> NConcurrency::TCFuture<void>
			{
				// Also after a failed, abandoned or transaction-scoped raw operation, which can have changed a pragma while
				// being prepared
				if (_pEntry->m_pDatabase->m_bNeedsRawSqlReset)
				{
					co_await fg_SqliteRestoreAfterRawSql(_pEntry->m_pDatabase, _Pool == ESQLiteDatabasePool::mc_Read).f_Wrap()
						> NConcurrency::fg_LogError("SQLite database backend", "Failed to restore connection after raw SQL")
					;
				}

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
			if (auto *pEntry = mp_WritePool.m_FreeEntries.f_Pop())
			{
				auto Checkout = fp_MakeCheckout(ESQLiteDatabasePool::mc_Write, pEntry);
				if (Checkout.m_pDatabase->m_bNeedsRawSqlReset)
					co_await fg_SqliteRestoreAfterRawSql(Checkout.m_pDatabase, false);

				co_return fg_Move(Checkout);
			}

			NConcurrency::TCPromiseFuturePair<void> Waiter;
			mp_WritePool.m_Waiters.f_InsertLast(CSQLiteDatabasePoolWaiter{.m_Promise = fg_Move(Waiter.m_Promise)});
			co_await fg_Move(Waiter.m_Future);
		}
	}

	NConcurrency::TCFuture<CSQLiteDatabaseCheckout> CSQLiteDatabaseBackendActor::fp_CheckoutReadDatabase()
	{
		if (mp_bSingleConnection)
			co_return co_await fp_CheckoutWriteDatabase();

		for (;;)
		{
			if (auto *pEntry = mp_ReadPool.m_FreeEntries.f_Pop())
			{
				auto Checkout = fp_MakeCheckout(ESQLiteDatabasePool::mc_Read, pEntry);
				if (Checkout.m_pDatabase->m_bNeedsRawSqlReset)
					co_await fg_SqliteRestoreAfterRawSql(Checkout.m_pDatabase, true);

				co_return fg_Move(Checkout);
			}

			NConcurrency::TCPromiseFuturePair<void> Waiter;
			mp_ReadPool.m_Waiters.f_InsertLast(CSQLiteDatabasePoolWaiter{.m_Promise = fg_Move(Waiter.m_Promise)});
			co_await fg_Move(Waiter.m_Future);
		}
	}

	// Resolves when either pool frees an entry, which another checkout can still take first. The wait is in both lists, and the
	// pool that wakes it removes it from the other one.
	NConcurrency::TCFuture<void> fg_SqliteWaitForEitherPool(CSQLiteDatabasePool &_ReadPool, CSQLiteDatabasePool &_WritePool)
	{
		NConcurrency::TCPromiseFuturePair<void> Waiter;
		auto &ReadWaiter = _ReadPool.m_Waiters.f_InsertLast(CSQLiteDatabasePoolWaiter{.m_Promise = Waiter.m_Promise});
		auto &WriteWaiter = _WritePool.m_Waiters.f_InsertLast(CSQLiteDatabasePoolWaiter{.m_Promise = fg_Move(Waiter.m_Promise), .m_pOtherPoolWaiter = &ReadWaiter});
		ReadWaiter.m_pOtherPoolWaiter = &WriteWaiter;

		return fg_Move(Waiter.m_Future);
	}

	void fg_SqliteWakePoolWaiter(CSQLiteDatabasePool &_Pool, CSQLiteDatabasePool &_OtherPool)
	{
		if (_Pool.m_Waiters.f_IsEmpty())
			return;

		auto &Waiter = _Pool.m_Waiters.f_GetFirst();
		if (Waiter.m_pOtherPoolWaiter)
			_OtherPool.m_Waiters.f_Remove(*Waiter.m_pOtherPoolWaiter);

		auto Promise = fg_Move(Waiter.m_Promise);
		_Pool.m_Waiters.f_Remove(Waiter);

		Promise.f_SetResult();
	}

	NConcurrency::TCFuture<void> CSQLiteDatabaseBackendActor::fp_WaitForEitherPool()
	{
		return fg_SqliteWaitForEitherPool(mp_ReadPool, mp_WritePool);
	}

	// Closing the last connection to a WAL database checkpoints it, which must not block the actor's thread. Connections still
	// checked out close when their last user releases them
	NConcurrency::TCFuture<void> CSQLiteDatabaseBackendActor::fp_Destroy()
	{
		auto pDestroyedException = DMibErrorDatabaseInstance("SQLite database backend destroyed while waiting for connection").f_ExceptionPointer();
		NContainer::TCVector<NStorage::TCSharedPointer<CSQLiteDatabaseHandle>> Databases;
		for (CSQLiteDatabasePool *pPool : {&mp_WritePool, &mp_ReadPool})
		{
			// A wait for either pool is in both lists
			for (auto &Waiter : pPool->m_Waiters)
			{
				if (!Waiter.m_Promise.f_IsSet())
					Waiter.m_Promise.f_SetException(pDestroyedException);
			}
			pPool->m_Waiters.f_Clear();

			// The entries stay, since the release subscriptions of checked out connections point at them
			while (auto *pEntry = pPool->m_FreeEntries.f_Pop())
				Databases.f_InsertLast(fg_Move(pEntry->m_pDatabase));
		}

		if (Databases.f_IsEmpty())
			co_return {};

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [Databases = fg_Move(Databases)]() mutable
				{
					Databases.f_Clear();
				}
			)
		;

		co_return {};
	}

	void CSQLiteDatabaseBackendActor::fp_ReleaseEntry(ESQLiteDatabasePool _Pool, CSQLiteDatabaseHandleEntry *_pEntry)
	{
		auto &Pool = fp_GetPool(_Pool);
		Pool.m_FreeEntries.f_InsertFirst(_pEntry);

		fg_SqliteWakePoolWaiter(Pool, fp_GetPool(_Pool == ESQLiteDatabasePool::mc_Read ? ESQLiteDatabasePool::mc_Write : ESQLiteDatabasePool::mc_Read));
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

		// The top-level description carries the left operand's modifiers, and m_SetOperations only the right operands
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
			Parameters.f_InsertLast(_Operation.m_nResultRowLimit != 0 ? NPrivate::fg_SqlRowCountParameter(_Operation.m_nResultRowLimit) : int64(-1));
		if (pStatementDescription->m_LimitOffset.m_bHasOffset)
			Parameters.f_InsertLast(NPrivate::fg_SqlRowCountParameter(_Operation.m_nResultRowOffset));
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : _pBackend->mp_Settings.m_nSelectRowsPerBatch;
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
		// Read checkouts would wait forever on an empty read pool
		if (!mp_bSingleConnection && mp_Settings.m_nReadConnections == 0)
			co_return DMibErrorDatabaseInstance("SQLite read connection count must be at least one");

		if (mp_Settings.m_nSelectRowsPerBatch == 0)
			co_return DMibErrorDatabaseInstance("SQLite select rows per batch must be at least one");

		if (mp_Settings.m_nPipelineLength == 0)
			co_return DMibErrorDatabaseInstance("SQLite pipeline length must be at least one");

		auto Path = mp_Settings.m_Path;
		auto bSingleConnection = mp_bSingleConnection;
		umint nReadConnections = mp_Settings.m_nReadConnections;

		auto Checkout = co_await fp_CheckoutWriteDatabase();
		auto pWriteDatabase = Checkout.m_pDatabase;

		// Opening again would replace the write connection, which for :memory: is the database itself, and add another set of read connections
		if (pWriteDatabase->f_IsOpen())
			co_return {};

		using CDatabases = NContainer::TCVector<NStorage::TCSharedPointer<CSQLiteDatabaseHandle>>;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		NConcurrency::TCAsyncResult<CDatabases> ReadDatabases = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pWriteDatabase, Path, bSingleConnection, nReadConnections] -> NConcurrency::TCFuture<CDatabases>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					CDatabases OpenedReadDatabases;
					// Opening again after a failure, for example when WAL could not be set up because another connection was reading, opens every
					// connection again, so the ones this call opened are closed
					auto CloseOnFailure = g_OnScopeExit / [&]
						{
							OpenedReadDatabases.f_Clear();
							pWriteDatabase->f_Close();
						}
					;

					co_await pWriteDatabase->f_Open(Path);

					if (!bSingleConnection)
					{
						co_await pWriteDatabase->f_Execute("PRAGMA journal_mode=WAL");
						co_await pWriteDatabase->f_Execute("PRAGMA synchronous=NORMAL");
						pWriteDatabase->m_bSynchronousNormal = true;
						co_await pWriteDatabase->f_RecordJournalMode();

						// SQLite keeps the old mode where WAL is unsupported, as in a VFS without shared memory. Readers in a rollback
						// journal hold locks that make the write connection's commits fail with SQLITE_BUSY.
						if (pWriteDatabase->m_JournalMode != "wal")
						{
							co_return DMibErrorDatabaseInstance
								(
									NStr::CStr::CFormat("SQLite database could not use WAL journaling, which its read connections need (journal mode is '{}')")
									<< pWriteDatabase->m_JournalMode
								)
							;
						}

						for (umint iReadConnection = 0; iReadConnection < nReadConnections; ++iReadConnection)
						{
							NStorage::TCSharedPointer<CSQLiteDatabaseHandle> pDatabase = fg_Construct();
							co_await pDatabase->f_Open(Path);
							co_await pDatabase->f_Execute("PRAGMA query_only=ON");
							OpenedReadDatabases.f_InsertLast(fg_Move(pDatabase));
						}
					}

					CloseOnFailure.f_Clear();

					co_return fg_Move(OpenedReadDatabases);
				}
			)
			.f_Wrap()
		;

		// Reads issued while the connections were opening wait for them, so they fail with the open. A wait for either pool is
		// also in the write pool's list, and releasing the write connection wakes it.
		if (!ReadDatabases)
		{
			auto pException = ReadDatabases.f_GetException();
			for (auto iWaiter = mp_ReadPool.m_Waiters.f_GetIterator(); iWaiter;)
			{
				auto &Waiter = *iWaiter;
				++iWaiter;

				if (Waiter.m_pOtherPoolWaiter)
					continue;

				auto Promise = fg_Move(Waiter.m_Promise);
				mp_ReadPool.m_Waiters.f_Remove(Waiter);
				Promise.f_SetException(pException);
			}

			co_return fg_Move(pException);
		}

		for (auto &pDatabase : *ReadDatabases)
		{
			auto &ReadEntry = mp_ReadPool.m_Entries.f_InsertLast();
			ReadEntry.m_pDatabase = fg_Move(pDatabase);
			fp_ReleaseEntry(ESQLiteDatabasePool::mc_Read, &ReadEntry);
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CSQLiteDatabaseBackendActor::f_ApplySchema()
	{
		auto Checkout = co_await fp_CheckoutWriteDatabase();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pWriteDatabase = Checkout.m_pDatabase, Schema = mp_Schema, SchemaVersionID = mp_Schema.f_ID(), SchemaVersions = mp_SchemaVersions, SchemaMigrations = mp_SchemaMigrations]
				-> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (auto pException = fg_SqliteValidateSchemaVersions(SchemaVersions))
						co_return fg_Move(pException);

					co_await pWriteDatabase->f_Execute(fg_SqliteCreateSchemaVersionTableSql());

					enum class ESchemaTransactionState
					{
						mc_NotStarted
						, mc_Rollback
						, mc_Commit
					};

					ESchemaTransactionState TransactionState = ESchemaTransactionState::mc_NotStarted;

					// Rebuilds drop and rename tables, which enforcement would turn into cascades, and the pragma cannot change
					// inside the transaction, so no migration statement runs foreign key actions: a backfill that changes a
					// referenced key fails the foreign_key_check below instead of cascading, and its migration also updates
					// the referencing columns
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
									// A failed COMMIT can leave the transaction open; roll it back before the pooled connection is reused
									if (!FinishTransactionResult)
									{
										NConcurrency::TCAsyncResult<void> RollbackResult = pDatabase->f_Execute("ROLLBACK TRANSACTION");
										if (!RollbackResult)
										{
											// The connection can still be in the transaction, which restoring it before reuse rolls back
											pDatabase->m_bNeedsRawSqlReset = true;
											NConcurrency::fg_LogError("SQLite database backend", "Failed to roll back schema transaction")(fg_Move(RollbackResult));
										}
									}
								}
								else if (State == ESchemaTransactionState::mc_Rollback)
								{
									FinishTransactionResult = pDatabase->f_Execute("ROLLBACK TRANSACTION");
									if (!FinishTransactionResult)
										pDatabase->m_bNeedsRawSqlReset = true;
								}
								else
									FinishTransactionResult.f_SetResult();

								co_await pDatabase->f_Execute("PRAGMA foreign_keys=ON");
								// A rollback does not restore legacy_alter_table, which a failed rebuild can leave ON
								co_await pDatabase->f_Execute("PRAGMA legacy_alter_table=OFF");

								if (!FinishTransactionResult)
									co_return fg_Move(FinishTransactionResult).f_GetException();

								co_return {};
							}
						)
					;

					// A deferred transaction would read the versions first, and a concurrent applier that commits before this
					// one upgrades to a write lock makes that upgrade fail with SQLITE_BUSY instead of waiting
					co_await pWriteDatabase->f_Execute("BEGIN IMMEDIATE TRANSACTION");
					TransactionState = ESchemaTransactionState::mc_Rollback;

					co_await pWriteDatabase->f_ValidateAppliedSchemaVersions(SchemaVersions);

					NStr::CStr CurrentSchemaVersionID;
					umint iCurrentSchemaVersion = umint(-1);
					NStr::CStr const *pFirstMissingVersionID = nullptr;

					for (umint iSchemaVersion = 0; iSchemaVersion < SchemaVersions.f_GetLen(); ++iSchemaVersion)
					{
						auto const &SchemaVersion = SchemaVersions[iSchemaVersion];
						if (!co_await pWriteDatabase->f_HasSchemaVersion(SchemaVersion.f_ID()))
						{
							if (!pFirstMissingVersionID)
								pFirstMissingVersionID = &SchemaVersion.f_ID();

							continue;
						}

						// Upgrading resumes after the latest applied version, so a version the history has before it would never run
						if (pFirstMissingVersionID)
						{
							co_return DMibErrorDatabaseInstance
								(
									NStr::CStr::CFormat("SQLite schema version '{}' is not applied, but the later version '{}' is") << *pFirstMissingVersionID << SchemaVersion.f_ID()
								)
							;
						}

						CurrentSchemaVersionID = SchemaVersion.f_ID();
						iCurrentSchemaVersion = iSchemaVersion;
					}

					if (CurrentSchemaVersionID == SchemaVersionID)
					{
						TransactionState = ESchemaTransactionState::mc_Commit;

						co_return {};
					}

					if (!CurrentSchemaVersionID.f_IsEmpty())
					{
						NContainer::TCVector<NStr::CStr> DroppedTables;
						for (umint iSchemaVersion = iCurrentSchemaVersion + 1; iSchemaVersion < SchemaVersions.f_GetLen(); ++iSchemaVersion)
						{
							auto const &PreviousSchema = SchemaVersions[iSchemaVersion - 1];
							auto const &NextSchema = SchemaVersions[iSchemaVersion];
							CSqlitePlannedSchemaStorage PlannedSchemaStorage;
							CSqlSchemaVersionDescription PlannedPreviousSchema = PreviousSchema;

							umint iMigration = fg_SqliteFindMigration(SchemaMigrations, PreviousSchema.f_ID(), NextSchema.f_ID());
							// The additive sync compares against the post-operation schema, so it does not redo what a rename or rebuild changed
							if (iMigration != umint(-1))
							{
								co_await fg_SqliteApplyMigrationOperations(*pWriteDatabase, SchemaMigrations[iMigration], NextSchema, PlannedPreviousSchema, PlannedSchemaStorage);

								for (auto const &Operation : SchemaMigrations[iMigration].m_Operations)
								{
									if (Operation.m_Type == ESqlSchemaMigrationOperationType::mc_DropTable)
										DroppedTables.f_InsertLast(*Operation.m_pTableName);
								}
							}

							co_await fg_SqliteSyncAdditiveSchema(*pWriteDatabase, NextSchema, &PlannedPreviousSchema);
							co_await pWriteDatabase->f_MarkSchemaVersionApplied(NextSchema);
						}

						// Migrations run with foreign_keys=OFF (rebuilds rename and recreate tables), so SQLite does
						// not validate foreign keys as statements execute. Re-enabling the pragma afterwards does not
						// retroactively check existing rows, so a migration that introduces a violation would commit
						// and record the schema version. Validate explicitly before committing.
						co_await fg_SqliteCheckNoReferencesToDroppedTables(*pWriteDatabase, DroppedTables);
						if (co_await pWriteDatabase->f_HasRows("PRAGMA foreign_key_check", {}))
							co_return DMibErrorDatabaseInstance("Schema migration left foreign key constraint violations");

						TransactionState = ESchemaTransactionState::mc_Commit;

						co_return {};
					}

					// A database without recorded versions can already contain the tables and their rows; reconcile them with the schema
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
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pWriteDatabase = fg_Move(pWriteDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<void>
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
		auto iBatch = co_await fg_Move(_Operation.m_RowBatches).f_GetPipelinedIterator(mp_Settings.m_nPipelineLength);

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		// Every batch commits together, as on PostgreSQL, so a failing row or producer leaves none of them behind
		co_await pWriteDatabase->f_Execute("BEGIN IMMEDIATE TRANSACTION");
		bool bCommitted = false;
		auto RollbackOnExit = co_await NConcurrency::fg_AsyncDestroy
			(
				[pWriteDatabase, &bCommitted]() -> NConcurrency::TCFuture<void>
				{
					if (!bCommitted)
					{
						NConcurrency::TCAsyncResult<void> RollbackResult = pWriteDatabase->f_Execute("ROLLBACK TRANSACTION");
						if (!RollbackResult)
							NConcurrency::fg_LogError("SQLite database backend", "Failed to roll back bulk insert")(fg_Move(RollbackResult));
					}

					co_return {};
				}
			)
		;

		umint nTotalAffected = 0;
		for (; iBatch; co_await ++iBatch)
		{
			if ((*iBatch).f_IsEmpty())
				continue;

			nTotalAffected += co_await pWriteDatabase->f_ExecuteBatchAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Insert}, Sql, *iBatch);
		}

		co_await pWriteDatabase->f_Execute("COMMIT TRANSACTION");
		bCommitted = true;

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
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pWriteDatabase = fg_Move(pWriteDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<int64>
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

	NConcurrency::TCFuture<CSqlValue> CSQLiteDatabaseBackendActor::f_UpsertReturning(CSqlUpsertOperation _Operation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteDatabaseBackendActor::f_UpdateReturning(CSqlUpdateOperation _Operation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteDatabaseBackendActor::f_DeleteReturning(CSqlDeleteOperation _Operation)
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
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pWriteDatabase = fg_Move(pWriteDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<umint>
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
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pWriteDatabase = fg_Move(pWriteDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<umint>
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
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pWriteDatabase = fg_Move(pWriteDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<umint>
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

		pWriteDatabase->m_bNeedsRawSqlReset = true;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pWriteDatabase = fg_Move(pWriteDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values)] -> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_return co_await pWriteDatabase->f_ExecuteAffected(Sql, Values);
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

		// Read-pool connections are query_only, even for mutations that return rows.
		// Waiting for a read connection only to learn that the statement writes would deadlock a caller that holds
		// the last one in an open stream, so a busy read pool leaves the statement to the write connection. Waiting
		// only for the write connection would deadlock a caller that holds it in a transaction, so with both busy the
		// statement takes the first that frees.
		while (!mp_bSingleConnection && mp_ReadPool.m_FreeEntries.f_IsEmpty() && mp_WritePool.m_FreeEntries.f_IsEmpty())
			co_await fp_WaitForEitherPool();

		if (!mp_bSingleConnection && !mp_ReadPool.m_FreeEntries.f_IsEmpty())
		{
			auto ReadCheckout = co_await fp_CheckoutReadDatabase();
			auto pReadDatabase = ReadCheckout.m_pDatabase;
			pReadDatabase->m_bNeedsRawSqlReset = true;
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

		pDatabase->m_bNeedsRawSqlReset = true;
		auto Result = co_await pDatabase->f_SelectRaw(Sql, Values);

		co_return Result;
	}

	auto CSQLiteDatabaseBackendActor::fs_QueryRawRowStream
		(
			CSQLiteDatabaseCheckout _Checkout
			, NStorage::TCSharedPointer<CSqliteStatement> _pStatement
			, CSqlRawRowBatch _FirstRows
			, bool _bExhausted
			, umint _nRowsPerBatch
		)
		-> NConcurrency::TCAsyncGenerator<CSqlRawRowBatch>
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

		// Stepping a statement past SQLITE_DONE resets and executes it again
		while (!_bExhausted)
		{
			auto NextRow = co_await pDatabase->f_SelectRawNext(*_pStatement->m_pStatement);
			if (!NextRow)
				break;

			Batch.f_InsertLast(fg_Move(*NextRow));
			if (Batch.f_GetLen() >= _nRowsPerBatch)
				co_yield fg_Move(Batch);
		}

		if (Batch.f_GetLen() != 0)
			co_yield fg_Move(Batch);

		co_return {};
	}

	NConcurrency::TCFuture<CSqlRawStream> CSQLiteDatabaseBackendActor::f_QueryRawStream(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_Postgres)
			co_return DMibErrorDatabaseInstance("PostgreSQL-specific raw SQL operation cannot run on the SQLite backend");

		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = fg_Move(_Operation.m_Parameters);
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : mp_Settings.m_nSelectRowsPerBatch;

		// A query_only read-pool connection cannot stream rows from a mutation such as INSERT ... RETURNING.
		CSQLiteDatabaseCheckout Checkout;
		bool bUseWriteConnection = true;
		// Waiting for a read connection only to learn that the statement writes would deadlock a caller that holds
		// the last one in an open stream, so a busy read pool leaves the statement to the write connection. Waiting
		// only for the write connection would deadlock a caller that holds it in a transaction, so with both busy the
		// statement takes the first that frees.
		while (!mp_bSingleConnection && mp_ReadPool.m_FreeEntries.f_IsEmpty() && mp_WritePool.m_FreeEntries.f_IsEmpty())
			co_await fp_WaitForEitherPool();

		if (!mp_bSingleConnection && !mp_ReadPool.m_FreeEntries.f_IsEmpty())
		{
			auto ReadCheckout = co_await fp_CheckoutReadDatabase();
			auto pReadDatabase = ReadCheckout.m_pDatabase;
			pReadDatabase->m_bNeedsRawSqlReset = true;
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

			if (!bMutates)
			{
				Checkout = fg_Move(ReadCheckout);
				bUseWriteConnection = false;
			}
		}

		if (bUseWriteConnection)
			Checkout = co_await fp_CheckoutWriteDatabase();

		auto pDatabase = Checkout.m_pDatabase;
		pDatabase->m_bNeedsRawSqlReset = true;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		auto Statement = co_await pDatabase->f_PrepareRaw(Sql, Values);
		auto Start = co_await fg_SqliteRawStreamStart(*pDatabase, *Statement.m_pStatement);

		NStorage::TCSharedPointer<CSqliteStatement> pSharedStatement = fg_Construct();
		*pSharedStatement = fg_Move(Statement);

		CSqlRawStream Stream
			{
				.m_Columns = fg_Move(Start.m_Columns)
				, .m_Rows = fs_QueryRawRowStream(fg_Move(Checkout), fg_Move(pSharedStatement), fg_Move(Start.m_FirstRows), Start.m_bExhausted, nRowsPerBatch)
			}
		;

		co_return Stream;
	}

	CPreparedSelectCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = mp_PreparedSelectCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedSelectCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteSelectSql(Entry.m_Description);
		Entry.m_CountSql = fg_SqliteSelectCountSql(Entry.m_Description);
		Entry.m_ExistsSql = fg_SqliteSelectExistsSql(Entry.m_Description);

		mp_PreparedSelectCache[_pDescription] = fg_Move(Entry);

		return *mp_PreparedSelectCache.f_FindEqual(_pDescription);
	}

	CPreparedInsertCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = mp_PreparedInsertCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedInsertCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		CSqlInsertOperation Operation;
		Operation.m_TableName = Entry.m_Description.m_TableName;
		Operation.m_pDescription = _pDescription;
		Entry.m_Sql = fg_SqliteInsertSql(Operation);

		mp_PreparedInsertCache[_pDescription] = fg_Move(Entry);

		return *mp_PreparedInsertCache.f_FindEqual(_pDescription);
	}

	CPreparedUpdateCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = mp_PreparedUpdateCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedUpdateCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteUpdateSql(Entry.m_Description);

		mp_PreparedUpdateCache[_pDescription] = fg_Move(Entry);

		return *mp_PreparedUpdateCache.f_FindEqual(_pDescription);
	}

	CPreparedDeleteCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = mp_PreparedDeleteCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedDeleteCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteDeleteSql(Entry.m_Description);

		mp_PreparedDeleteCache[_pDescription] = fg_Move(Entry);

		return *mp_PreparedDeleteCache.f_FindEqual(_pDescription);
	}

	CPreparedUpsertCacheEntry const &CSQLiteDatabaseBackendActor::fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = mp_PreparedUpsertCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedUpsertCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteUpsertSql(Entry.m_Description);

		mp_PreparedUpsertCache[_pDescription] = fg_Move(Entry);

		return *mp_PreparedUpsertCache.f_FindEqual(_pDescription);
	}
}
