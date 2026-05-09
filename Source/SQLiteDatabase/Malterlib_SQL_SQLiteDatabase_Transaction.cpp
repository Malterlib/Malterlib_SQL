// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

namespace NMib::NSQL::NPrivate
{
	namespace
	{
		void fg_SqliteAppendSavepointName(NStr::CStr::CAppender &_Appender, umint _iSavepoint)
		{
			using namespace NStr;

			_Appender += "mib_sp_";
			{
				auto Committed = _Appender.f_Commit();
				Committed.m_String += "{}"_f << _iSavepoint;
			}
		}

		NStr::CStr fg_SqliteSavepointName(umint _iSavepoint)
		{
			NStr::CStr Name;
			{
				NStr::CStr::CAppender Appender(Name);
				fg_SqliteAppendSavepointName(Appender, _iSavepoint);
			}

			return Name;
		}

		NStr::CStr fg_SqliteSavepointSql(ch8 const *_pPrefix, NStr::CStr const &_Name)
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += _pPrefix;
				Appender += _Name;
			}

			return Sql;
		}
	}

	CSQLiteTransactionActor::CSQLiteTransactionActor
		(
			CSqlSchemaVersionDescription _Schema
			, CSQLiteDatabaseBackendSettings _Settings
			, CSqlTransactionSettings _TransactionSettings
			, bool _bReadOnly
			, CSQLiteDatabaseCheckout _Checkout
			, NStorage::TCSharedPointer<CSQLiteDatabaseHandle> _pOwnedDatabase
		)
		: mp_Schema(fg_Move(_Schema))
		, mp_Settings(fg_Move(_Settings))
		, mp_TransactionSettings(_TransactionSettings)
		, mp_bReadOnly(_bReadOnly)
		, mp_Checkout(fg_Move(_Checkout))
		, mp_pOwnedDatabase(fg_Move(_pOwnedDatabase))
		, mp_Sequencer("SQLite transaction")
	{
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CSQLiteTransactionActor::fs_Select(CSQLiteTransactionActor *_pTransaction, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto Sequence = co_await _pTransaction->mp_Sequencer.f_Sequence();
		if (auto pException = _pTransaction->fp_CheckNotFinished())
			co_return pException;

		auto const &StatementEntry = _pTransaction->fp_GetPreparedSelect(_Operation.m_pDescription);
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
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : _pTransaction->mp_Settings.m_nSelectRowsPerBatch;
		auto pDescriptionKey = _Operation.m_pDescription;
		auto pDatabase = _pTransaction->fp_DatabasePointer();

		// The stream keeps its thread between batches, as the transaction keeps its connection until the caller ends it, so the threads
		// are bounded by the transactions the caller holds open. Releasing the thread at each batch would add two thread switches to
		// every batch, and threads shared between transactions would queue each one behind the others' SQLite calls.
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

	NConcurrency::TCFuture<umint> CSQLiteTransactionActor::fs_Count(CSQLiteTransactionActor *_pTransaction, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto Sequence = co_await _pTransaction->mp_Sequencer.f_Sequence();
		if (auto pException = _pTransaction->fp_CheckNotFinished())
			co_return pException;

		auto const &StatementEntry = _pTransaction->fp_GetPreparedSelect(_Operation.m_pDescription);
		if (auto pException = fg_SqliteValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_CountSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto pDescriptionKey = _Operation.m_pDescription;
		auto pDatabase = _pTransaction->fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		co_return umint(co_await pDatabase->f_SelectIntegerCached({pDescriptionKey, ESqliteCacheBucket::mc_SelectCount}, Sql, Parameters));
	}

	NConcurrency::TCFuture<bool> CSQLiteTransactionActor::fs_Exists(CSQLiteTransactionActor *_pTransaction, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto Sequence = co_await _pTransaction->mp_Sequencer.f_Sequence();
		if (auto pException = _pTransaction->fp_CheckNotFinished())
			co_return pException;

		auto const &StatementEntry = _pTransaction->fp_GetPreparedSelect(_Operation.m_pDescription);
		if (auto pException = fg_SqliteValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_ExistsSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto pDescriptionKey = _Operation.m_pDescription;
		auto pDatabase = _pTransaction->fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		co_return (co_await pDatabase->f_SelectIntegerCached({pDescriptionKey, ESqliteCacheBucket::mc_SelectExists}, Sql, Parameters)) != 0;
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::f_OpenBegin()
	{
		if (mp_TransactionSettings.m_Isolation != ESqlTransactionIsolation::mc_Default && mp_TransactionSettings.m_Isolation != ESqlTransactionIsolation::mc_Serializable)
			co_return DMibErrorDatabaseInstance("SQLite transaction isolation level is not supported");

		auto pDatabase = fp_DatabasePointer();
		auto bSharedDatabase = bool(mp_Checkout.m_pDatabase);
		auto bReadOnly = mp_bReadOnly;
		auto Path = mp_Settings.m_Path;

		auto Sequence = co_await mp_Sequencer.f_Sequence();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pDatabase = fg_Move(pDatabase), bSharedDatabase, bReadOnly, Path] -> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (!bSharedDatabase)
					{
						co_await pDatabase->f_Open(Path);

						if (bReadOnly)
							co_await pDatabase->f_Execute("PRAGMA query_only=ON");
					}

					co_await pDatabase->f_Execute("BEGIN TRANSACTION");

					co_return {};
				}
			)
		;

		co_return {};
	}

	bool CSQLiteTransactionActor::fp_EndedBySqlite()
	{
		if (mp_bFinished)
			return mp_bEndedBySqlite;

		// The transaction began when it opened, so a connection back in autocommit mode means SQLite ended it, for example
		// by rolling back on INSERT OR ROLLBACK. Further statements would run outside any transaction.
		auto pDatabase = fp_DatabasePointer();
		if (!pDatabase || !pDatabase->f_IsAutocommit())
			return false;

		mp_bFinished = true;
		mp_bEndedBySqlite = true;
		mp_Checkout = {};
		fp_CloseOwnedDatabase() > NConcurrency::fg_LogError("SQLite database backend", "Failed to close the connection of a transaction SQLite ended");

		return true;
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::fp_Destroy()
	{
		co_await fp_CloseOwnedDatabase();

		co_return {};
	}

	// A finished transaction closes the connection it opened for itself, on a blocking actor since closing does file I/O
	NConcurrency::TCFuture<void> CSQLiteTransactionActor::fp_CloseOwnedDatabase()
	{
		if (!mp_pOwnedDatabase)
			co_return {};

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pDatabase = fg_Move(mp_pOwnedDatabase)]() mutable
				{
					pDatabase.f_Clear();
				}
			)
		;

		co_return {};
	}

	NException::CExceptionPointer CSQLiteTransactionActor::fp_CheckNotFinished()
	{
		if (fp_EndedBySqlite())
			return DMibErrorDatabaseInstance("Cannot use SQLite transaction after SQLite ended it");

		if (mp_bFinished)
			return DMibErrorDatabaseInstance("Cannot use SQLite transaction after transaction finished");

		return {};
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::f_Insert(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in SQLite read transaction");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedInsert(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteInsertSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);
		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pDatabase = fg_Move(pDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_await pDatabase->f_ExecuteCached({pDescriptionKey, ESqliteCacheBucket::mc_Insert}, Sql, Values);
					else
						co_await pDatabase->f_Execute(Sql, Values);

					co_return {};
				}
			)
		;

		co_return {};
	}

	NConcurrency::TCFuture<umint> CSQLiteTransactionActor::f_InsertMany(CSqlBulkInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in SQLite read transaction");

		if (!_Operation.m_pDescription)
			co_return DMibErrorDatabaseInstance("SQLite bulk insert requires a prepared insert description");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto *pDescriptionKey = _Operation.m_pDescription;
		auto Sql = fp_GetPreparedInsert(pDescriptionKey).m_Sql;
		auto pDatabase = fp_DatabasePointer();
		auto iBatch = co_await fg_Move(_Operation.m_RowBatches).f_GetPipelinedIterator(mp_Settings.m_nPipelineLength);

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		umint nTotalAffected = 0;
		for (; iBatch; co_await ++iBatch)
		{
			if ((*iBatch).f_IsEmpty())
				continue;

			nTotalAffected += co_await pDatabase->f_ExecuteBatchAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Insert}, Sql, *iBatch);
		}

		co_return nTotalAffected;
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteTransactionActor::f_InsertReturning(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in SQLite read transaction");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedInsert(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteInsertSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);
		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		int64 ID = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pDatabase = fg_Move(pDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<int64>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pDatabase->f_ExecuteReturningLastInsertIDCached({pDescriptionKey, ESqliteCacheBucket::mc_Insert}, Sql, Values);
					else
						co_return co_await pDatabase->f_ExecuteReturningLastInsertID(Sql, Values);
				}
			)
		;

		co_return ID;
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteTransactionActor::f_UpsertReturning(CSqlUpsertOperation _Operation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteTransactionActor::f_UpdateReturning(CSqlUpdateOperation _Operation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<CSqlValue> CSQLiteTransactionActor::f_DeleteReturning(CSqlDeleteOperation _Operation)
	{
		co_return DMibErrorSqlInstance("SQLite typed mutation RETURNING is not supported", fg_SqlErrorData(ESqlErrorCategory::mc_Generic));
	}

	NConcurrency::TCFuture<umint> CSQLiteTransactionActor::f_Upsert(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot upsert in SQLite read transaction");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedUpsert(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteUpsertSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);
		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pDatabase = fg_Move(pDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pDatabase->f_ExecuteAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Upsert}, Sql, Values);
					else
						co_return co_await pDatabase->f_ExecuteAffected(Sql, Values);
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCFuture<umint> CSQLiteTransactionActor::f_Update(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot update in SQLite read transaction");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedUpdate(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteUpdateSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);
		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pDatabase = fg_Move(pDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pDatabase->f_ExecuteAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Update}, Sql, Values);
					else
						co_return co_await pDatabase->f_ExecuteAffected(Sql, Values);
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCFuture<umint> CSQLiteTransactionActor::f_Delete(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot delete in SQLite read transaction");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		NStr::CStr Sql;
		auto *pDescriptionKey = _Operation.m_pDescription;
		if (pDescriptionKey)
			Sql = fp_GetPreparedDelete(pDescriptionKey).m_Sql;
		else
			Sql = fg_SqliteDeleteSql(_Operation);
		auto Values = fg_Move(_Operation.m_Values);
		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout)
				/ [pDatabase = fg_Move(pDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values), pDescriptionKey] -> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					if (pDescriptionKey)
						co_return co_await pDatabase->f_ExecuteAffectedCached({pDescriptionKey, ESqliteCacheBucket::mc_Delete}, Sql, Values);
					else
						co_return co_await pDatabase->f_ExecuteAffected(Sql, Values);
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CSQLiteTransactionActor::f_Select(CSqlSelectOperation _Operation)
	{
		return fs_Select(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CSQLiteTransactionActor::f_Count(CSqlSelectOperation _Operation)
	{
		return fs_Count(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<bool> CSQLiteTransactionActor::f_Exists(CSqlSelectOperation _Operation)
	{
		return fs_Exists(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CSQLiteTransactionActor::f_ExecuteRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_Postgres)
			co_return DMibErrorDatabaseInstance("PostgreSQL-specific raw SQL operation cannot run on the SQLite backend");

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot execute raw SQL in SQLite read transaction");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = fg_Move(_Operation.m_Parameters);
		auto pDatabase = fp_DatabasePointer();
		pDatabase->m_bNeedsRawSqlReset = true;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		umint nAffected = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pDatabase = fg_Move(pDatabase), Sql = fg_Move(Sql), Values = fg_Move(Values)] -> NConcurrency::TCFuture<umint>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_return co_await pDatabase->f_ExecuteAffected(Sql, Values);
				}
			)
		;

		co_return nAffected;
	}

	NConcurrency::TCFuture<CSqlRawResult> CSQLiteTransactionActor::f_QueryRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_Postgres)
			co_return DMibErrorDatabaseInstance("PostgreSQL-specific raw SQL operation cannot run on the SQLite backend");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = fg_Move(_Operation.m_Parameters);
		auto pDatabase = fp_DatabasePointer();
		pDatabase->m_bNeedsRawSqlReset = true;
		// Copied before the coroutine continues on the blocking actor, where reading the members of the transaction would race with its actor
		bool bReadOnly = mp_bReadOnly;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		auto Statement = co_await pDatabase->f_PrepareRaw(Sql, Values);
		if (bReadOnly && !pDatabase->f_RawStatementIsReadOnly(Statement))
			co_return DMibErrorDatabaseInstance("Cannot execute mutating raw SQL query in SQLite read transaction");

		co_return co_await pDatabase->f_SelectRaw(fg_Move(Statement));
	}

	static auto fg_SqliteTransactionRawRowStream
		(
			NConcurrency::CActorSubscription _Sequence
			, NStorage::TCSharedPointer<CSQLiteDatabaseHandle> _pDatabase
			, NStorage::TCSharedPointer<CSqliteStatement> _pStatement
			, CSqlRawRowBatch _FirstRows
			, bool _bExhausted
			, umint _nRowsPerBatch
		)
		-> NConcurrency::TCAsyncGenerator<CSqlRawRowBatch>
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		// Keeps its thread between batches for the reasons fs_Select does
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
			auto NextRow = co_await _pDatabase->f_SelectRawNext(*_pStatement->m_pStatement);
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

	NConcurrency::TCFuture<CSqlRawStream> CSQLiteTransactionActor::f_QueryRawStream(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_Postgres)
			co_return DMibErrorDatabaseInstance("PostgreSQL-specific raw SQL operation cannot run on the SQLite backend");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = fg_Move(_Operation.m_Parameters);
		auto pDatabase = fp_DatabasePointer();
		pDatabase->m_bNeedsRawSqlReset = true;
		// Copied before the coroutine continues on the blocking actor, where reading the members of the transaction would race with its actor
		bool bReadOnly = mp_bReadOnly;
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : mp_Settings.m_nSelectRowsPerBatch;

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await NConcurrency::fg_ContinueRunningOnActor(BlockingActorCheckout);

		auto Statement = co_await pDatabase->f_PrepareRaw(Sql, Values);
		if (bReadOnly && !pDatabase->f_RawStatementIsReadOnly(Statement))
			co_return DMibErrorDatabaseInstance("Cannot execute mutating raw SQL stream in SQLite read transaction");

		auto Start = co_await fg_SqliteRawStreamStart(*pDatabase, Statement.f_Statement());

		NStorage::TCSharedPointer<CSqliteStatement> pSharedStatement = fg_Construct();
		*pSharedStatement = fg_Move(Statement);

		CSqlRawStream Stream
			{
				.m_Columns = fg_Move(Start.m_Columns)
				, .m_Rows = fg_SqliteTransactionRawRowStream(fg_Move(Sequence), fg_Move(pDatabase), fg_Move(pSharedStatement), fg_Move(Start.m_FirstRows), Start.m_bExhausted, nRowsPerBatch)
			}
		;

		co_return Stream;
	}

	// Runs on the transaction's sequence, which the caller holds
	NConcurrency::TCFuture<void> CSQLiteTransactionActor::fp_ExecuteSavepointSql(NStr::CStr _Sql)
	{
		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pDatabase = fg_Move(pDatabase), Sql = fg_Move(_Sql)] -> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_await pDatabase->f_Execute(Sql);

					co_return {};
				}
			)
		;

		co_return {};
	}

	NConcurrency::TCFuture<NStr::CStr> CSQLiteTransactionActor::f_CreateSavepoint()
	{
		return fp_CreateSavepoint(NPrivate::CSqlSavepointStack::EKind::mc_Manual);
	}

	NConcurrency::TCFuture<NStr::CStr> CSQLiteTransactionActor::f_CreateScopeSavepoint()
	{
		return fp_CreateSavepoint(NPrivate::CSqlSavepointStack::EKind::mc_Scope);
	}

	NConcurrency::TCFuture<NStr::CStr> CSQLiteTransactionActor::fp_CreateSavepoint(NPrivate::CSqlSavepointStack::EKind _Kind)
	{
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedBySqlite() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot create SQLite savepoint after transaction finished");

		auto Name = fg_SqliteSavepointName(++mp_iNextSavepoint);
		co_await fp_ExecuteSavepointSql(fg_SqliteSavepointSql("SAVEPOINT ", Name));
		mp_Savepoints.f_Created(Name, _Kind);

		co_return Name;
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::f_ReleaseSavepoint(NStr::CStr _Name)
	{
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedBySqlite() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot release SQLite savepoint after transaction finished");

		co_await fp_ExecuteSavepointSql(fg_SqliteSavepointSql("RELEASE SAVEPOINT ", _Name));
		mp_Savepoints.f_Released(_Name);

		co_return {};
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::f_RollbackToSavepoint(NStr::CStr _Name)
	{
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedBySqlite() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot roll back SQLite savepoint after transaction finished");

		co_await fp_ExecuteSavepointSql(fg_SqliteSavepointSql("ROLLBACK TO SAVEPOINT ", _Name));
		mp_Savepoints.f_RolledBackTo(_Name);

		co_return {};
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::f_EndScopeSavepoint(NStr::CStr _Name, ESqlTransactionScopeEnd _End)
	{
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedBySqlite() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot end SQLite transaction scope after transaction finished");

		if (auto pException = mp_Savepoints.f_CheckScopeEnd(_Name))
			co_return pException;

		if (_End == ESqlTransactionScopeEnd::mc_RollbackAndRelease)
		{
			co_await fp_ExecuteSavepointSql(fg_SqliteSavepointSql("ROLLBACK TO SAVEPOINT ", _Name));
			mp_Savepoints.f_RolledBackTo(_Name);
		}

		co_await fp_ExecuteSavepointSql(fg_SqliteSavepointSql("RELEASE SAVEPOINT ", _Name));
		mp_Savepoints.f_Released(_Name);

		co_return {};
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::f_CommitTransaction()
	{
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedBySqlite())
			co_return DMibErrorDatabaseInstance("Cannot commit SQLite transaction that SQLite already ended");

		if (mp_bFinished)
		{
			if (!mp_bCommitted)
				co_return DMibErrorDatabaseInstance("Cannot commit SQLite transaction that was rolled back");

			co_return {};
		}

		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pDatabase = fg_Move(pDatabase)] -> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_await pDatabase->f_Execute("COMMIT TRANSACTION");

					co_return {};
				}
			)
		;
		// Not reached when COMMIT fails: SQLite can leave the transaction open for the rollback that destroying the handle runs
		mp_bFinished = true;
		mp_bCommitted = true;
		// The finished transaction gives its pooled connection back, a handle kept in scope would otherwise hold it
		mp_Checkout = {};
		co_await fp_CloseOwnedDatabase();

		co_return {};
	}

	NConcurrency::TCFuture<void> CSQLiteTransactionActor::f_RollbackTransaction()
	{
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedBySqlite() || mp_bFinished)
			co_return {};

		auto pDatabase = fp_DatabasePointer();

		auto BlockingActorCheckout = NConcurrency::fg_BlockingActor();
		auto RollbackResult = co_await
			(
				NConcurrency::g_Dispatch(BlockingActorCheckout) / [pDatabase] -> NConcurrency::TCFuture<void>
				{
					auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

					co_await pDatabase->f_Execute("ROLLBACK TRANSACTION");

					co_return {};
				}
			)
			.f_Wrap()
		;
		if (!RollbackResult)
		{
			// The connection can still be in the transaction, which restoring it before reuse rolls back
			pDatabase->m_bNeedsRawSqlReset = true;
			co_return RollbackResult.f_GetException();
		}

		pDatabase.f_Clear();
		mp_bFinished = true;
		mp_Checkout = {};
		co_await fp_CloseOwnedDatabase();

		co_return {};
	}

	NStorage::TCSharedPointer<CSQLiteDatabaseHandle> CSQLiteTransactionActor::fp_DatabasePointer()
	{
		if (mp_Checkout.m_pDatabase)
			return mp_Checkout.m_pDatabase;

		return mp_pOwnedDatabase;
	}

	CPreparedSelectCacheEntry const &CSQLiteTransactionActor::fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
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

	CPreparedInsertCacheEntry const &CSQLiteTransactionActor::fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
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

	CPreparedUpdateCacheEntry const &CSQLiteTransactionActor::fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = mp_PreparedUpdateCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedUpdateCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteUpdateSql(Entry.m_Description);

		mp_PreparedUpdateCache[_pDescription] = fg_Move(Entry);

		return *mp_PreparedUpdateCache.f_FindEqual(_pDescription);
	}

	CPreparedDeleteCacheEntry const &CSQLiteTransactionActor::fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (auto const *pEntry = mp_PreparedDeleteCache.f_FindEqual(_pDescription))
			return *pEntry;

		CPreparedDeleteCacheEntry Entry;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_SqliteDeleteSql(Entry.m_Description);

		mp_PreparedDeleteCache[_pDescription] = fg_Move(Entry);

		return *mp_PreparedDeleteCache.f_FindEqual(_pDescription);
	}

	CPreparedUpsertCacheEntry const &CSQLiteTransactionActor::fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
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
