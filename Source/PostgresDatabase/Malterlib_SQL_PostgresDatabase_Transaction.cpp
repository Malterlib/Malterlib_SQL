// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_PostgresDatabase_Internal.h"

namespace NMib::NSQL::NPrivate
{
	namespace
	{
		void fg_PostgresAppendSavepointName(NStr::CStr::CAppender &_Appender, umint _iSavepoint)
		{
			using namespace NStr;

			_Appender += "mib_sp_";
			{
				auto Committed = _Appender.f_Commit();
				Committed.m_String += "{}"_f << _iSavepoint;
			}
		}

		NStr::CStr fg_PostgresSavepointName(umint _iSavepoint)
		{
			NStr::CStr Name;
			{
				NStr::CStr::CAppender Appender(Name);
				fg_PostgresAppendSavepointName(Appender, _iSavepoint);
			}

			return Name;
		}

		NStr::CStr fg_PostgresSavepointSql(ch8 const *_pPrefix, NStr::CStr const &_Name)
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

	CPostgresTransactionActor::CPostgresTransactionActor
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
		: mp_bReadOnly(_bReadOnly)
		, mp_TransactionSettings(_TransactionSettings)
		, mp_ConnectionCheckout(fg_Move(_ConnectionCheckout))
		, mp_nSelectRowsPerBatch(_nSelectRowsPerBatch)
		, mp_nPipelineLength(_nPipelineLength)
		, mp_Backend(fg_Move(_Backend))
		, mp_Sequencer("PostgreSQL transaction")
		, mp_pPreparedSelectCache(fg_Move(_pPreparedSelectCache))
		, mp_pPreparedInsertCache(fg_Move(_pPreparedInsertCache))
		, mp_pPreparedUpdateCache(fg_Move(_pPreparedUpdateCache))
		, mp_pPreparedDeleteCache(fg_Move(_pPreparedDeleteCache))
		, mp_pPreparedUpsertCache(fg_Move(_pPreparedUpsertCache))
	{
		mp_pServerState->m_bReadOnly = mp_bReadOnly;
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_OpenBegin()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();
		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_BeginTransaction, mp_bReadOnly, mp_TransactionSettings);

		co_return {};
	}

	// A local setting reverts to the session value when its transaction ends, so the transaction COMMIT AND CHAIN starts lacks it
	// unless the session sets it. It is set before any savepoint, and again after raw SQL that runs RESET, so rolling back to one
	// keeps it. SET takes no snapshot, so SET TRANSACTION SNAPSHOT can still follow.
	static auto fg_PostgresMarkTransaction(NConcurrency::TCActor<CPostgresClientActor> _Client, NStorage::TCSharedPointer<CPostgresTransactionServerState> _pServerState)
		-> NConcurrency::TCFuture<void>
	{
		if (_pServerState->m_bTransactionMarkerSet)
			co_return {};

		auto MarkResult = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SET LOCAL malterlib.transaction_marker = 'on'")).f_Wrap();
		if (MarkResult)
			_pServerState->m_bTransactionMarkerSet = true;
		else
			DMibLogWithCategory(PostgreSQL, Info, "Failed to mark the transaction: {}", MarkResult.f_GetExceptionStr());

		co_return {};
	}

	// Marked before running, since the SQL can change the session even when the operation then fails or is abandoned
	static auto fg_PostgresBeforeTransactionRawSql(NConcurrency::TCActor<CPostgresClientActor> _Client, NStorage::TCSharedPointer<CPostgresTransactionServerState> _pServerState)
		-> NConcurrency::TCFuture<CPostgresSessionCounters>
	{
		_pServerState->m_bRawSqlAttempted = true;
		co_await fg_PostgresMarkTransaction(_Client, _pServerState);

		co_return co_await _Client(&CPostgresClientActor::f_GetSessionCounters);
	}

	// Raw SQL can end the transaction on the server, after which further statements would run in autocommit mode
	static auto fg_PostgresSyncTransactionServerState
		(
			NConcurrency::TCActor<CPostgresClientActor> _Client
			, NStorage::TCSharedPointer<CPostgresTransactionServerState> _pServerState
			, CPostgresSessionCounters _CountersBefore
		)
		-> NConcurrency::TCFuture<void>
	{
		_pServerState->m_bRawSqlAttempted = true;

		// Calls that check the transaction before they take its sequence release the connection once they see it ended, so the
		// state shows it only when this function no longer uses the client
		bool bEnded = false;
		auto PublishEnded = g_OnScopeExit / [&]
			{
				if (bEnded)
					_pServerState->m_bEnded = true;
			}
		;

		if (!co_await _Client(&CPostgresClientActor::f_IsConnected))
		{
			_pServerState->m_bDisconnected = true;
			bEnded = true;
		}
		else if (!co_await _Client(&CPostgresClientActor::f_IsInTransaction))
			bEnded = true;

		auto Counters = co_await _Client(&CPostgresClientActor::f_GetSessionCounters);

		// After COMMIT AND CHAIN or ROLLBACK AND CHAIN the session is in a new transaction that this one must not continue in.
		// ROLLBACK TO SAVEPOINT has the same command tag but keeps the marker, and a failed transaction is still this one.
		bool bChained = false;
		if (Counters.m_nTransactionEndTags != _CountersBefore.m_nTransactionEndTags && !bEnded && !co_await _Client(&CPostgresClientActor::f_IsTransactionFailed))
		{
			bChained = true;
			if (_pServerState->m_bTransactionMarkerSet)
			{
				auto Marker = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SELECT current_setting('malterlib.transaction_marker', true)")).f_Wrap();
				if (!Marker)
					DMibLogWithCategory(PostgreSQL, Info, "Failed to read the transaction marker after raw SQL: {}", Marker.f_GetExceptionStr());
				else if (Marker->m_Rows.f_GetLen() == 1 && Marker->m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() == "on")
					bChained = false;
			}
		}

		if (bChained)
		{
			co_await _Client(&CPostgresClientActor::f_RollbackTransaction).f_Wrap()
				> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to roll back the transaction raw SQL chained")
			;
			bEnded = true;
		}

		// RESET ALL clears the marker, which a savepoint created later would then lack
		if (Counters.m_nResetTags != _CountersBefore.m_nResetTags && !bEnded && !co_await _Client(&CPostgresClientActor::f_IsTransactionFailed))
		{
			_pServerState->m_bTransactionMarkerSet = false;
			co_await fg_PostgresMarkTransaction(_Client, _pServerState);
		}

		if (Counters.m_nStatementDrops != _CountersBefore.m_nStatementDrops)
		{
			_pServerState->m_bStatementsDropped = true;
			_pServerState->m_bPreparedCachesStale = true;
		}

		// SET TRANSACTION READ WRITE would let the following raw statements write in a read transaction
		// A failed transaction can't write either, and probing it would replace the original error with 25P02. SHOW takes no snapshot, so
		// SET TRANSACTION SNAPSHOT can still follow.
		if (_pServerState->m_bReadOnly && !bEnded && !co_await _Client(&CPostgresClientActor::f_IsTransactionFailed))
		{
			auto Result = co_await _Client(&CPostgresClientActor::f_Execute, NStr::CStr("SHOW transaction_read_only"));
			if (Result.m_Rows.f_GetLen() != 1 || Result.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>() != "on")
			{
				co_await _Client(&CPostgresClientActor::f_RollbackTransaction).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to roll back a read transaction raw SQL made writable")
				;
				bEnded = true;

				co_return DMibErrorDatabaseInstance("Raw SQL made the PostgreSQL read transaction writable, so it was rolled back");
			}
		}

		co_return {};
	}

	static bool fg_PostgresIsStalePreparedStatementError(NException::CExceptionPointer const &_pException)
	{
		auto ErrorData = fg_TryGetSqlErrorData(_pException);

		// 0A000 is how a cached plan reports a changed result type, 26000 a statement name the session no longer has
		return ErrorData && ErrorData->m_Backend == "postgres" && (ErrorData->m_BackendCode == "0A000" || ErrorData->m_BackendCode == "26000");
	}

	bool CPostgresTransactionActor::fp_EndedByServer()
	{
		// Raw SQL dropped this connection's prepared statements, so the typed operations that follow must prepare again
		if (mp_pServerState->m_bPreparedCachesStale && mp_ConnectionCheckout.m_Client)
		{
			mp_pServerState->m_bPreparedCachesStale = false;
			mp_ConnectionCheckout.m_pPreparedSelectCache->f_Clear();
			mp_ConnectionCheckout.m_pPreparedInsertCache->f_Clear();
			mp_ConnectionCheckout.m_pPreparedUpdateCache->f_Clear();
			mp_ConnectionCheckout.m_pPreparedDeleteCache->f_Clear();
			mp_ConnectionCheckout.m_pPreparedUpsertCache->f_Clear();
		}

		if (mp_bEndedByServer)
			return true;

		// Also while a commit or rollback waits for the operations queued ahead of it, which must not run outside the
		// transaction either
		if (!mp_pServerState->m_bEnded)
			return false;

		mp_bFinished = true;
		mp_bEndedByServer = true;
		// Raw SQL ended the transaction, so it can also have left session state that only an asynchronous reset clears
		if (!mp_pServerState->m_bStatementsDropped && !mp_pServerState->m_bDisconnected && !mp_pServerState->m_bRawSqlAttempted)
			mp_ConnectionCheckout.f_MarkReusable();
		mp_ConnectionCheckout = {};

		return true;
	}

	NException::CExceptionPointer CPostgresTransactionActor::fp_CheckNotFinished()
	{
		if (fp_EndedByServer())
			return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");

		if (mp_bFinished)
			return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after transaction finished");

		return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_Insert(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedInsert(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && mp_ConnectionCheckout.m_pPreparedInsertCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto PostgresValues = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

			if (bUsePrepared)
				co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecutePrepared, StatementEntry.m_Name, fg_Move(PostgresValues));
			else
				co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, StatementEntry.m_Sql, fg_Move(PostgresValues));

			if (bWarmPrepared)
				co_await fp_WarmPreparedInsert(&Description).f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared insert");
		}
		else
		{
			auto Sql = fg_PostgresInsertSql(_Operation);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));
		}

		co_return {};
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_InsertMany(CSqlBulkInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		if (!_Operation.m_pDescription)
			co_return DMibErrorDatabaseInstance("PostgreSQL bulk insert requires a prepared insert description");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = fp_GetPreparedInsert(&Description);
		bool bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && mp_ConnectionCheckout.m_pPreparedInsertCache->f_Find(&Description) != nullptr;

		umint nTotalAffected = 0;
		if (bUsePrepared)
		{
			nTotalAffected = co_await mp_ConnectionCheckout.m_Client
				(
					&CPostgresClientActor::f_ExecutePreparedBulk
					, StatementEntry.m_Name
					, fg_PostgresParameterBatchGenerator(fg_Move(_Operation.m_RowBatches), mp_nPipelineLength)
				)
			;
		}
		else
		{
			for (auto iBatch = co_await fg_Move(_Operation.m_RowBatches).f_GetPipelinedIterator(mp_nPipelineLength); iBatch; co_await ++iBatch)
			{
				for (auto &Row : *iBatch)
				{
					auto Values = co_await fg_PostgresValues(fg_Move(Row));
					nTotalAffected += co_await fg_PostgresExecuteAffected(mp_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));
				}
			}

			if (!StatementEntry.m_Name.f_IsEmpty())
				co_await fp_WarmPreparedInsert(&Description).f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared insert");
		}

		co_return nTotalAffected;
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_InsertReturning(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		auto Sql = fg_PostgresInsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_Upsert(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot upsert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedUpsert(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && mp_ConnectionCheckout.m_pPreparedUpsertCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

			umint nAffected = 0;
			if (bUsePrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(mp_ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(mp_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));

			if (bWarmPrepared)
				co_await fp_WarmPreparedUpsert(&Description).f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared upsert");

			co_return nAffected;
		}

		auto Sql = fg_PostgresUpsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

		co_return co_await fg_PostgresExecuteAffected(mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_UpsertReturning(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot upsert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		auto Sql = fg_PostgresUpsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_Update(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot update in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedUpdate(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && mp_ConnectionCheckout.m_pPreparedUpdateCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			auto ParameterTypes = fg_PostgresParameterValueTypes(Values);
			bWarmPrepared = bWarmPrepared && !fg_PostgresValuesHaveNull(Values);

			umint nAffected = 0;
			if (bUsePrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(mp_ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(mp_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));

			if (bWarmPrepared)
			{
				co_await fp_WarmPreparedUpdate(&Description, fg_Move(ParameterTypes)).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared update")
				;
			}

			co_return nAffected;
		}

		auto Sql = fg_PostgresUpdateSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

		co_return co_await fg_PostgresExecuteAffected(mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_UpdateReturning(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot update in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		auto Sql = fg_PostgresUpdateSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_Delete(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot delete in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedDelete(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && mp_ConnectionCheckout.m_pPreparedDeleteCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			auto ParameterTypes = fg_PostgresParameterValueTypes(Values);
			bWarmPrepared = bWarmPrepared && !fg_PostgresValuesHaveNull(Values);

			umint nAffected = 0;
			if (bUsePrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(mp_ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(mp_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));

			if (bWarmPrepared)
			{
				co_await fp_WarmPreparedDelete(&Description, fg_Move(ParameterTypes)).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared delete")
				;
			}

			co_return nAffected;
		}

		auto Sql = fg_PostgresDeleteSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

		co_return co_await fg_PostgresExecuteAffected(mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_DeleteReturning(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot delete in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		auto Sql = fg_PostgresDeleteSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CPostgresTransactionActor::fs_Select(CPostgresTransactionActor *_pTransaction, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await _pTransaction->f_CheckDestroyedOnResume();
		if (auto pException = _pTransaction->fp_CheckNotFinished())
			co_return pException;

		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = _pTransaction->fp_GetPreparedSelect(&Description);
		CSqlPreparedSelectStatementDescription const *pStatementDescription = &StatementEntry.m_Description;
		if (auto pException = fg_PostgresValidateSelectStatement(*pStatementDescription))
			co_return pException;

		if (_Operation.m_nResultRowLimit != 0 && !pStatementDescription->m_LimitOffset.m_bHasLimit)
			co_return DMibErrorDatabaseInstance("SELECT specifies a result-row limit but the prepared statement does not declare f_WithLimit()");
		if (_Operation.m_nResultRowOffset != 0 && !pStatementDescription->m_LimitOffset.m_bHasOffset)
			co_return DMibErrorDatabaseInstance("SELECT specifies a result-row offset but the prepared statement does not declare f_WithOffset()");
		auto Sql = StatementEntry.m_Sql;
		auto &OperationParameters = _Operation.m_Parameters;
		if (pStatementDescription->m_LimitOffset.m_bHasLimit)
			OperationParameters.f_InsertLast(_Operation.m_nResultRowLimit != 0 ? NPrivate::fg_SqlRowCountParameter(_Operation.m_nResultRowLimit) : TCLimitsInt<int64>::mc_Max);
		if (pStatementDescription->m_LimitOffset.m_bHasOffset)
			OperationParameters.f_InsertLast(NPrivate::fg_SqlRowCountParameter(_Operation.m_nResultRowOffset));
		auto Mapping = pStatementDescription->m_RowMapping;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto PostgresParameters = co_await fg_PostgresValues(fg_Move(Parameters));
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : _pTransaction->mp_nSelectRowsPerBatch;
		auto nPipelineLength = _pTransaction->mp_nPipelineLength;

		auto Sequence = co_await _pTransaction->mp_Sequencer.f_Sequence();
		if (_pTransaction->fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");

		// Operations queued ahead of this one can drop the prepared statements, so whether this one is still prepared is checked only now
		auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && _pTransaction->mp_ConnectionCheckout.m_pPreparedSelectCache->f_Find(&Description) != nullptr;
		auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();

		// Finishing the select does not finish its transaction, so only a successful commit or rollback makes the connection reusable
		_pTransaction->mp_ConnectionCheckout.f_MarkReusable(false);

		NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch> Rows;
		if (bUsePrepared)
			Rows = co_await _pTransaction->mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecutePreparedRows, StatementEntry.m_Name, fg_Move(PostgresParameters), nRowsPerBatch);
		else
			Rows = co_await _pTransaction->mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteRows, fg_Move(Sql), fg_Move(PostgresParameters), nRowsPerBatch);

		// A prepared statement whose result type a schema change altered, or that is gone, keeps failing until it is prepared again on
		// another session, so this transaction stops using its prepared statements and the connection is not pooled again
		auto fOnRowsFailed = [&](NException::CExceptionPointer const &_pException)
			{
				if (bUsePrepared && fg_PostgresIsStalePreparedStatementError(_pException))
				{
					_pTransaction->mp_pServerState->m_bStatementsDropped = true;
					_pTransaction->mp_pServerState->m_bPreparedCachesStale = true;
				}
			}
		;

		auto IteratorResult = co_await fg_Move(Rows).f_GetPipelinedIterator(nPipelineLength).f_Wrap();
		if (!IteratorResult)
		{
			fOnRowsFailed(IteratorResult.f_GetException());
			co_return IteratorResult.f_GetException();
		}

		auto iBatch = IteratorResult.f_Move();

		// The sequence is held until the inner stream has drained, also when this stream is abandoned, so the transaction's next
		// operation starts after it. The references are used only before the cleanup's first suspension.
		auto DestroyRows = co_await NConcurrency::fg_AsyncDestroy
			(
				[&Sequence, &iBatch]() -> NConcurrency::TCFuture<void>
				{
					auto LocalSequence = fg_Move(Sequence);
					auto iLocalBatch = fg_Move(iBatch);

					co_await fg_Move(iLocalBatch).f_Destroy().f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to destroy a transaction row stream");

					co_return {};
				}
			)
		;

		while (iBatch)
		{
			CSqlRowDataBatch Batch;
			for (auto &Row : *iBatch)
				Batch.f_InsertLast(co_await fg_PostgresMapRow(fg_Move(Row), Mapping));

			co_yield fg_Move(Batch);

			auto NextResult = co_await (++iBatch).f_Wrap();
			if (!NextResult)
			{
				fOnRowsFailed(NextResult.f_GetException());
				co_return NextResult.f_GetException();
			}
		}

		if (bWarmPrepared)
			co_await _pTransaction->fp_WarmPreparedSelect(&Description).f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared select");

		co_return {};
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::fs_Count(CPostgresTransactionActor *_pTransaction, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await _pTransaction->f_CheckDestroyedOnResume();
		if (auto pException = _pTransaction->fp_CheckNotFinished())
			co_return pException;

		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = _pTransaction->fp_GetPreparedSelect(&Description);
		if (auto pException = fg_PostgresValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_CountSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);

		auto Sequence = co_await _pTransaction->mp_Sequencer.f_Sequence();
		if (_pTransaction->fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		co_return co_await fg_PostgresExecuteCount(_pTransaction->mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<bool> CPostgresTransactionActor::fs_Exists(CPostgresTransactionActor *_pTransaction, CSqlSelectOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await _pTransaction->f_CheckDestroyedOnResume();
		if (auto pException = _pTransaction->fp_CheckNotFinished())
			co_return pException;

		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = _pTransaction->fp_GetPreparedSelect(&Description);
		if (auto pException = fg_PostgresValidateSelectStatement(StatementEntry.m_Description))
			co_return pException;

		auto Sql = StatementEntry.m_ExistsSql;
		auto Parameters = fg_Move(_Operation.m_Parameters);

		auto Sequence = co_await _pTransaction->mp_Sequencer.f_Sequence();
		if (_pTransaction->fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		co_return co_await fg_PostgresExecuteExists(_pTransaction->mp_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCAsyncGenerator<CSqlRowDataBatch> CPostgresTransactionActor::f_Select(CSqlSelectOperation _Operation)
	{
		return fs_Select(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_Count(CSqlSelectOperation _Operation)
	{
		return fs_Count(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<bool> CPostgresTransactionActor::f_Exists(CSqlSelectOperation _Operation)
	{
		return fs_Exists(this, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_ExecuteRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_SQLite)
			co_return DMibErrorDatabaseInstance("SQLite-specific raw SQL operation cannot run on the PostgreSQL backend");

		if (mp_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot execute raw SQL in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		// A failing statement can end the transaction too, for example COMMIT with a violated deferred constraint
		auto Client = mp_ConnectionCheckout.m_Client;
		auto CountersBefore = co_await fg_PostgresBeforeTransactionRawSql(Client, mp_pServerState);
		auto Result = co_await fg_PostgresExecuteAffected(Client, fg_Move(Sql), fg_Move(Values)).f_Wrap();
		co_await fg_PostgresSyncTransactionServerState(fg_Move(Client), mp_pServerState, CountersBefore);

		co_return fg_Move(Result);
	}

	NConcurrency::TCFuture<CSqlRawResult> CPostgresTransactionActor::f_QueryRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_SQLite)
			co_return DMibErrorDatabaseInstance("SQLite-specific raw SQL operation cannot run on the PostgreSQL backend");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		auto Client = mp_ConnectionCheckout.m_Client;
		auto CountersBefore = co_await fg_PostgresBeforeTransactionRawSql(Client, mp_pServerState);
		auto QueryResult = co_await Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values)).f_Wrap();
		co_await fg_PostgresSyncTransactionServerState(fg_Move(Client), mp_pServerState, CountersBefore);
		if (!QueryResult)
			co_return QueryResult.f_GetException();

		co_return fg_PostgresRawResult(QueryResult.f_Move());
	}

	// Holds the transaction's sequence until the inner stream is gone and the state is synchronized, so no queued operation sees the
	// stale state
	static auto fg_PostgresFinishTransactionRawStream
		(
			NConcurrency::CActorSubscription _Sequence
			, NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator _iItems
			, NConcurrency::TCActor<CPostgresClientActor> _Client
			, NStorage::TCSharedPointer<CPostgresTransactionServerState> _pServerState
			, CPostgresSessionCounters _CountersBefore
		)
		-> NConcurrency::TCFuture<void>
	{
		co_await fg_Move(_iItems).f_Destroy().f_Wrap() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to destroy a raw transaction row stream");
		co_await fg_PostgresSyncTransactionServerState(fg_Move(_Client), fg_Move(_pServerState), _CountersBefore);

		co_return {};
	}

	// A raw stream's state until it finishes. A stream dropped before its rows are read finishes from the destructor, on the
	// transaction's actor, which owns the server state.
	struct CPostgresTransactionRawStreamState
	{
		CPostgresTransactionRawStreamState
			(
				NConcurrency::TCActor<NConcurrency::CActor> &&_Owner
				, NConcurrency::CActorSubscription &&_Sequence
				, NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator &&_iItems
				, NConcurrency::TCActor<CPostgresClientActor> const &_Client
				, NStorage::TCSharedPointer<CPostgresTransactionServerState> const &_pServerState
				, CPostgresSessionCounters const &_CountersBefore
			)
		;
		~CPostgresTransactionRawStreamState();

		NConcurrency::TCFuture<void> f_Finish();

		NConcurrency::TCActor<NConcurrency::CActor> m_Owner;
		NConcurrency::CActorSubscription m_Sequence;
		NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator m_iItems;
		NConcurrency::TCActor<CPostgresClientActor> m_Client;
		NStorage::TCSharedPointer<CPostgresTransactionServerState> m_pServerState;
		CPostgresSessionCounters m_CountersBefore;
		bool m_bFinished = false;
	};

	CPostgresTransactionRawStreamState::CPostgresTransactionRawStreamState
		(
			NConcurrency::TCActor<NConcurrency::CActor> &&_Owner
			, NConcurrency::CActorSubscription &&_Sequence
			, NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator &&_iItems
			, NConcurrency::TCActor<CPostgresClientActor> const &_Client
			, NStorage::TCSharedPointer<CPostgresTransactionServerState> const &_pServerState
			, CPostgresSessionCounters const &_CountersBefore
		)
		: m_Owner(fg_Move(_Owner))
		, m_Sequence(fg_Move(_Sequence))
		, m_iItems(fg_Move(_iItems))
		, m_Client(_Client)
		, m_pServerState(_pServerState)
		, m_CountersBefore(_CountersBefore)
	{
	}

	CPostgresTransactionRawStreamState::~CPostgresTransactionRawStreamState()
	{
		if (!m_bFinished)
			f_Finish() > NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to finish an unread raw transaction row stream");
	}

	NConcurrency::TCFuture<void> CPostgresTransactionRawStreamState::f_Finish()
	{
		m_bFinished = true;

		return NConcurrency::g_Dispatch(m_Owner)
			/ [Sequence = fg_Move(m_Sequence), iItems = fg_Move(m_iItems), Client = fg_Move(m_Client), pServerState = fg_Move(m_pServerState), CountersBefore = m_CountersBefore]
			() mutable -> NConcurrency::TCFuture<void>
			{
				return fg_PostgresFinishTransactionRawStream(fg_Move(Sequence), fg_Move(iItems), fg_Move(Client), fg_Move(pServerState), CountersBefore);
			}
		;
	}

	static auto fg_PostgresTransactionRawRowStreamFromIterator(NStorage::TCUniquePointer<CPostgresTransactionRawStreamState> _pState) -> NConcurrency::TCAsyncGenerator<CSqlRawRowBatch>
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		// The parameter reference is valid only until the cleanup's first suspension, which f_Finish does not reach before it moves the state
		auto FinishStream = co_await NConcurrency::fg_AsyncDestroy
			(
				[&_pState]() -> NConcurrency::TCFuture<void>
				{
					return _pState->f_Finish();
				}
			)
		;

		auto &iItems = _pState->m_iItems;
		for (; iItems; co_await ++iItems)
		{
			CPostgresRowStreamItem Item = fg_Move(*iItems);
			if (Item.m_Description)
				continue;

			if (Item.m_Rows.f_IsEmpty())
				continue;

			CSqlRawRowBatch Batch;
			Batch.f_Reserve(Item.m_Rows.f_GetLen());
			for (auto &Row : Item.m_Rows)
				Batch.f_InsertLast(fg_PostgresRawRow(fg_Move(Row)));

			co_yield fg_Move(Batch);
		}

		co_return {};
	}

	NConcurrency::TCFuture<CSqlRawStream> CPostgresTransactionActor::f_QueryRawStream(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_SQLite)
			co_return DMibErrorDatabaseInstance("SQLite-specific raw SQL operation cannot run on the PostgreSQL backend");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : mp_nSelectRowsPerBatch;
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after raw SQL ended it");
		mp_ConnectionCheckout.f_MarkReusable(false);
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		auto Client = mp_ConnectionCheckout.m_Client;
		auto CountersBefore = co_await fg_PostgresBeforeTransactionRawSql(Client, mp_pServerState);
		auto Items = co_await Client(&CPostgresClientActor::f_ExecuteRowsStream, fg_Move(Sql), fg_Move(Values), nRowsPerBatch);
		auto ItemsResult = co_await fg_Move(Items).f_GetPipelinedIterator(mp_nPipelineLength).f_Wrap();
		// A statement that ended without rows, such as ROLLBACK, or failed can still have ended the transaction
		if (!ItemsResult || !*ItemsResult)
		{
			co_await fg_PostgresSyncTransactionServerState(Client, mp_pServerState, CountersBefore);
			if (!ItemsResult)
				co_return ItemsResult.f_GetException();

			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream completed without producing a row description");
		}

		// From here every exit, including a stream that is never read, drains the inner stream and synchronizes the state before
		// the sequence is released
		NStorage::TCUniquePointer<CPostgresTransactionRawStreamState> pState = fg_Construct
			(
				NConcurrency::fg_ThisActor(this)
				, fg_Move(Sequence)
				, ItemsResult.f_Move()
				, Client
				, mp_pServerState
				, CountersBefore
			)
		;

		CPostgresRowStreamItem FirstItem = fg_Move(*pState->m_iItems);
		if (!FirstItem.m_Description)
		{
			co_await pState->f_Finish();
			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream did not produce a row description before rows");
		}

		auto NextResult = co_await (++pState->m_iItems).f_Wrap();
		if (!NextResult)
		{
			co_await pState->f_Finish();
			co_return NextResult.f_GetException();
		}

		CSqlRawStream Stream;
		Stream.m_Columns = fg_PostgresRawColumns(*FirstItem.m_Description);
		Stream.m_Rows = fg_PostgresTransactionRawRowStreamFromIterator(fg_Move(pState));

		co_return Stream;
	}

	NConcurrency::TCFuture<NStr::CStr> CPostgresTransactionActor::f_CreateSavepoint()
	{
		return fp_CreateSavepoint(NPrivate::CSqlSavepointStack::EKind::mc_Manual);
	}

	NConcurrency::TCFuture<NStr::CStr> CPostgresTransactionActor::f_CreateScopeSavepoint()
	{
		return fp_CreateSavepoint(NPrivate::CSqlSavepointStack::EKind::mc_Scope);
	}

	NConcurrency::TCFuture<NStr::CStr> CPostgresTransactionActor::fp_CreateSavepoint(NPrivate::CSqlSavepointStack::EKind _Kind)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (fp_EndedByServer() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot create PostgreSQL savepoint after transaction finished");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot create PostgreSQL savepoint after raw SQL ended the transaction");
		auto Name = fg_PostgresSavepointName(++mp_iNextSavepoint);
		co_await fg_PostgresMarkTransaction(mp_ConnectionCheckout.m_Client, mp_pServerState);
		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("SAVEPOINT ", Name));
		mp_Savepoints.f_Created(Name, _Kind);

		co_return Name;
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_ReleaseSavepoint(NStr::CStr _Name)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (fp_EndedByServer() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot release PostgreSQL savepoint after transaction finished");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot release PostgreSQL savepoint after raw SQL ended the transaction");
		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("RELEASE SAVEPOINT ", _Name));
		mp_Savepoints.f_Released(_Name);

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_RollbackToSavepoint(NStr::CStr _Name)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (fp_EndedByServer() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot roll back PostgreSQL savepoint after transaction finished");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot roll back PostgreSQL savepoint after raw SQL ended the transaction");
		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("ROLLBACK TO SAVEPOINT ", _Name));
		mp_Savepoints.f_RolledBackTo(_Name);

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_EndScopeSavepoint(NStr::CStr _Name, ESqlTransactionScopeEnd _End)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (fp_EndedByServer() || mp_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot end PostgreSQL transaction scope after transaction finished");

		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot end PostgreSQL transaction scope after raw SQL ended the transaction");

		if (auto pException = mp_Savepoints.f_CheckScopeEnd(_Name))
			co_return pException;

		if (_End == ESqlTransactionScopeEnd::mc_RollbackAndRelease)
		{
			co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("ROLLBACK TO SAVEPOINT ", _Name));
			mp_Savepoints.f_RolledBackTo(_Name);
		}

		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("RELEASE SAVEPOINT ", _Name));
		mp_Savepoints.f_Released(_Name);

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_CommitTransaction()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (fp_EndedByServer())
			co_return DMibErrorDatabaseInstance("Cannot commit PostgreSQL transaction that raw SQL already ended");

		if (!mp_bFinished)
		{
			mp_bFinished = true;
			auto Sequence = co_await mp_Sequencer.f_Sequence();

			// An operation queued ahead of this one can have ended the transaction
			if (fp_EndedByServer())
				co_return DMibErrorDatabaseInstance("Cannot commit PostgreSQL transaction that raw SQL already ended");

			auto Result = co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_CommitTransaction).f_Wrap();
			if (Result && !mp_pServerState->m_bStatementsDropped && (!mp_pServerState->m_bRawSqlAttempted || co_await fg_PostgresResetSession(mp_ConnectionCheckout.m_Client)))
				mp_ConnectionCheckout.f_MarkReusable();

			// The finished transaction gives its pooled connection back, a handle kept in scope would otherwise hold it
			mp_ConnectionCheckout = {};

			if (!Result)
			{
				mp_pCommitException = Result.f_GetException();
				co_return mp_pCommitException;
			}

			mp_bCommitted = true;

			co_return {};
		}

		// Another commit or rollback is finishing the transaction; report a commit's outcome only once it is known
		auto Sequence = co_await mp_Sequencer.f_Sequence();
		if (mp_pCommitException)
			co_return mp_pCommitException;

		if (!mp_bCommitted)
			co_return DMibErrorDatabaseInstance("Cannot commit PostgreSQL transaction that was rolled back");

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_RollbackTransaction()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (fp_EndedByServer())
			co_return {};

		if (mp_bFinished)
		{
			// mp_bFinished is set before the commit or rollback that set it has completed
			auto Sequence = co_await mp_Sequencer.f_Sequence();

			co_return {};
		}

		{
			mp_bFinished = true;
			auto Sequence = co_await mp_Sequencer.f_Sequence();

			// An operation queued ahead of this one can have ended the transaction, leaving nothing to roll back
			if (fp_EndedByServer())
				co_return {};

			auto Result = co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_RollbackTransaction).f_Wrap();
			if (Result && !mp_pServerState->m_bStatementsDropped && (!mp_pServerState->m_bRawSqlAttempted || co_await fg_PostgresResetSession(mp_ConnectionCheckout.m_Client)))
				mp_ConnectionCheckout.f_MarkReusable();

			// The finished transaction gives its pooled connection back, a handle kept in scope would otherwise hold it
			mp_ConnectionCheckout = {};

			if (!Result)
				co_return Result.f_GetException();
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_Destroy()
	{
		if (mp_ConnectionCheckout.m_Client && !mp_bFinished)
		{
			auto RollbackResult = co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_RollbackTransaction).f_Wrap();
			if (RollbackResult && !mp_pServerState->m_bStatementsDropped && (!mp_pServerState->m_bRawSqlAttempted || co_await fg_PostgresResetSession(mp_ConnectionCheckout.m_Client)))
				mp_ConnectionCheckout.f_MarkReusable();
			else
				NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to roll back transaction during destroy")(fg_Move(RollbackResult));

			mp_bFinished = true;
		}

		co_return {};
	}

	CPostgresPreparedSelectCacheEntry CPostgresTransactionActor::fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedSelectCacheEntry const *pEntry = mp_ConnectionCheckout.m_pPreparedSelectCache->f_FindByPointer(_pDescription))
			return *pEntry;

		if (mp_pPreparedSelectCache)
		{
			if (CPostgresPreparedSelectCacheEntry const *pEntry = mp_pPreparedSelectCache->f_FindByPointer(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedSelectCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresSelectSql(Entry.m_Description, _pDescription->m_ParameterTypes.m_nTypes);
		Entry.m_CountSql = fg_PostgresSelectCountSql(Entry.m_Description);
		Entry.m_ExistsSql = fg_PostgresSelectExistsSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedSelectName(_pDescription->m_QueryID);

		return Entry;
	}

	// NOLINTNEXTLINE(malterlib-coroutine-reference-parameter): the description is in static storage, as an operation's m_pDescription must be, so it outlives every suspension
	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		// A connection that is not pooled again gains nothing from preparing, and it can still hold the statement under its name
		if (!mp_Backend || mp_pServerState->m_bStatementsDropped)
			co_return {};

		auto Entry = co_await mp_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedSelect, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (mp_ConnectionCheckout.m_pPreparedSelectCache->f_Find(_pDescription))
			co_return {};

		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_PostgresParameterTypes(_pDescription->m_ParameterTypes));

		// Cache this description's own row mapping: the server statement, shared by identical SQL, does not identify it
		auto LocalEntry = fp_GetPreparedSelect(_pDescription);
		mp_ConnectionCheckout.m_pPreparedSelectCache->f_Insert(_pDescription, fg_Move(LocalEntry));

		co_return {};
	}

	CPostgresPreparedInsertCacheEntry CPostgresTransactionActor::fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedInsertCacheEntry const *pEntry = mp_ConnectionCheckout.m_pPreparedInsertCache->f_Find(_pDescription))
			return *pEntry;

		if (mp_pPreparedInsertCache)
		{
			if (CPostgresPreparedInsertCacheEntry const *pEntry = mp_pPreparedInsertCache->f_Find(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedInsertCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		CSqlInsertOperation Operation;
		Operation.m_TableName = Entry.m_Description.m_TableName;
		Operation.m_pDescription = _pDescription;
		Entry.m_Sql = fg_PostgresInsertSql(Operation);
		Entry.m_Name = fg_PostgresPreparedInsertName(_pDescription->m_QueryID);

		return Entry;
	}

	// NOLINTNEXTLINE(malterlib-coroutine-reference-parameter): the description is in static storage, as an operation's m_pDescription must be, so it outlives every suspension
	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		// A connection that is not pooled again gains nothing from preparing, and it can still hold the statement under its name
		if (!mp_Backend || mp_pServerState->m_bStatementsDropped)
			co_return {};

		auto Entry = co_await mp_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedInsert, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (mp_ConnectionCheckout.m_pPreparedInsertCache->f_Find(_pDescription))
			co_return {};

		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_PostgresValueTypes(Entry.m_Description.m_InsertColumnTypes));

		mp_ConnectionCheckout.m_pPreparedInsertCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}

	CPostgresPreparedUpdateCacheEntry CPostgresTransactionActor::fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpdateCacheEntry const *pEntry = mp_ConnectionCheckout.m_pPreparedUpdateCache->f_Find(_pDescription))
			return *pEntry;

		if (mp_pPreparedUpdateCache)
		{
			if (CPostgresPreparedUpdateCacheEntry const *pEntry = mp_pPreparedUpdateCache->f_Find(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedUpdateCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresUpdateSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedUpdateName(_pDescription->m_QueryID);

		return Entry;
	}

	// NOLINTNEXTLINE(malterlib-coroutine-reference-parameter): the description is in static storage, as an operation's m_pDescription must be, so it outlives every suspension
	auto CPostgresTransactionActor::fp_WarmPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription, NContainer::TCVector<EPostgresValueType> _ParameterTypes)
		-> NConcurrency::TCFuture<void>
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		// A connection that is not pooled again gains nothing from preparing, and it can still hold the statement under its name
		if (!mp_Backend || mp_pServerState->m_bStatementsDropped)
			co_return {};

		auto Entry = co_await mp_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedUpdate, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (mp_ConnectionCheckout.m_pPreparedUpdateCache->f_Find(_pDescription))
			co_return {};

		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_Move(_ParameterTypes));

		mp_ConnectionCheckout.m_pPreparedUpdateCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}

	CPostgresPreparedDeleteCacheEntry CPostgresTransactionActor::fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedDeleteCacheEntry const *pEntry = mp_ConnectionCheckout.m_pPreparedDeleteCache->f_Find(_pDescription))
			return *pEntry;

		if (mp_pPreparedDeleteCache)
		{
			if (CPostgresPreparedDeleteCacheEntry const *pEntry = mp_pPreparedDeleteCache->f_Find(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedDeleteCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresDeleteSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedDeleteName(_pDescription->m_QueryID);

		return Entry;
	}

	// NOLINTNEXTLINE(malterlib-coroutine-reference-parameter): the description is in static storage, as an operation's m_pDescription must be, so it outlives every suspension
	auto CPostgresTransactionActor::fp_WarmPreparedDelete(CSqlDeleteOperationDescription const *_pDescription, NContainer::TCVector<EPostgresValueType> _ParameterTypes)
		-> NConcurrency::TCFuture<void>
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		// A connection that is not pooled again gains nothing from preparing, and it can still hold the statement under its name
		if (!mp_Backend || mp_pServerState->m_bStatementsDropped)
			co_return {};

		auto Entry = co_await mp_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedDelete, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (mp_ConnectionCheckout.m_pPreparedDeleteCache->f_Find(_pDescription))
			co_return {};

		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_Move(_ParameterTypes));

		mp_ConnectionCheckout.m_pPreparedDeleteCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}

	CPostgresPreparedUpsertCacheEntry CPostgresTransactionActor::fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpsertCacheEntry const *pEntry = mp_ConnectionCheckout.m_pPreparedUpsertCache->f_Find(_pDescription))
			return *pEntry;

		if (mp_pPreparedUpsertCache)
		{
			if (CPostgresPreparedUpsertCacheEntry const *pEntry = mp_pPreparedUpsertCache->f_Find(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedUpsertCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresUpsertSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedUpsertName(_pDescription->m_QueryID);

		return Entry;
	}

	// NOLINTNEXTLINE(malterlib-coroutine-reference-parameter): the description is in static storage, as an operation's m_pDescription must be, so it outlives every suspension
	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		// A connection that is not pooled again gains nothing from preparing, and it can still hold the statement under its name
		if (!mp_Backend || mp_pServerState->m_bStatementsDropped)
			co_return {};

		auto Entry = co_await mp_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedUpsert, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (mp_ConnectionCheckout.m_pPreparedUpsertCache->f_Find(_pDescription))
			co_return {};

		co_await mp_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_PostgresValueTypes(Entry.m_Description.m_InsertColumnTypes));

		mp_ConnectionCheckout.m_pPreparedUpsertCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}
}
