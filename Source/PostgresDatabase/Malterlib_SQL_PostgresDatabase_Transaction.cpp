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
		: m_bReadOnly(_bReadOnly)
		, m_TransactionSettings(_TransactionSettings)
		, m_ConnectionCheckout(fg_Move(_ConnectionCheckout))
		, m_nSelectRowsPerBatch(_nSelectRowsPerBatch)
		, m_nPipelineLength(_nPipelineLength)
		, m_Backend(fg_Move(_Backend))
		, m_Sequencer("PostgreSQL transaction")
		, m_pPreparedSelectCache(fg_Move(_pPreparedSelectCache))
		, m_pPreparedInsertCache(fg_Move(_pPreparedInsertCache))
		, m_pPreparedUpdateCache(fg_Move(_pPreparedUpdateCache))
		, m_pPreparedDeleteCache(fg_Move(_pPreparedDeleteCache))
		, m_pPreparedUpsertCache(fg_Move(_pPreparedUpsertCache))
	{
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_OpenBegin()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();
		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_BeginTransaction, m_bReadOnly, m_TransactionSettings);

		co_return {};
	}

	NException::CExceptionPointer CPostgresTransactionActor::fp_CheckNotFinished()
	{
		if (m_bFinished)
			return DMibErrorDatabaseInstance("Cannot use PostgreSQL transaction after transaction finished");

		return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_Insert(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedInsert(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && m_ConnectionCheckout.m_pPreparedInsertCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto PostgresValues = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

			if (bUsePrepared)
				co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecutePrepared, StatementEntry.m_Name, fg_Move(PostgresValues));
			else
				co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, StatementEntry.m_Sql, fg_Move(PostgresValues));

			if (bWarmPrepared)
			{
				co_await fp_WarmPreparedInsert(&Description).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared insert")
				;
			}
		}
		else
		{
			auto Sql = fg_PostgresInsertSql(_Operation);
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
			co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));
		}

		co_return {};
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_InsertMany(CSqlBulkInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		if (!_Operation.m_pDescription)
			co_return DMibErrorDatabaseInstance("PostgreSQL bulk insert requires a prepared insert description");

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto const &Description = *_Operation.m_pDescription;
		auto StatementEntry = fp_GetPreparedInsert(&Description);
		bool bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && m_ConnectionCheckout.m_pPreparedInsertCache->f_Find(&Description) != nullptr;

		umint nTotalAffected = 0;
		if (bUsePrepared)
		{
			nTotalAffected = co_await m_ConnectionCheckout.m_Client
				(
					&CPostgresClientActor::f_ExecutePreparedBulk
					, StatementEntry.m_Name
					, fg_PostgresParameterBatchGenerator(fg_Move(_Operation.m_RowBatches), m_nPipelineLength)
				)
			;
		}
		else
		{
			for (auto iBatch = co_await fg_Move(_Operation.m_RowBatches).f_GetPipelinedIterator(m_nPipelineLength); iBatch; co_await ++iBatch)
			{
				for (auto &Row : *iBatch)
				{
					auto Values = co_await fg_PostgresValues(fg_Move(Row));
					nTotalAffected += co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));
				}
			}

			if (!StatementEntry.m_Name.f_IsEmpty())
			{
				co_await fp_WarmPreparedInsert(&Description).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared insert")
				;
			}
		}

		co_return nTotalAffected;
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_InsertReturning(CSqlInsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot insert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto Sql = fg_PostgresInsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_Upsert(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot upsert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedUpsert(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && m_ConnectionCheckout.m_pPreparedUpsertCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

			umint nAffected = 0;
			if (bUsePrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(m_ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));

			if (bWarmPrepared)
			{
				co_await fp_WarmPreparedUpsert(&Description).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared upsert")
				;
			}

			co_return nAffected;
		}

		auto Sql = fg_PostgresUpsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

		co_return co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_UpsertReturning(CSqlUpsertOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot upsert in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto Sql = fg_PostgresUpsertSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_Update(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot update in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedUpdate(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && m_ConnectionCheckout.m_pPreparedUpdateCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

			umint nAffected = 0;
			if (bUsePrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(m_ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));

			if (bWarmPrepared)
			{
				co_await fp_WarmPreparedUpdate(&Description).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared update")
				;
			}

			co_return nAffected;
		}

		auto Sql = fg_PostgresUpdateSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

		co_return co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_UpdateReturning(CSqlUpdateOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot update in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto Sql = fg_PostgresUpdateSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
	}

	NConcurrency::TCFuture<umint> CPostgresTransactionActor::f_Delete(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot delete in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		if (_Operation.m_pDescription)
		{
			auto const &Description = *_Operation.m_pDescription;
			auto StatementEntry = fp_GetPreparedDelete(&Description);
			auto bUsePrepared = !StatementEntry.m_Name.f_IsEmpty() && m_ConnectionCheckout.m_pPreparedDeleteCache->f_Find(&Description) != nullptr;
			auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
			auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

			umint nAffected = 0;
			if (bUsePrepared)
				nAffected = co_await fg_PostgresExecutePreparedAffected(m_ConnectionCheckout.m_Client, StatementEntry.m_Name, fg_Move(Values));
			else
				nAffected = co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, StatementEntry.m_Sql, fg_Move(Values));

			if (bWarmPrepared)
			{
				co_await fp_WarmPreparedDelete(&Description).f_Wrap()
					> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared delete")
				;
			}

			co_return nAffected;
		}

		auto Sql = fg_PostgresDeleteSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));

		co_return co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<CSqlValue> CPostgresTransactionActor::f_DeleteReturning(CSqlDeleteOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot delete in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto Sql = fg_PostgresDeleteSql(_Operation);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Values));
		CSqlRowFieldMapping Field;
		Field.m_ColumnName = fg_Move(_Operation.m_ReturningColumnName);
		Field.m_ValueType = _Operation.m_ReturningValueType;

		co_return co_await fg_PostgresExecuteReturningValue(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values), fg_Move(Field));
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
		auto bUsePrepared =
			!StatementEntry.m_Name.f_IsEmpty()
			&& _pTransaction->m_ConnectionCheckout.m_pPreparedSelectCache->f_Find(&Description) != nullptr
		;
		auto bWarmPrepared = !bUsePrepared && !StatementEntry.m_Name.f_IsEmpty();
		auto Sql = StatementEntry.m_Sql;
		auto &OperationParameters = _Operation.m_Parameters;
		if (pStatementDescription->m_LimitOffset.m_bHasLimit)
			OperationParameters.f_InsertLast(_Operation.m_nResultRowLimit != 0 ? int64(_Operation.m_nResultRowLimit) : TCLimitsInt<int64>::mc_Max);
		if (pStatementDescription->m_LimitOffset.m_bHasOffset)
			OperationParameters.f_InsertLast(int64(_Operation.m_nResultRowOffset));
		auto Mapping = pStatementDescription->m_RowMapping;
		auto Parameters = fg_Move(_Operation.m_Parameters);
		auto PostgresParameters = co_await fg_PostgresValues(fg_Move(Parameters));
		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : _pTransaction->m_nSelectRowsPerBatch;
		auto nPipelineLength = _pTransaction->m_nPipelineLength;

		auto Sequence = co_await _pTransaction->m_Sequencer.f_Sequence();
		// The connection holds an open transaction for the whole select, so it must never be returned to the pool
		// here: keep it non-reusable (this also guards the streaming section, which cannot be pooled mid-stream) and
		// let only a successful commit/rollback mark it reusable. Restoring reusability once the select completes
		// would leave the still-open transaction poolable if a later commit/rollback or destroy then failed, leaking
		// an open or aborted session into unrelated work.
		_pTransaction->m_ConnectionCheckout.f_MarkReusable(false);

		NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch> Rows;
		if (bUsePrepared)
		{
			Rows = co_await _pTransaction->m_ConnectionCheckout.m_Client
				(
					&CPostgresClientActor::f_ExecutePreparedRows
					, StatementEntry.m_Name
					, fg_Move(PostgresParameters)
					, nRowsPerBatch
				)
			;
		}
		else
		{
			Rows = co_await _pTransaction->m_ConnectionCheckout.m_Client
				(
					&CPostgresClientActor::f_ExecuteRows
					, fg_Move(Sql)
					, fg_Move(PostgresParameters)
					, nRowsPerBatch
				)
			;
		}

		for (auto iBatch = co_await fg_Move(Rows).f_GetPipelinedIterator(nPipelineLength); iBatch; co_await ++iBatch)
		{
			CSqlRowDataBatch Batch;
			for (auto &Row : *iBatch)
				Batch.f_InsertLast(co_await fg_PostgresMapRow(fg_Move(Row), Mapping));

			co_yield fg_Move(Batch);
		}

		if (bWarmPrepared)
		{
			co_await _pTransaction->fp_WarmPreparedSelect(&Description).f_Wrap()
				> NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to warm transaction prepared select")
			;
		}

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

		auto Sequence = co_await _pTransaction->m_Sequencer.f_Sequence();

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		co_return co_await fg_PostgresExecuteCount(_pTransaction->m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
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

		auto Sequence = co_await _pTransaction->m_Sequencer.f_Sequence();

		auto Values = co_await fg_PostgresValues(fg_Move(Parameters));
		co_return co_await fg_PostgresExecuteExists(_pTransaction->m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
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

		if (m_bReadOnly)
			co_return DMibErrorDatabaseInstance("Cannot execute raw SQL in PostgreSQL read transaction");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		co_return co_await fg_PostgresExecuteAffected(m_ConnectionCheckout.m_Client, fg_Move(Sql), fg_Move(Values));
	}

	NConcurrency::TCFuture<CSqlRawResult> CPostgresTransactionActor::f_QueryRaw(CSqlRawOperation _Operation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (_Operation.m_BackendRequirement == ESqlRawBackend::mc_SQLite)
			co_return DMibErrorDatabaseInstance("SQLite-specific raw SQL operation cannot run on the PostgreSQL backend");
		if (auto pException = fp_CheckNotFinished())
			co_return pException;

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		auto QueryResult = co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteWithParameters, fg_Move(Sql), fg_Move(Values));

		co_return fg_PostgresRawResult(fg_Move(QueryResult));
	}

	static NConcurrency::TCAsyncGenerator<CSqlRawRowBatch> fg_PostgresTransactionRawRowStreamFromIterator
		(
			NConcurrency::CActorSubscription _Sequence
			, NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>::CPipelinedIterator _iItems
		)
	{
		(void)_Sequence;

		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;

		for (; _iItems; co_await ++_iItems)
		{
			CPostgresRowStreamItem Item = fg_Move(*_iItems);
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

		auto nRowsPerBatch = _Operation.m_nRowsPerBatch ? _Operation.m_nRowsPerBatch : m_nSelectRowsPerBatch;
		auto Sequence = co_await m_Sequencer.f_Sequence();
		m_ConnectionCheckout.f_MarkReusable(false);
		auto Sql = fg_Move(_Operation.m_Sql);
		auto Values = co_await fg_PostgresValues(fg_Move(_Operation.m_Parameters));

		auto Items = co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_ExecuteRowsStream, fg_Move(Sql), fg_Move(Values), nRowsPerBatch);
		auto iItems = co_await fg_Move(Items).f_GetPipelinedIterator(m_nPipelineLength);
		if (!iItems)
			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream completed without producing a row description");

		CPostgresRowStreamItem FirstItem = fg_Move(*iItems);
		if (!FirstItem.m_Description)
			co_return DMibErrorDatabaseInstance("PostgreSQL raw stream did not produce a row description before rows");

		co_await ++iItems;

		CSqlRawStream Stream;
		Stream.m_Columns = fg_PostgresRawColumns(*FirstItem.m_Description);
		Stream.m_Rows = fg_PostgresTransactionRawRowStreamFromIterator(fg_Move(Sequence), fg_Move(iItems));

		co_return Stream;
	}

	NConcurrency::TCFuture<NStr::CStr> CPostgresTransactionActor::f_CreateSavepoint()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot create PostgreSQL savepoint after transaction finished");

		auto Sequence = co_await m_Sequencer.f_Sequence();
		auto Name = fg_PostgresSavepointName(++m_iNextSavepoint);
		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("SAVEPOINT ", Name));

		co_return Name;
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_ReleaseSavepoint(NStr::CStr _Name)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot release PostgreSQL savepoint after transaction finished");

		auto Sequence = co_await m_Sequencer.f_Sequence();
		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("RELEASE SAVEPOINT ", _Name));

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_RollbackToSavepoint(NStr::CStr _Name)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (m_bFinished)
			co_return DMibErrorDatabaseInstance("Cannot roll back PostgreSQL savepoint after transaction finished");

		auto Sequence = co_await m_Sequencer.f_Sequence();
		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_Execute, fg_PostgresSavepointSql("ROLLBACK TO SAVEPOINT ", _Name));

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_CommitTransaction()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!m_bFinished)
		{
			m_bFinished = true;
			auto Sequence = co_await m_Sequencer.f_Sequence();

			co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_CommitTransaction);
			m_ConnectionCheckout.f_MarkReusable();
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::f_RollbackTransaction()
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!m_bFinished)
		{
			m_bFinished = true;
			auto Sequence = co_await m_Sequencer.f_Sequence();

			co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_RollbackTransaction);
			m_ConnectionCheckout.f_MarkReusable();
		}

		co_return {};
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_Destroy()
	{
		if (m_ConnectionCheckout.m_Client && !m_bFinished)
		{
			auto RollbackResult = co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_RollbackTransaction).f_Wrap();
			if (RollbackResult)
				m_ConnectionCheckout.f_MarkReusable();
			else
				NConcurrency::fg_LogError("PostgreSQL database backend", "Failed to roll back transaction during destroy")(fg_Move(RollbackResult));

			m_bFinished = true;
		}

		co_return {};
	}

	CPostgresPreparedSelectCacheEntry CPostgresTransactionActor::fp_GetPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		// Resolve the row mapping by the description pointer, never the shared QueryID key (see f_FindByPointer): an
		// aliased expression selection's mapping is not encoded in the SQL/QueryID, so a QueryID match could return a
		// different statement's mapping and decode rows into the wrong result-struct members.
		if (CPostgresPreparedSelectCacheEntry const *pEntry = m_ConnectionCheckout.m_pPreparedSelectCache->f_FindByPointer(_pDescription))
			return *pEntry;

		if (m_pPreparedSelectCache)
		{
			if (CPostgresPreparedSelectCacheEntry const *pEntry = m_pPreparedSelectCache->f_FindByPointer(_pDescription))
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

	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedSelect(CSqlSelectOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!m_Backend)
			co_return {};

		auto Entry = co_await m_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedSelect, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (m_ConnectionCheckout.m_pPreparedSelectCache->f_Find(_pDescription))
			co_return {};

		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_PostgresParameterTypes(_pDescription->m_ParameterTypes));

		// fp_EnsurePreparedSelect resolves by QueryID, so for an aliased-expression select it may return another
		// description's entry that shares this SQL/QueryID but maps rows into different result members. The server-side
		// statement (name/SQL) is shared and was just prepared from it, but this connection's per-pointer cache entry
		// must carry THIS description's row mapping - fp_GetPreparedSelect resolves it by pointer (building it from the
		// live statement here, since neither cache holds this pointer yet) - or a later f_FindByPointer on this
		// connection would decode rows with the other description's mapping.
		auto LocalEntry = fp_GetPreparedSelect(_pDescription);
		m_ConnectionCheckout.m_pPreparedSelectCache->f_Insert(_pDescription, fg_Move(LocalEntry));

		co_return {};
	}

	CPostgresPreparedInsertCacheEntry CPostgresTransactionActor::fp_GetPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedInsertCacheEntry const *pEntry = m_ConnectionCheckout.m_pPreparedInsertCache->f_Find(_pDescription))
			return *pEntry;

		if (m_pPreparedInsertCache)
		{
			if (CPostgresPreparedInsertCacheEntry const *pEntry = m_pPreparedInsertCache->f_Find(_pDescription))
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

	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedInsert(CSqlInsertOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!m_Backend)
			co_return {};

		auto Entry = co_await m_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedInsert, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (m_ConnectionCheckout.m_pPreparedInsertCache->f_Find(_pDescription))
			co_return {};

		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_PostgresValueTypes(Entry.m_Description.m_InsertColumnTypes));

		m_ConnectionCheckout.m_pPreparedInsertCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}

	CPostgresPreparedUpdateCacheEntry CPostgresTransactionActor::fp_GetPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpdateCacheEntry const *pEntry = m_ConnectionCheckout.m_pPreparedUpdateCache->f_Find(_pDescription))
			return *pEntry;

		if (m_pPreparedUpdateCache)
		{
			if (CPostgresPreparedUpdateCacheEntry const *pEntry = m_pPreparedUpdateCache->f_Find(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedUpdateCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresUpdateSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedUpdateName(_pDescription->m_QueryID);

		return Entry;
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedUpdate(CSqlUpdateOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!m_Backend)
			co_return {};

		auto Entry = co_await m_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedUpdate, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (m_ConnectionCheckout.m_pPreparedUpdateCache->f_Find(_pDescription))
			co_return {};

		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, NContainer::TCVector<EPostgresValueType>());

		m_ConnectionCheckout.m_pPreparedUpdateCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}

	CPostgresPreparedDeleteCacheEntry CPostgresTransactionActor::fp_GetPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedDeleteCacheEntry const *pEntry = m_ConnectionCheckout.m_pPreparedDeleteCache->f_Find(_pDescription))
			return *pEntry;

		if (m_pPreparedDeleteCache)
		{
			if (CPostgresPreparedDeleteCacheEntry const *pEntry = m_pPreparedDeleteCache->f_Find(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedDeleteCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresDeleteSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedDeleteName(_pDescription->m_QueryID);

		return Entry;
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedDelete(CSqlDeleteOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!m_Backend)
			co_return {};

		auto Entry = co_await m_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedDelete, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (m_ConnectionCheckout.m_pPreparedDeleteCache->f_Find(_pDescription))
			co_return {};

		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, NContainer::TCVector<EPostgresValueType>());

		m_ConnectionCheckout.m_pPreparedDeleteCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}

	CPostgresPreparedUpsertCacheEntry CPostgresTransactionActor::fp_GetPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		if (CPostgresPreparedUpsertCacheEntry const *pEntry = m_ConnectionCheckout.m_pPreparedUpsertCache->f_Find(_pDescription))
			return *pEntry;

		if (m_pPreparedUpsertCache)
		{
			if (CPostgresPreparedUpsertCacheEntry const *pEntry = m_pPreparedUpsertCache->f_Find(_pDescription))
				return *pEntry;
		}

		CPostgresPreparedUpsertCacheEntry Entry;
		Entry.m_QueryID = _pDescription->m_QueryID;
		Entry.m_Description = _pDescription->m_pStatement->f_Describe();
		Entry.m_Sql = fg_PostgresUpsertSql(Entry.m_Description);
		Entry.m_Name = fg_PostgresPreparedUpsertName(_pDescription->m_QueryID);

		return Entry;
	}

	NConcurrency::TCFuture<void> CPostgresTransactionActor::fp_WarmPreparedUpsert(CSqlUpsertOperationDescription const *_pDescription)
	{
		auto CheckDestroy = co_await f_CheckDestroyedOnResume();

		if (!m_Backend)
			co_return {};

		auto Entry = co_await m_Backend(&CPostgresDatabaseBackendActor::fp_EnsurePreparedUpsert, _pDescription);
		if (Entry.m_Name.f_IsEmpty())
			co_return {};

		if (m_ConnectionCheckout.m_pPreparedUpsertCache->f_Find(_pDescription))
			co_return {};

		co_await m_ConnectionCheckout.m_Client(&CPostgresClientActor::f_PrepareStatement, Entry.m_Name, Entry.m_Sql, fg_PostgresValueTypes(Entry.m_Description.m_InsertColumnTypes));

		m_ConnectionCheckout.m_pPreparedUpsertCache->f_Insert(_pDescription, fg_Move(Entry));

		co_return {};
	}
}
