// Copyright © 2015 Hansoft AB
// Distributed under the MIT license, see license text in LICENSE.Malterlib

#include "Malterlib_SQL.h"
#include <Mib/Core/RuntimeType>

using namespace NMib::NStr;
using namespace NMib::NStorage;

namespace NMib::NSQL
{
	DMibImpErrorClassImplement(CExceptionDatabase);

	CQuery::CQuery()
	{
	}

	CQuery::~CQuery()
	{
	}

	bool CQueryInstance::f_BindParameterNull(int _iParam)
	{
		return f_BindParameter(_iParam, 0, 0);
	}

	CTransactionResult::~CTransactionResult()
	{
		m_Results.f_Clear();
	}

	CQueryResult *CTransactionResult::f_GetQueryResult(aint _iQuery)
	{
		if (_iQuery < 0 || (mint)_iQuery >= m_Results.f_GetLen())
			return nullptr;
		return m_Results[_iQuery].f_Get();
	}

	CTransactionHandler::~CTransactionHandler()
	{
	}

	void CTransactionHandler::fs_HandleTransaction(NStorage::TCUniquePointer<CTransactionResult> const &_pResult, void *_pContext)
	{
		((CTransactionHandler *)_pContext)->f_HandleTransaction(_pResult);
	}


	CSQLConnection::CTask::CTask(CSQLConnection *_pSQL)
	{
		m_pSQL = _pSQL;
		m_pTransaction = nullptr;
		m_pContext = nullptr;
		m_pEvent = nullptr;
		m_fCallback = nullptr;
		m_bAsync = false;
		m_pResult = nullptr;
	}

	CSQLConnection::CTask::~CTask()
	{
	}

	void CSQLConnection::fp_QueueTask(CTask *_pTask)
	{
		DMibLockTyped(NThread::CMutual, mp_QueueLock);
		mp_QueuedTasks.f_Insert(_pTask);
		if (_pTask->m_pEvent)
			_pTask->m_pEvent->f_Signal();

	}

	NStorage::TCUniquePointer<CTransactionResult> CSQLConnection::fp_CommitTransaction(NStorage::TCUniquePointer<CTransaction> &&_pTransaction, CDatabaseImplementation *_pImp)
	{
		_pImp->f_BeginTransaction();

		auto pTransaction = fg_Move(_pTransaction);

		NStorage::TCUniquePointer<CTransactionResult> pResult = fg_Construct();
		pResult->m_Results.f_SetLen(pTransaction->m_nTransactions);
		int iResult = 0;
		CTransaction::CQueryIter Iter = pTransaction->m_Transactions;

		while (Iter)
		{
			auto pRes = _pImp->f_RunQuery(Iter->m_pQueryInstance);
			if (!pRes && !pTransaction->m_bAllowFail)
			{
				_pImp->f_RollbackTransaction();
				return nullptr;
			}
			pResult->m_Results[iResult++] = fg_Move(pRes);

			++Iter;
		}

		_pImp->f_CommitTransaction();

		return pResult;
	}

	NStr::CStr CSQLConnection::CWorkerThread::f_GetThreadName()
	{
		return "Malterlib_DBConnWorkerThread";
	}

	CSQLConnection::CWorkerThread::CWorkerThread(CSQLConnection *_pSQL)
	{
		m_pSQLConnection = _pSQL;
		f_Start();
	}

	CSQLConnection::CWorkerThread::~CWorkerThread()
	{
		f_Stop();
	}

	aint CSQLConnection::CWorkerThread::f_Main()
	{
		auto pImp = NMib::fg_CreateRuntimeType<CDatabaseImplementation>(m_pSQLConnection->mp_Implementation);

		if (pImp)
		{
			if (pImp->f_Create(m_pSQLConnection->mp_Parameters))
			{
				while (f_GetState() != NThread::EThreadState_EventWantQuit)
				{
					if (m_pTask)
					{
						fg_Exchange(m_pTask, nullptr)->f_Run(pImp.f_Get());
						{
							DMibLock(m_pSQLConnection->mp_ThreadsLock);
							m_pSQLConnection->mp_FreeThreads.f_Insert(this);
							m_pSQLConnection->mp_ThreadEvent.f_Signal();
						}
					}
					m_EventWantQuit.f_Wait();
				}
			}

			pImp->f_Destroy(true);
			pImp.f_Clear();
		}

		{
			DMibLock(m_pSQLConnection->mp_ThreadsLock);
			m_pSQLConnection->mp_DeletedThreads.f_Insert(this);
			m_pSQLConnection->mp_ThreadEvent.f_Signal();
		}

		return 0;
	}

	CSQLConnection::CSQLConnection()
	{
		mp_pMainImp = nullptr;
	}

	CSQLConnection::~CSQLConnection()
	{
		f_Close();
	}

	void CSQLConnection::CTask::f_Run(CDatabaseImplementation *_pImp)
	{
		auto pResult = m_pSQL->fp_CommitTransaction(fg_Move(m_pTransaction), _pImp);
		if (m_bAsync)
		{
			m_fCallback(pResult, m_pContext);
			delete this;
			return;
		}
		m_pResult = fg_Move(pResult);
		m_pSQL->fp_QueueTask(this);
		//_pImp->f_CreateQuery()
	}

	bool CSQLConnection::f_Create(const ch8 *_pImplementation, const NContainer::CRegistry &_Parameters, mint _nWorkerThreads)
	{
		if (mp_pMainImp)
		{
			mp_pMainImp->f_Destroy(false);
			mp_pMainImp.f_Clear();
		}
		mp_Implementation = _pImplementation;
		mp_Parameters = _Parameters;

		mp_pMainImp = NMib::fg_CreateRuntimeType<CDatabaseImplementation>(mp_Implementation);
		if (!mp_pMainImp)
			DMibErrorDatabase(NStr::CStrNonTracked::CFormat("Could not create database implementation {}") << mp_Implementation);

		if (!mp_pMainImp->f_Create(mp_Parameters))
			return false;

		for (mint i = 0; i < _nWorkerThreads; ++i)
		{
			CWorkerThread *pThread = DMibNew CWorkerThread(this);
			mp_FreeThreads.f_Insert(pThread);
		}
		return true;
	}

	void CSQLConnection::f_Close()
	{
		{
			DMibLock(mp_ThreadsLock);
			while (mp_WorkingThreads.f_GetFirst())
			{
				CWorkerThread *pThread = mp_WorkingThreads.f_GetFirst();

				{
					DMibUnlock(mp_ThreadsLock);
					pThread->f_Stop();
				}
			}
			while (mp_FreeThreads.f_GetFirst())
			{
				CWorkerThread *pThread = mp_FreeThreads.f_GetFirst();

				{
					DMibUnlock(mp_ThreadsLock);
					pThread->f_Stop();
				}
			}
		}
		mp_DeletedThreads.f_DeleteAll();

		if (mp_pMainImp)
		{
			mp_pMainImp->f_Destroy(false);
			mp_pMainImp.f_Clear();
		}
	}


	NStorage::TCUniquePointer<CQueryResult> CSQLConnection::f_ExecuteQuery(NStorage::TCUniquePointer<CQueryInstance> const &_pQueryInst)
	{
		mp_pMainImp->f_BeginTransaction();
		auto pRes = mp_pMainImp->f_RunQuery(_pQueryInst);
		if (!pRes)
		{
			mp_pMainImp->f_RollbackTransaction();
			return nullptr;
		}
		mp_pMainImp->f_CommitTransaction();
		return pRes;
	}

	NStorage::TCUniquePointer<CTransactionResult> CSQLConnection::f_CommitTransaction(NStorage::TCUniquePointer<CTransaction> &&_pTransaction)
	{
		return fp_CommitTransaction(fg_Move(_pTransaction), mp_pMainImp.f_Get());
	}

	// Async transactions
	void CSQLConnection::f_CommitTransaction
		(
			NStorage::TCUniquePointer<CTransaction> &&_pTransaction
			, void * _pContext
			, NThread::CSemaphoreAggregate *_pEvent
			, FTransactionResultCallback *_fCallback
			, bool _bAsync
		)
	{
		CWorkerThread *pThread = nullptr;
		while (!pThread)
		{
			{
				DMibLock(mp_ThreadsLock);
				pThread = mp_FreeThreads.f_Pop();
				if (pThread)
					mp_WorkingThreads.f_Insert(pThread);
			}
			if (!pThread)
				mp_ThreadEvent.f_Wait();
		}

		CTask *pTask = DMibNew CTask(this);
		pTask->m_pTransaction = fg_Move(_pTransaction);
		pTask->m_pContext = _pContext;
		pTask->m_pEvent = _pEvent;
		pTask->m_fCallback = _fCallback;
		pTask->m_bAsync = _bAsync;
		pThread->m_pTask = pTask;
		pThread->m_EventWantQuit.f_Signal();
	}

	void CSQLConnection::f_CommitTransaction(NStorage::TCUniquePointer<CTransaction> &&_pTransaction, CTransactionHandler *_pHandler, NThread::CSemaphoreAggregate *_pEvent, bool _bAsync)
	{
		f_CommitTransaction(fg_Move(_pTransaction), _pHandler, _pEvent, &CTransactionHandler::fs_HandleTransaction, _bAsync);
	}

	void CSQLConnection::f_ProcessTansactions()
	{
		int nTasks = 0;
		{
			DMibLockTyped(NThread::CMutual, mp_QueueLock);
			nTasks = mp_QueuedTasks.f_GetLen();
		}

		while (nTasks)
		{
			CTask *pTask;
			{
				DMibLockTyped(NThread::CMutual, mp_QueueLock);
				pTask = mp_QueuedTasks.f_Pop();
			}

			if (!pTask)
				break;

			pTask->m_fCallback(pTask->m_pResult, pTask->m_pContext);
			delete pTask;
		}
	}

	NStorage::TCUniquePointer<CTransaction> CSQLConnection::f_CreateTransaction()
	{
		return fg_Construct();
	}

	NStorage::TCUniquePointer<CTransaction> CSQLConnection::f_CreateTransaction(CStr const& _Statements)	// Create a transaction from a list of semi-colon separated statements.
	{
		TCUniquePointer<CTransaction> pTransaction = fg_Construct();

		CStr Statements = _Statements;
		CStr CurStatement;

		while (!Statements.f_IsEmpty())
		{
			CurStatement = fg_GetStrSep(Statements, ";");
			CurStatement = CurStatement.f_Trim();

			auto pQuery = f_CreateQuery(CurStatement);
			if (!pQuery)
				return nullptr;

			pTransaction->f_AddQuery(pQuery);
			pTransaction->m_OwnedQueries.f_Insert(fg_Move(pQuery));
		}

		return pTransaction;
	}

	NStorage::TCUniquePointer<CQuery> CSQLConnection::f_CreateQuery(const NStr::CStr &_Query)
	{
		return mp_pMainImp->f_CreateQuery(_Query);
	}

	NStorage::TCUniquePointer<CQueryInstance> CSQLConnection::f_CreateQueryInstance(NStorage::TCUniquePointer<CQuery> const &_pQuery)
	{
		return _pQuery->f_CreateQueryInstance();
	}

	CTransaction::CTransaction()
	{
		m_nTransactions = 0;
		m_bAllowFail = false;
	}

	CTransaction::~CTransaction()
	{
		m_Transactions.f_DeleteAll();
	}

	NStorage::TCUniquePointer<CQueryInstance> const &CTransaction::f_AddQuery(NStorage::TCUniquePointer<CQuery> const &_pQuery)
	{
		++m_nTransactions;
		CQueryLink *pLink = DMibNew CQueryLink;
		pLink->m_pQueryInstance = _pQuery->f_CreateQueryInstance();
		m_Transactions.f_Insert(pLink);
		return pLink->m_pQueryInstance;
	}

	void CTransaction::f_SetAllowFail(bool _bAllow)
	{
		m_bAllowFail = _bAllow;
	}

//		DMibRuntimeClassBaseNamed(CDatabaseImplementation, NMib::NSQL::CDatabaseImplementation);

	/*
	CQuery *CSQLConnection::f_CreateQuery(const NStr::CStr &_Query);
	CTransaction *CSQLConnection::f_CreateTransaciton();
	CQueryInstance *CSQLConnection::f_CreateQueryInstance(CQuery *_pQuery);

	// Async transactions
	void CSQLConnection::f_CommitTransaction(CTransaction *_pTransaction, void * _pContext, NThread::CSemaphoreAggregate *_pEvent, FTransactionResultCallback *_fCallback);
	CTransactionResult *CSQLConnection::f_GetTransacitonResult(void * &_pContext);
*/
}
