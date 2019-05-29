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
		m_Results.f_DeleteAll();
	}

	CQueryResult *CTransactionResult::f_GetQueryResult(aint _iQuery)
	{
		if (_iQuery < 0 || (mint)_iQuery >= m_Results.f_GetLen())
			return nullptr;
		return m_Results[_iQuery];
	}


	CTransaction::CQueryLink::CQueryLink()
	{
		m_pQueryInstance = nullptr;
	}

	CTransaction::CQueryLink::~CQueryLink()
	{
		if (m_pQueryInstance)
			m_pQueryInstance->f_Delete();
	}

	CTransactionHandler::~CTransactionHandler()
	{
	}

	void CTransactionHandler::fs_HandleTransaction(CTransactionResult *_pResult, void *_pContext)
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
		if (m_pResult)
			delete m_pResult;
	}

	void CSQLConnection::fp_QueueTask(CTask *_pTask)
	{
		DMibLockTyped(NThread::CMutual, mp_QueueLock);
		mp_QueuedTasks.f_Insert(_pTask);
		if (_pTask->m_pEvent)
			_pTask->m_pEvent->f_Signal();

	}

	CTransactionResult *CSQLConnection::fp_CommitTransaction(CTransaction *_pTransaction, CDatabaseImplementation *_pImp)
	{
		_pImp->f_BeginTransaction();

		CTransactionResult *pResult = DMibNew CTransactionResult;
		pResult->m_Results.f_SetLen(_pTransaction->m_nTransactions);
		NMemory::fg_ObjectSet(pResult->m_Results.f_GetArray(), (CQueryResult *)nullptr, _pTransaction->m_nTransactions);
		int iResult = 0;
		CTransaction::CQueryIter Iter = _pTransaction->m_Transactions;

		while (Iter)
		{
			CQueryResult * pRes = _pImp->f_RunQuery(Iter->m_pQueryInstance);
			if (!pRes && !_pTransaction->m_bAllowFail)
			{
				delete pResult;
				_pImp->f_RollbackTransaction();
				return nullptr;
			}
			pResult->m_Results[iResult++] = pRes;

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
		m_pTask = nullptr;
	}

	CSQLConnection::CWorkerThread::~CWorkerThread()
	{
		f_Stop();
	}

	aint CSQLConnection::CWorkerThread::f_Main()
	{
		m_EventWantQuit.f_ReportTo(&m_Event);
		auto pImp = NMib::fg_CreateRuntimeType<CDatabaseImplementation>(m_pSQLConnection->mp_Implementation);

		if (pImp)
		{
			if (pImp->f_Create(m_pSQLConnection->mp_Parameters))
			{
				while (f_GetState() != NThread::EThreadState_EventWantQuit)
				{
					if (m_pTask)
					{
						m_pTask->f_Run(pImp.f_Get());
						m_pTask = nullptr;
						{
							DMibLock(m_pSQLConnection->mp_ThreadsLock);
							m_pSQLConnection->mp_FreeThreads.f_Insert(this);
							m_pSQLConnection->mp_ThreadEvent.f_Signal();
						}
					}
					m_Event.f_Wait();
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
		CTransactionResult *pResult = m_pSQL->fp_CommitTransaction(m_pTransaction, _pImp);
		if (m_bAsync)
		{
			m_fCallback(pResult, m_pContext);
			delete pResult;
			delete this;
			return;
		}
		m_pResult = pResult;
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


	CQueryResult *CSQLConnection::f_ExecuteQuery(const NStr::CStr &_Query, bool _bTransaction)
	{
		CQuery *pQuery = f_CreateQuery(_Query);
		if (!pQuery)
			return nullptr;
		CQueryInstance *pInst = pQuery->f_CreateQueryInstance();
		if (!pInst)
		{
			pQuery->f_Delete();
			return nullptr;
		}
		if (_bTransaction)
			mp_pMainImp->f_BeginTransaction();
		CQueryResult *pRes = mp_pMainImp->f_RunQuery(pInst);
		pInst->f_Delete();
		pQuery->f_Delete();
		if (!pRes)
		{
			if (_bTransaction)
				mp_pMainImp->f_RollbackTransaction();
			return nullptr;
		}
		if (_bTransaction)
			mp_pMainImp->f_CommitTransaction();
		return pRes;
	}

	CQueryResult *CSQLConnection::f_ExecuteQuery(CQuery *_pQuery)
	{
		TCUniquePointer<CQueryInstance>pInst = fg_Explicit(_pQuery->f_CreateQueryInstance());
		if (!pInst)
			return nullptr;

		mp_pMainImp->f_BeginTransaction();
		CQueryResult *pRes = mp_pMainImp->f_RunQuery(pInst.f_Get());
		pInst = 0;

		if (!pRes)
		{
			mp_pMainImp->f_RollbackTransaction();
			return nullptr;
		}

		mp_pMainImp->f_CommitTransaction();
		return pRes;
	}


	CQueryResult *CSQLConnection::f_ExecuteQuery(CQueryInstance *_pQueryInst)
	{
		mp_pMainImp->f_BeginTransaction();
		CQueryResult *pRes = mp_pMainImp->f_RunQuery(_pQueryInst);
		if (!pRes)
		{
			mp_pMainImp->f_RollbackTransaction();
			return nullptr;
		}
		mp_pMainImp->f_CommitTransaction();
		return pRes;
	}

	CTransactionResult *CSQLConnection::f_CommitTransaction(CTransaction *_pTransaction)
	{
		return fp_CommitTransaction(_pTransaction, mp_pMainImp.f_Get());
	}

	// Async transactions
	void CSQLConnection::f_CommitTransaction(CTransaction *_pTransaction, void * _pContext, NThread::CSemaphoreReportableAggregate *_pEvent, PFTransactionResultCallback *_fCallback, bool _bAsync)
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
		pTask->m_pTransaction = _pTransaction;
		pTask->m_pContext = _pContext;
		pTask->m_pEvent = _pEvent;
		pTask->m_fCallback = _fCallback;
		pTask->m_bAsync = _bAsync;
		pThread->m_pTask = pTask;
		pThread->m_Event.f_Signal();
	}

	void CSQLConnection::f_CommitTransaction(CTransaction *_pTransaction, CTransactionHandler *_pHandler, NThread::CSemaphoreReportableAggregate *_pEvent, bool _bAsync)
	{
		f_CommitTransaction(_pTransaction, _pHandler, _pEvent, CTransactionHandler::fs_HandleTransaction, _bAsync);
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

	CTransaction *CSQLConnection::f_CreateTransaction()
	{
		return DMibNew CTransaction;

	}

	CTransaction *CSQLConnection::f_CreateTransaction(CStr const& _Statements)	// Create a transaction from a list of semi-colon separated statements.
	{
		TCUniquePointer<CTransaction> pTransaction = fg_Construct();

		CStr Statements = _Statements;
		CStr CurStatement;

		TCUniquePointer<CQuery> pQuery;

		while (!Statements.f_IsEmpty())
		{
			CurStatement = fg_GetStrSep(Statements, ";");
			CurStatement = CurStatement.f_Trim();

			pQuery = fg_Explicit(f_CreateQuery(CurStatement));
			if (!pQuery)
				return nullptr;

			pTransaction->f_AddQuery(pQuery.f_Get());
			pTransaction->m_OwnedQueries.f_Insert() = fg_Explicit(pQuery.f_Detach());
		}

		return pTransaction.f_Detach();
	}

	CQuery *CSQLConnection::f_CreateQuery(const NStr::CStr &_Query)
	{
		return mp_pMainImp->f_CreateQuery(_Query);
	}

	CQueryInstance *CSQLConnection::f_CreateQueryInstance(CQuery* _pQuery)
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

	CQueryInstance *CTransaction::f_AddQuery(CQuery *_pQuery)
	{
		++m_nTransactions;
		CQueryLink *pLink = DMibNew CQueryLink;
		pLink->m_pQueryInstance = _pQuery->f_CreateQueryInstance();
		m_Transactions.f_Insert(pLink);
		return pLink->m_pQueryInstance;
	}

	void CTransaction::f_Delete()
	{
		delete this;
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
	void CSQLConnection::f_CommitTransaction(CTransaction *_pTransaction, void * _pContext, NThread::CSemaphoreReportableAggregate *_pEvent, PFTransactionResultCallback *_fCallback);
	CTransactionResult *CSQLConnection::f_GetTransacitonResult(void * &_pContext);
*/
}
