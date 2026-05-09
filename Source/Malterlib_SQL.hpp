// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL
{
	template <typename tf_CStream>
	void CSqlErrorData::f_Stream(tf_CStream &_Stream)
	{
		_Stream % m_Category;
		_Stream % m_RetryClass;
		_Stream % m_Backend;
		_Stream % m_BackendCode;
		_Stream % m_BackendMessage;
		_Stream % m_Detail;
		_Stream % m_Hint;
	}

	template <typename ...tfp_CParam>
	NStorage::TCUniquePointer<CQueryResult> CSQLConnection::f_ExecuteBindWithoutTransaction(const NStr::CStr &_Query, tfp_CParam const &...p_Params)
	{
		NStorage::TCUniquePointer<CQuery> pQuery = f_CreateQuery(_Query);
		if (!pQuery)
			return nullptr;

		NStorage::TCUniquePointer<CQueryInstance> pInst = pQuery->f_CreateQueryInstance();
		if (!pInst)
			return nullptr;

		umint iParam = 0;
		bool bError = false;
		(
			[&]
			{
				if (bError)
					return;

				if (!pInst->f_BindParameter(iParam, p_Params))
					bError = true;

				++iParam;
			}
			()
			, ...
		);

		if (bError)
			return nullptr;

		return mp_pMainImp->f_RunQuery(pInst);
	}

	template <typename ...tfp_CParam>
	NStorage::TCUniquePointer<CQueryResult> CSQLConnection::f_ExecuteBind(const NStr::CStr &_Query, tfp_CParam const &...p_Params)
	{
		NStorage::TCUniquePointer<CQuery> pQuery = f_CreateQuery(_Query);
		if (!pQuery)
			return nullptr;

		NStorage::TCUniquePointer<CQueryInstance> pInst = pQuery->f_CreateQueryInstance();
		if (!pInst)
			return nullptr;

		umint iParam = 0;
		bool bError = false;
		(
			[&]
			{
				if (bError)
					return;

				if (!pInst->f_BindParameter(iParam, p_Params))
					bError = true;

				++iParam;
			}
			()
			, ...
		);

		if (bError)
			return nullptr;

		mp_pMainImp->f_BeginTransaction();

		auto pRes = mp_pMainImp->f_RunQuery(pInst);
		if (!pRes)
		{
			mp_pMainImp->f_RollbackTransaction();

			return nullptr;
		}

		mp_pMainImp->f_CommitTransaction();

		return pRes;
	}

	template <typename ...tfp_CParam>
	NStorage::TCUniquePointer<CQueryResult> CSQLConnection::f_ExecuteBind(NStorage::TCUniquePointer<CQuery> const &_pQuery, tfp_CParam const &...p_Params)
	{
		NStorage::TCUniquePointer<CQueryInstance> pInst = _pQuery->f_CreateQueryInstance();
		if (!pInst)
			return nullptr;

		umint iParam = 0;
		bool bError = false;
		(
			[&]
			{
				if (bError)
					return;

				if (!pInst->f_BindParameter(iParam, p_Params))
					bError = true;

				++iParam;
			}
			()
			, ...
		);

		if (bError)
			return nullptr;

		mp_pMainImp->f_BeginTransaction();

		auto pRes = mp_pMainImp->f_RunQuery(pInst);
		if (!pRes)
		{
			mp_pMainImp->f_RollbackTransaction();

			return nullptr;
		}

		mp_pMainImp->f_CommitTransaction();

		return pRes;
	}

}
