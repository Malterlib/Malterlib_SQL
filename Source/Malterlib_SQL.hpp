// Copyright © 2024 Favro Holding AB
// Distributed under the MIT license, see license text in LICENSE.Malterlib

#pragma once

namespace NMib::NSQL
{
	template <typename ...tfp_CParam>
	NStorage::TCUniquePointer<CQueryResult> CSQLConnection::f_ExecuteBind(const NStr::CStr &_Query, tfp_CParam const &...p_Params)
	{
		NStorage::TCUniquePointer<CQuery> pQuery = f_CreateQuery(_Query);
		if (!pQuery)
			return nullptr;
 
		NStorage::TCUniquePointer<CQueryInstance> pInst = pQuery->f_CreateQueryInstance();
		if (!pInst)
			return nullptr;

		mint iParam = 0;
		bool bError = false;
		TCInitializerList<bool> Dummy =
			{
				[&]
				{
					if (bError)
						return false;

					if (!pInst->f_BindParameter(iParam, p_Params))
						bError = true;

					++iParam;

					return false;
				}
				()...
			}
		;

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
	NStorage::TCUniquePointer<CQueryResult> CSQLConnection::f_ExecuteBindWithoutTransaction(const NStr::CStr &_Query, tfp_CParam const &...p_Params)
	{
		NStorage::TCUniquePointer<CQuery> pQuery = f_CreateQuery(_Query);
		if (!pQuery)
			return nullptr;
 
		NStorage::TCUniquePointer<CQueryInstance> pInst = pQuery->f_CreateQueryInstance();
		if (!pInst)
			return nullptr;

		mint iParam = 0;
		bool bError = false;
		TCInitializerList<bool> Dummy =
			{
				[&]
				{
					if (bError)
						return false;

					if (!pInst->f_BindParameter(iParam, p_Params))
						bError = true;

					++iParam;

					return false;
				}
				()...
			}
		;

		if (bError)
			return nullptr;

		return mp_pMainImp->f_RunQuery(pInst);
	}
}
