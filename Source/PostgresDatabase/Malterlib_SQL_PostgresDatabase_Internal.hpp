// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NPrivate
{
	inline CPostgresPreparedCacheEntryBase::CPostgresPreparedCacheEntryBase(CPostgresPreparedCacheEntryBase const &_Other)
		: m_QueryID(_Other.m_QueryID)
		, m_Sql(_Other.m_Sql)
		, m_Name(_Other.m_Name)
	{
	}

	inline CPostgresPreparedCacheEntryBase &CPostgresPreparedCacheEntryBase::operator = (CPostgresPreparedCacheEntryBase const &_Other)
	{
		m_QueryID = _Other.m_QueryID;
		m_Sql = _Other.m_Sql;
		m_Name = _Other.m_Name;

		return *this;
	}

	inline CSqlQueryID CPostgresPreparedCacheQueryIDCompare::operator () (CPostgresPreparedCacheEntryBase const &_Node) const
	{
		return _Node.m_QueryID;
	}

	template <typename t_CEntry, typename t_CDescription>
	TCPostgresPreparedCache<t_CEntry, t_CDescription>::TCPostgresPreparedCache(TCPostgresPreparedCache const &_Other)
		: m_ByPointer(_Other.m_ByPointer)
	{
		for (auto iEntry = m_ByPointer.f_Entries().f_GetIterator(); iEntry; ++iEntry)
		{
			if (!m_ByQueryID.f_FindEqual(iEntry->f_Value().m_QueryID))
				m_ByQueryID.f_Insert(static_cast<CPostgresPreparedCacheEntryBase &>(iEntry->f_Value()));
		}
	}

	template <typename t_CEntry, typename t_CDescription>
	TCPostgresPreparedCache<t_CEntry, t_CDescription>::~TCPostgresPreparedCache()
	{
		m_ByQueryID.f_RemoveAll();
	}

	template <typename t_CEntry, typename t_CDescription>
	auto TCPostgresPreparedCache<t_CEntry, t_CDescription>::operator = (TCPostgresPreparedCache const &_Other) -> TCPostgresPreparedCache &
	{
		if (this == &_Other)
			return *this;

		f_Clear();
		m_ByPointer = _Other.m_ByPointer;
		for (auto iEntry = m_ByPointer.f_Entries().f_GetIterator(); iEntry; ++iEntry)
		{
			if (!m_ByQueryID.f_FindEqual(iEntry->f_Value().m_QueryID))
				m_ByQueryID.f_Insert(static_cast<CPostgresPreparedCacheEntryBase &>(iEntry->f_Value()));
		}

		return *this;
	}

	// The QueryID fallback can return the entry of another description with the same SQL, whose row mapping may differ
	template <typename t_CEntry, typename t_CDescription>
	auto TCPostgresPreparedCache<t_CEntry, t_CDescription>::f_Find(CDescription const *_pDescription) const -> CEntry const *
	{
		if (auto const *pEntry = m_ByPointer.f_FindEqual(_pDescription))
			return pEntry;

		if (auto *pBase = m_ByQueryID.f_FindEqual(_pDescription->m_QueryID))
			return static_cast<CEntry const *>(pBase);

		return nullptr;
	}

	// Identical SQL can decode into different result members, so row mappings belong to the exact description
	template <typename t_CEntry, typename t_CDescription>
	auto TCPostgresPreparedCache<t_CEntry, t_CDescription>::f_FindByPointer(CDescription const *_pDescription) const -> CEntry const *
	{
		return m_ByPointer.f_FindEqual(_pDescription);
	}

	template <typename t_CEntry, typename t_CDescription>
	auto TCPostgresPreparedCache<t_CEntry, t_CDescription>::f_Insert(CDescription const *_pDescription, CEntry const &_Entry) -> CEntry &
	{
		fp_CheckCollision(_pDescription, _Entry);
		m_ByPointer[_pDescription] = _Entry;
		auto *pEntry = m_ByPointer.f_FindEqual(_pDescription);
		// Descriptions with the same QueryID share one statement, which the first of them stands for
		if (!m_ByQueryID.f_FindEqual(_Entry.m_QueryID))
			m_ByQueryID.f_Insert(static_cast<CPostgresPreparedCacheEntryBase &>(*pEntry));

		return *pEntry;
	}

	template <typename t_CEntry, typename t_CDescription>
	void TCPostgresPreparedCache<t_CEntry, t_CDescription>::f_Clear()
	{
		m_ByQueryID.f_RemoveAll();
		m_ByPointer.f_Clear();
	}

	template <typename t_CEntry, typename t_CDescription>
	bool TCPostgresPreparedCache<t_CEntry, t_CDescription>::f_IsEmpty() const
	{
		return m_ByPointer.f_IsEmpty();
	}

	template <typename t_CEntry, typename t_CDescription>
	void TCPostgresPreparedCache<t_CEntry, t_CDescription>::fp_CheckCollision(CDescription const *_pDescription, CEntry const &_Entry) const
	{
#if defined DMibContractConfigure_CheckEnabled
		if (auto *pBase = m_ByQueryID.f_FindEqual(_pDescription->m_QueryID))
			DMibCheck(pBase->m_Name == _Entry.m_Name);
#endif
	}
}
