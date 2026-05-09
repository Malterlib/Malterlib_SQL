// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	// Runs _fFunction on another actor, since the test thread makes the coroutines it creates capture exceptions, which would hide code
	// under test whose coroutines do not capture their own.
	template <typename tf_FFunction>
	auto fg_CallOnOtherActor(tf_FFunction _fFunction) -> decltype(_fFunction())
	{
		return NConcurrency::g_Dispatch(NConcurrency::fg_ConcurrentActor()) / fg_Move(_fFunction);
	}

	constexpr CAccountID::CAccountID(uint64 _Value)
		: m_Value(_Value)
	{
	}

	constexpr CAccountID::operator uint64 () const
	{
		return m_Value;
	}
}

namespace NMib::NSQL
{
	constexpr auto TCSqlTypeTraits<NTest::NDatabaseBackend::CStorageOnlyID>::fs_ToSqlStorage(NTest::NDatabaseBackend::CStorageOnlyID const &_Value) -> CSqlType
	{
		return _Value.m_Value;
	}

	constexpr NTest::NDatabaseBackend::CStorageOnlyID TCSqlTypeTraits<NTest::NDatabaseBackend::CStorageOnlyID>::fs_FromSqlStorage(CSqlType _Value)
	{
		NTest::NDatabaseBackend::CStorageOnlyID Value;
		Value.m_Value = _Value;

		return Value;
	}

	inline auto TCSqlTypeTraits<NTest::NDatabaseBackend::CCheckedValue>::fs_ToSqlStorage(NTest::NDatabaseBackend::CCheckedValue const &_Value) -> CSqlType
	{
		if (_Value.m_Value < 0 || _Value.m_Value > 1'000'000)
			DMibError("Checked value is out of range");

		return _Value.m_Value;
	}

	inline auto TCSqlTypeTraits<NTest::NDatabaseBackend::CCheckedValue>::fs_FromSqlStorage(CSqlType _Value) -> NTest::NDatabaseBackend::CCheckedValue
	{
		if (_Value < 0 || _Value > 1'000'000)
			DMibError("Stored checked value is out of range");

		return {.m_Value = _Value};
	}
}
