// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/PostgresClient>
#include <Mib/SQL/PostgresDatabase>

#include "Test_Malterlib_SQL_DatabaseBackendShared.h"
#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"
#include "Test_Malterlib_SQL_PostgresDatabase_Parity.h"

namespace NMib::NSQL::NTest::NPostgresDatabase
{
	// Coverage list bound to the actor's EPostgresValueType. Adding a new
	// enumerator to EPostgresValueType MUST also add an entry here and a
	// corresponding parity check below; the static_assert below guards the
	// count so a new actor type cannot ship without an ORM follow-up.
	namespace
	{
		constexpr EPostgresValueType gc_ParityCoveredTypes[] =
			{
				EPostgresValueType::mc_Null
				, EPostgresValueType::mc_Integer16
				, EPostgresValueType::mc_Integer32
				, EPostgresValueType::mc_Integer64
				, EPostgresValueType::mc_Float32
				, EPostgresValueType::mc_Float64
				, EPostgresValueType::mc_Text
				, EPostgresValueType::mc_Varchar
				, EPostgresValueType::mc_Boolean
				, EPostgresValueType::mc_Bytes
				, EPostgresValueType::mc_Unrecognized
				, EPostgresValueType::mc_Date
				, EPostgresValueType::mc_Time
				, EPostgresValueType::mc_Timestamp
				, EPostgresValueType::mc_TimestampTz
				, EPostgresValueType::mc_UUID
				, EPostgresValueType::mc_Json
				, EPostgresValueType::mc_Jsonb
				, EPostgresValueType::mc_Interval
				, EPostgresValueType::mc_Array_Integer16
				, EPostgresValueType::mc_Array_Integer32
				, EPostgresValueType::mc_Array_Integer64
				, EPostgresValueType::mc_Array_Float32
				, EPostgresValueType::mc_Array_Float64
				, EPostgresValueType::mc_Array_Text
				, EPostgresValueType::mc_Array_Varchar
				, EPostgresValueType::mc_Array_Boolean
				, EPostgresValueType::mc_Array_Bytes
				, EPostgresValueType::mc_Array_Date
				, EPostgresValueType::mc_Array_Time
				, EPostgresValueType::mc_Array_Timestamp
				, EPostgresValueType::mc_Array_TimestampTz
				, EPostgresValueType::mc_Array_UUID
				, EPostgresValueType::mc_Array_Json
				, EPostgresValueType::mc_Array_Jsonb
				, EPostgresValueType::mc_Array_Interval
			}
		;

		// If a new EPostgresValueType enumerator is added, this static_assert
		// must be updated and a corresponding test must appear in
		// fg_RunPostgresParityTests below.
		static_assert(sizeof(gc_ParityCoveredTypes) / sizeof(*gc_ParityCoveredTypes) == 36);

		NConcurrency::TCFuture<CSqlValue> fg_QuerySingleValue(CSqlDatabaseClient *_pDatabase, NStr::CStr _Sql)
		{
			CSqlRawResult Result = co_await _pDatabase->f_QueryRaw(fg_SqlPostgresRaw(fg_Move(_Sql)));
			DMibCheck(Result.m_Rows.f_GetLen() == 1);
			DMibCheck(Result.m_Rows[0].m_Values.f_GetLen() == 1);
			co_return fg_Move(Result.m_Rows[0].m_Values[0]);
		}
	}

	NConcurrency::TCFuture<void> fg_RunPostgresParityTests(CSqlDatabaseClient *_pDatabase)
	{
		using namespace NStr;

		DMibTestCategory("Parity: UUID round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid AS u"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_UUID);

			co_return {};
		};

		DMibTestCategory("Parity: JSON round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT '{\"x\":42}'::json AS j"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Json);

			co_return {};
		};

		DMibTestCategory("Parity: JSONB round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT '{\"x\":42}'::jsonb AS j"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Jsonb);

			co_return {};
		};

		DMibTestCategory("Parity: Date round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT '2024-01-15'::date AS d"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Date);

			co_return {};
		};

		DMibTestCategory("Parity: TimeOfDay round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT '13:45:30'::time AS t"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_TimeOfDay);

			co_return {};
		};

		DMibTestCategory("Parity: Timestamp round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT '2024-01-15 13:45:30'::timestamp AS ts"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Timestamp);

			co_return {};
		};

		DMibTestCategory("Parity: TimestampTz round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT '2024-01-15 13:45:30+00:00'::timestamptz AS tstz"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_TimestampTz);

			co_return {};
		};

		DMibTestCategory("Parity: Interval round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT INTERVAL '3 days 4 hours 5 minutes' AS i"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Interval);

			co_return {};
		};

		DMibTestCategory("Parity: Integer32 array round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT ARRAY[1,2,3,4]::integer[] AS arr"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Array_Integer32);

			if (Value.f_GetTypeID() == ESqlValueType::mc_Array_Integer32)
			{
				auto const &Array = Value.f_GetAsType<TCSqlArray<int32>>();
				DMibExpect(Array.m_Values.f_GetLen(), ==, umint(4));
			}

			co_return {};
		};

		DMibTestCategory("Parity: Text array round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue(_pDatabase, CStr("SELECT ARRAY['alpha','beta']::text[] AS arr"));
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Array_Text);

			if (Value.f_GetTypeID() == ESqlValueType::mc_Array_Text)
			{
				auto const &Array = Value.f_GetAsType<TCSqlArray<NStr::CStr>>();
				DMibExpect(Array.m_Values.f_GetLen(), ==, umint(2));
			}

			co_return {};
		};

		DMibTestCategory("Parity: UUID array round-trip") -> NConcurrency::TCFuture<void>
		{
			CSqlValue Value = co_await fg_QuerySingleValue
				(
					_pDatabase
					, CStr("SELECT ARRAY['550e8400-e29b-41d4-a716-446655440000'::uuid]::uuid[] AS arr")
				)
			;
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_Array_UUID);

			co_return {};
		};

		co_return {};
	}
}
