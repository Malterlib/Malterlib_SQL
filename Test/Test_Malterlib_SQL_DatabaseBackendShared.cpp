// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NContainer::CIOByteVector fg_TestSqlByteVector()
	{
		NContainer::CIOByteVector Bytes;
		Bytes.f_InsertLast(uint8(0));
		Bytes.f_InsertLast(uint8(1));
		Bytes.f_InsertLast(uint8(2));
		Bytes.f_InsertLast(uint8(255));

		return Bytes;
	}

	NTime::CTime fg_TestSqlTime()
	{
		return NTime::CTimeConvert::fs_CreateTime(2024, 3, 15, 14, 30, 45, 0.123);
	}

	CValueTypesRow fg_TestValueTypesRow(NStr::CStr _Key)
	{
		CValueTypesRow Row;
		Row.m_Key = fg_Move(_Key);
		Row.m_Int8 = -12;
		Row.m_Int16 = -1234;
		Row.m_Int32 = -123456;
		Row.m_Int64 = -1234567890123;
		Row.m_UInt8 = 12;
		Row.m_UInt16 = 1234;
		Row.m_UInt32 = 123456;
		Row.m_UInt64 = 9000000000ull;
		Row.m_bFlag = true;
		Row.m_Float32 = 1.25f;
		Row.m_Float64 = 2.5;
		Row.m_Blob = fg_TestSqlByteVector();
		Row.m_Time = fg_TestSqlTime();
		Row.m_State = EAccountState::mc_Enabled;
		Row.m_AccountID = CAccountID(123456789);

		return Row;
	}

	void fg_TestExpectByteVector(NContainer::CIOByteVector const &_Actual, NContainer::CIOByteVector const &_Expected)
	{
		DMibExpect(_Actual.f_GetLen(), ==, _Expected.f_GetLen());
		for (umint i = 0; i < _Actual.f_GetLen() && i < _Expected.f_GetLen(); ++i)
			DMibExpect(_Actual[i], ==, _Expected[i])(ETestFlag_Aggregated);
	}

	void fg_TestSqlDatabaseStorageAndMappings()
	{
		static_assert(TCSqlTypeTraits<CAccountID>::mc_ColumnType == ESqlColumnType::mc_UnsignedInteger64);
		static_assert(TCSqlTypeTraits<CAccountID>::mc_ValueType == ESqlValueType::mc_UnsignedInteger64);
		static_assert(TCSqlTypeTraits<EAccountState>::mc_ValueType == ESqlValueType::mc_UnsignedInteger8);
		static_assert(TCSqlTypeTraits<fp32>::mc_ColumnType == ESqlColumnType::mc_Float32);
		static_assert(TCSqlTypeTraits<fp64>::mc_ColumnType == ESqlColumnType::mc_Float64);
		static_assert(TCSqlTypeTraits<fp32>::mc_ValueType == ESqlValueType::mc_Float32);
		static_assert(TCSqlTypeTraits<fp64>::mc_ValueType == ESqlValueType::mc_Float64);

		DMibTestCategory("Trait storage insert operation construction")
		{
			auto TableOperation = NPrivate::fg_SqlInsertOperation(gc_StorageOnlyInsertTable, uint64(42));
			DMibExpect(TableOperation.m_Values.f_GetLen(), ==, umint(1));
			DMibExpect(TableOperation.m_Values[0].m_Value.f_GetTypeID(), ==, ESqlValueType::mc_UnsignedInteger64);
			DMibExpect(TableOperation.m_Values[0].m_Value.f_GetAsType<uint64>(), ==, uint64(42));

			auto PreparedOperation = NPrivate::fg_SqlInsertOperation<gc_StorageOnlyPreparedInsert>(uint64(43));
			DMibExpect(PreparedOperation.m_Values.f_GetLen(), ==, umint(1));
			DMibExpect(PreparedOperation.m_Values[0].m_Value.f_GetTypeID(), ==, ESqlValueType::mc_UnsignedInteger64);
			DMibExpect(PreparedOperation.m_Values[0].m_Value.f_GetAsType<uint64>(), ==, uint64(43));

			auto SelectOperation = NPrivate::fg_SqlSelectOperation<gc_SelectStorageOnlyByID>(uint64(44), {});
			DMibExpect(SelectOperation.m_Parameters.f_GetLen(), ==, umint(1));
			DMibExpect(SelectOperation.m_Parameters[0].f_GetTypeID(), ==, ESqlValueType::mc_UnsignedInteger64);
			DMibExpect(SelectOperation.m_Parameters[0].f_GetAsType<uint64>(), ==, uint64(44));
			DMibExpect(SelectOperation.m_pDescription->m_ParameterTypes.f_GetType(0), ==, ESqlValueType::mc_UnsignedInteger64);
			DMibExpect(SelectOperation.m_pDescription->m_pStatement->f_Describe().m_Predicate.m_Type, ==, ESqlPredicateType::mc_EqualParameter);
			DMibExpect(SelectOperation.m_pDescription->m_pStatement->f_Describe().m_Predicate.m_ColumnName, ==, NStr::CStr("id"));
			DMibExpect(SelectOperation.m_pDescription->m_pStatement->f_Describe().m_Predicate.m_iParameter, ==, umint(0));
		};

		DMibTestCategory("Float32 materialization")
		{
			CSqlValue MaxFloat32 = fp64(fp32::fs_LimitMax());
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<fp32>(fg_Move(MaxFloat32), NStr::gc_Str<"float32_value">.m_Str), ==, fp32::fs_LimitMax());

			CSqlValue OutOfRange = fp64(fp32::fs_LimitMax()) * 2.0;
			auto OutOfRangeResult = NPrivate::fg_SqlStorageFromValue<fp32>(fg_Move(OutOfRange), NStr::gc_Str<"float32_value">.m_Str);
			DMibExpect(bool(OutOfRangeResult), ==, false);

			CSqlValue Infinity = fp64::fs_Inf();
			DMibExpect((*NPrivate::fg_SqlStorageFromValue<fp32>(fg_Move(Infinity), NStr::gc_Str<"float32_value">.m_Str)).f_IsInfinity(), ==, true);

			CSqlValue Nan = fp64::fs_QNan();
			DMibExpect((*NPrivate::fg_SqlStorageFromValue<fp32>(fg_Move(Nan), NStr::gc_Str<"float32_value">.m_Str)).f_IsNan(), ==, true);
		};

		DMibTestCategory("UUID value mapping")
		{
			NCryptography::CUniversallyUniqueIdentifier UUID(0xC2EA34BBu, 0x5C04, 0x4945, 0xA798, constant_uint64(0xD5685B7CD2A8));
			CSqlValue Value = UUID;
			DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_UUID);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<NCryptography::CUniversallyUniqueIdentifier>(fg_Move(Value), NStr::gc_Str<"uuid_value">.m_Str), ==, UUID);
		};

		DMibTestCategory("PostgreSQL temporal value mapping")
		{
			auto Time = fg_TestSqlTime();
			CSqlDate Date{.m_Time = Time};
			CSqlTimeOfDay TimeOfDay{.m_Time = Time};
			CSqlTimestamp Timestamp{.m_Time = Time};
			CSqlTimestampTz TimestampTz{.m_Time = Time};
			CSqlInterval Interval{.m_Months = 2, .m_Days = 3, .m_Time = NTime::CTimeSpan(4)};

			CSqlValue DateValue = Date;
			CSqlValue TimeValue = TimeOfDay;
			CSqlValue TimestampValue = Timestamp;
			CSqlValue TimestampTzValue = TimestampTz;
			CSqlValue IntervalValue = Interval;

			DMibExpect(DateValue.f_GetTypeID(), ==, ESqlValueType::mc_Date);
			DMibExpect(TimeValue.f_GetTypeID(), ==, ESqlValueType::mc_TimeOfDay);
			DMibExpect(TimestampValue.f_GetTypeID(), ==, ESqlValueType::mc_Timestamp);
			DMibExpect(TimestampTzValue.f_GetTypeID(), ==, ESqlValueType::mc_TimestampTz);
			DMibExpect(IntervalValue.f_GetTypeID(), ==, ESqlValueType::mc_Interval);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<CSqlDate>(fg_Move(DateValue), NStr::gc_Str<"date_value">.m_Str), ==, Date);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<CSqlTimeOfDay>(fg_Move(TimeValue), NStr::gc_Str<"time_value">.m_Str), ==, TimeOfDay);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<CSqlTimestamp>(fg_Move(TimestampValue), NStr::gc_Str<"timestamp_value">.m_Str), ==, Timestamp);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<CSqlTimestampTz>(fg_Move(TimestampTzValue), NStr::gc_Str<"timestamptz_value">.m_Str), ==, TimestampTz);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<CSqlInterval>(fg_Move(IntervalValue), NStr::gc_Str<"interval_value">.m_Str), ==, Interval);
		};

		DMibTestCategory("PostgreSQL JSON value mapping")
		{
			NEncoding::CJsonOrdered Json = NEncoding::CJsonOrdered::fs_FromString("{\"a\":1}");
			NEncoding::CJsonSorted Jsonb = NEncoding::CJsonSorted::fs_FromString("{\"b\":2}");
			CSqlValue JsonValue = Json;
			CSqlValue JsonbValue = Jsonb;

			DMibExpect(JsonValue.f_GetTypeID(), ==, ESqlValueType::mc_Json);
			DMibExpect(JsonbValue.f_GetTypeID(), ==, ESqlValueType::mc_Jsonb);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<NEncoding::CJsonOrdered>(fg_Move(JsonValue), NStr::gc_Str<"json_value">.m_Str), ==, Json);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<NEncoding::CJsonSorted>(fg_Move(JsonbValue), NStr::gc_Str<"jsonb_value">.m_Str), ==, Jsonb);
		};

		DMibTestCategory("PostgreSQL array value mapping")
		{
			TCSqlArray<int32> Array;
			Array.m_Dimensions.f_InsertLast({.m_Length = 2, .m_LowerBound = 1});
			Array.m_Values.f_InsertLast(1);
			Array.m_Values.f_InsertLast(NStorage::TCOptional<int32>());
			CSqlValue ArrayValue = Array;

			DMibExpect(ArrayValue.f_GetTypeID(), ==, ESqlValueType::mc_Array_Integer32);
			DMibExpect(*NPrivate::fg_SqlStorageFromValue<TCSqlArray<int32>>(fg_Move(ArrayValue), NStr::gc_Str<"array_value">.m_Str), ==, Array);
		};

		DMibTestCategory("PostgreSQL unrecognized value mapping")
		{
			CSqlUnrecognizedBackendValue Unrecognized;
			Unrecognized.m_TypeID = 123456;
			Unrecognized.m_Bytes.f_InsertLast(uint8(1));
			Unrecognized.m_Bytes.f_InsertLast(uint8(2));
			CSqlValue UnrecognizedValue = Unrecognized;

			DMibExpect(UnrecognizedValue.f_GetTypeID(), ==, ESqlValueType::mc_UnrecognizedBackend);
			DMibExpect(UnrecognizedValue.f_GetAsType<CSqlUnrecognizedBackendValue>(), ==, Unrecognized);
		};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabase(FCreateBackend _fCreateBackend)
	{
		fg_TestSqlDatabaseStorageAndMappings();

		NConcurrency::TCActor<ICSqlDatabaseBackendActor> Backend = _fCreateBackend(&gc_SchemaVersions);
		CSqlDatabaseBackendCapabilities Capabilities = co_await Backend(&ICSqlDatabaseBackendActor::f_Capabilities);
		DMibExpect(Capabilities.m_bReadTransactions, ==, true);

		CTestDatabaseClient Database(Backend);
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		co_await fg_TestSqlDatabaseInsertAndSelect(&Database);
		co_await fg_TestSqlDatabaseTransactionInsertAndSelect(&Database, Capabilities);
		co_await fg_TestSqlDatabaseSave(&Database);
		co_await fg_TestSqlDatabaseMutationsAndUpsert(&Database);
		co_await fg_TestSqlDatabaseJoins(&Database);
		co_await fg_TestSqlDatabaseTableAndPreparedInserts(&Database);
		co_await fg_TestSqlDatabaseValueRoundTrips(&Database, Capabilities);
		co_await fg_TestSqlDatabaseErrorModel(&Database);
		co_await fg_TestSqlDatabaseRaw(&Database, Capabilities);

		co_return {};
	}
}
