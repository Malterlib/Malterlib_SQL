// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseValueRoundTrips(CTestDatabaseClient *_pDatabase, CSqlDatabaseBackendCapabilities _Capabilities)
	{
		auto &Database = *_pDatabase;
		auto Capabilities = _Capabilities;

		DMibTestCategory("Value type round trips") -> NConcurrency::TCFuture<void>
		{
			auto Expected = fg_TestValueTypesRow("value-types@example.com");
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(Expected));

			auto Actual = co_await fg_TestSqlQuerySingle<gc_SelectValueTypesByKey>(&Database, "value types", NStr::CStr(Expected.m_Key));
			DMibExpect(Actual.m_Int8, ==, Expected.m_Int8)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_Int16, ==, Expected.m_Int16)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_Int32, ==, Expected.m_Int32)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_Int64, ==, Expected.m_Int64)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_UInt8, ==, Expected.m_UInt8)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_UInt16, ==, Expected.m_UInt16)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_UInt32, ==, Expected.m_UInt32)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_UInt64, ==, Expected.m_UInt64)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_bFlag, ==, Expected.m_bFlag)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_Float32, ==, Expected.m_Float32)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_Float64, ==, Expected.m_Float64)(ETestFlag_Aggregated);
			fg_TestExpectByteVector(Actual.m_Blob, Expected.m_Blob);
			DMibExpect(NTime::fg_GetFullTimeStr(Actual.m_Time), ==, NTime::fg_GetFullTimeStr(Expected.m_Time))(ETestFlag_Aggregated);
			DMibExpect(Actual.m_State, ==, Expected.m_State)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_AccountID, ==, Expected.m_AccountID)(ETestFlag_Aggregated);

			auto InRows = Database.template f_Query<gc_SelectValueTypesByKeyIn>(NStr::CStr("missing-value-types@example.com"), NStr::CStr(Expected.m_Key));
			umint nInRows = 0;

			for (auto iBatch = co_await fg_Move(InRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nInRows;
					DMibExpect(pRow->m_Data.m_Key, ==, Expected.m_Key)(ETestFlag_Aggregated);
				}
			}

			DMibExpect(nInRows, ==, umint(1));

			auto MixedParameterActual = co_await fg_TestSqlQuerySingle<gc_SelectValueTypesByKeyAndInt32>
				(
					&Database
					, "mixed parameter predicate"
					, NStr::CStr(Expected.m_Key)
					, Expected.m_Int32
				)
			;
			DMibExpect(MixedParameterActual.m_Key, ==, Expected.m_Key);
			DMibExpect(MixedParameterActual.m_Int32, ==, Expected.m_Int32);

			auto Aggregate0 = fg_TestValueTypesRow("aggregate-0@example.com");
			Aggregate0.m_Int32 = 10;
			Aggregate0.m_Int64 = 4000000000;
			Aggregate0.m_Float64 = 1.5;
			Aggregate0.m_bFlag = false;
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(Aggregate0));

			auto Aggregate1 = fg_TestValueTypesRow("aggregate-1@example.com");
			Aggregate1.m_Int32 = 20;
			Aggregate1.m_Int64 = 5000000000;
			Aggregate1.m_Float64 = 2.0;
			Aggregate1.m_bFlag = true;
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(Aggregate1));

			auto Aggregate2 = fg_TestValueTypesRow("aggregate-2@example.com");
			Aggregate2.m_Int32 = 30;
			Aggregate2.m_Int64 = 6000000000;
			Aggregate2.m_Float64 = 3.5;
			Aggregate2.m_bFlag = true;
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(Aggregate2));

			auto Expressions = co_await Database.template f_QueryOne<gc_SelectValueTypeExpressionsByKey>
				(
					NStr::CStr("aggregate-0@example.com")
				)
			;
			DMibExpect(fg_Get<0>(Expressions), ==, int64(-1224))(ETestFlag_Aggregated);
			DMibExpect(fg_Get<1>(Expressions), ==, NStr::CStr("AGGREGATE-0@EXAMPLE.COM"))(ETestFlag_Aggregated);
			DMibExpect(fg_Get<2>(Expressions), ==, int64(23))(ETestFlag_Aggregated);
			DMibExpect(fg_Get<3>(Expressions), ==, fp64(10))(ETestFlag_Aggregated);
			DMibExpect(fg_Get<4>(Expressions), ==, int32(10))(ETestFlag_Aggregated);

			auto AliasedExpressions = co_await Database.template f_QueryOne<gc_SelectValueTypeAliasedExpressionsByKey>
				(
					NStr::CStr("aggregate-0@example.com")
				)
			;
			DMibExpect(AliasedExpressions.m_Sum, ==, int64(-1224))(ETestFlag_Aggregated);
			DMibExpect(AliasedExpressions.m_UpperKey, ==, NStr::CStr("AGGREGATE-0@EXAMPLE.COM"))(ETestFlag_Aggregated);
			DMibExpect(AliasedExpressions.m_KeyLength, ==, int64(23))(ETestFlag_Aggregated);
			DMibExpect(AliasedExpressions.m_FloatValue, ==, fp64(10))(ETestFlag_Aggregated);
			DMibExpect(co_await Database.template f_Count<gc_SelectDistinctValueTypeExpressionsByKeyLike>(NStr::CStr("aggregate-%@example.com")), ==, umint(3))(ETestFlag_Aggregated);
			DMibExpect(co_await Database.template f_Exists<gc_SelectDistinctValueTypeExpressionsByKeyLike>(NStr::CStr("aggregate-%@example.com")), ==, true)(ETestFlag_Aggregated);

			auto Aggregates = co_await Database.template f_QueryOne<gc_SelectValueTypeAggregatesByKeyLike>
				(
					NStr::CStr("aggregate-%@example.com")
				)
			;
			DMibExpect(fg_Get<0>(Aggregates), ==, int64(3))(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<1>(Aggregates)), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*fg_Get<1>(Aggregates), ==, int64(60))(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<2>(Aggregates)), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*fg_Get<2>(Aggregates), ==, fp64(20))(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<3>(Aggregates)), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*fg_Get<3>(Aggregates), ==, int32(10))(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<4>(Aggregates)), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*fg_Get<4>(Aggregates), ==, int32(30))(ETestFlag_Aggregated);

			// SUM over a 64-bit column: PostgreSQL widens SUM(BIGINT) to NUMERIC, which the int64 decoder must
			// still resolve (via the BIGINT cast the generator now emits). The summed values exceed the 32-bit
			// range so a bigint result is required.
			auto Int64Sum = co_await Database.template f_QueryOne<gc_SelectValueTypeInt64SumByKeyLike>
				(
					NStr::CStr("aggregate-%@example.com")
				)
			;
			DMibExpect(bool(fg_Get<0>(Int64Sum)), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*fg_Get<0>(Int64Sum), ==, int64(15000000000))(ETestFlag_Aggregated);

			// SUM over a floating-point column keeps its double-precision result and must not be cast.
			auto Float64Sum = co_await Database.template f_QueryOne<gc_SelectValueTypeFloat64SumByKeyLike>
				(
					NStr::CStr("aggregate-%@example.com")
				)
			;
			DMibExpect(bool(fg_Get<0>(Float64Sum)), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*fg_Get<0>(Float64Sum), ==, fp64(7))(ETestFlag_Aggregated);

			auto GroupedAggregates = co_await Database.template f_QueryVector<gc_SelectValueTypeGroupedAggregatesByKeyLike>
				(
					NStr::CStr("aggregate-%@example.com")
					, int64(1)
				)
			;
			DMibExpect(GroupedAggregates.f_GetLen(), ==, umint(1))(ETestFlag_Aggregated);
			if (GroupedAggregates.f_GetLen() == 1)
			{
				DMibExpect(fg_Get<0>(GroupedAggregates[0]), ==, true)(ETestFlag_Aggregated);
				DMibExpect(fg_Get<1>(GroupedAggregates[0]), ==, int64(2))(ETestFlag_Aggregated);
				DMibExpect(bool(fg_Get<2>(GroupedAggregates[0])), ==, true)(ETestFlag_Aggregated);
				DMibExpect(*fg_Get<2>(GroupedAggregates[0]), ==, int64(50))(ETestFlag_Aggregated);
			}

			DMibExpect
				(
					co_await Database.template f_Count<gc_SelectValueTypeGroupedAggregatesByKeyLike>
						(
							NStr::CStr("aggregate-%@example.com")
							, int64(1)
						)
					, ==
					, umint(1)
				)
				(ETestFlag_Aggregated)
			;

			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectValueTypeGroupedAggregatesByKeyLike>
						(
							NStr::CStr("aggregate-%@example.com")
							, int64(1)
						)
					, ==
					, true
				)
				(ETestFlag_Aggregated)
			;

			DMibExpect
				(
					co_await Database.template f_Count<gc_SelectValueTypeGroupedAggregatesByKeyLike>
						(
							NStr::CStr("aggregate-%@example.com")
							, int64(2)
						)
					, ==
					, umint(0)
				)
				(ETestFlag_Aggregated)
			;

			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectValueTypeGroupedAggregatesByKeyLike>
						(
							NStr::CStr("aggregate-%@example.com")
							, int64(2)
						)
					, ==
					, false
				)
				(ETestFlag_Aggregated)
			;

			auto EmptyAggregates = co_await Database.template f_QueryOne<gc_SelectValueTypeAggregatesByKeyLike>
				(
					NStr::CStr("aggregate-missing-%@example.com")
				)
			;
			DMibExpect(fg_Get<0>(EmptyAggregates), ==, int64(0))(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<1>(EmptyAggregates)), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<2>(EmptyAggregates)), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<3>(EmptyAggregates)), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(fg_Get<4>(EmptyAggregates)), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Nullable value round trips") -> NConcurrency::TCFuture<void>
		{
			CNullableTypesRow NullRow;
			NullRow.m_Key = "nullable-null@example.com";
			co_await Database.template f_Insert<gc_InsertNullableTypes>(fg_TempCopy(NullRow));

			auto NullActual = co_await fg_TestSqlQuerySingle<gc_SelectNullableTypesByKey>(&Database, "null row", NStr::CStr(NullRow.m_Key));
			DMibExpect(bool(NullActual.m_Integer), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(NullActual.m_Text), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(NullActual.m_Boolean), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(NullActual.m_Blob), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(NullActual.m_Time), ==, false)(ETestFlag_Aggregated);

			auto NullProjection = co_await Database.template f_QueryOneAs
				<
					gc_SelectNullableTypesByKey
					, CNullableProjection
					, &CNullableTypesRow::m_Integer
					, &CNullableTypesRow::m_Text
				>
				(NStr::CStr(NullRow.m_Key))
			;
			DMibExpect(bool(NullProjection.m_Integer), ==, false)(ETestFlag_Aggregated);
			DMibExpect(bool(NullProjection.m_Text), ==, false)(ETestFlag_Aggregated);

			auto NullPredicateActual = co_await fg_TestSqlQuerySingle<gc_SelectNullableTypesWithNullInteger>(&Database, "is-null predicate");
			DMibExpect(NullPredicateActual.m_Key, ==, NullRow.m_Key)(ETestFlag_Aggregated);

			CNullableTypesRow ValueRow;
			ValueRow.m_Key = "nullable-value@example.com";
			ValueRow.m_Integer = 42;
			ValueRow.m_Text = NStr::CStr("nullable text");
			ValueRow.m_Boolean = true;
			ValueRow.m_Blob = fg_TestSqlByteVector();
			ValueRow.m_Time = fg_TestSqlTime();

			co_await Database.template f_Insert<gc_InsertNullableTypes>(fg_TempCopy(ValueRow));

			auto ValueActual = co_await fg_TestSqlQuerySingle<gc_SelectNullableTypesByKey>(&Database, "value row", NStr::CStr(ValueRow.m_Key));
			DMibExpect(bool(ValueActual.m_Integer), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*ValueActual.m_Integer, ==, *ValueRow.m_Integer)(ETestFlag_Aggregated);
			DMibExpect(bool(ValueActual.m_Text), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*ValueActual.m_Text, ==, *ValueRow.m_Text)(ETestFlag_Aggregated);
			DMibExpect(bool(ValueActual.m_Boolean), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*ValueActual.m_Boolean, ==, *ValueRow.m_Boolean)(ETestFlag_Aggregated);
			DMibExpect(bool(ValueActual.m_Blob), ==, true)(ETestFlag_Aggregated);
			fg_TestExpectByteVector(*ValueActual.m_Blob, *ValueRow.m_Blob);
			DMibExpect(bool(ValueActual.m_Time), ==, true)(ETestFlag_Aggregated);
			DMibExpect(NTime::fg_GetFullTimeStr(*ValueActual.m_Time), ==, NTime::fg_GetFullTimeStr(*ValueRow.m_Time))(ETestFlag_Aggregated);

			auto ValueProjection = co_await Database.template f_QueryOneAs
				<
					gc_SelectNullableTypesByKey
					, CNullableProjection
					, &CNullableTypesRow::m_Integer
					, &CNullableTypesRow::m_Text
				>
				(NStr::CStr(ValueRow.m_Key))
			;
			DMibExpect(bool(ValueProjection.m_Integer), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*ValueProjection.m_Integer, ==, *ValueRow.m_Integer)(ETestFlag_Aggregated);
			DMibExpect(bool(ValueProjection.m_Text), ==, true)(ETestFlag_Aggregated);
			DMibExpect(*ValueProjection.m_Text, ==, *ValueRow.m_Text)(ETestFlag_Aggregated);

			auto NotNullPredicateActual = co_await fg_TestSqlQuerySingle<gc_SelectNullableTypesWithInteger>(&Database, "is-not-null predicate");
			DMibExpect(NotNullPredicateActual.m_Key, ==, ValueRow.m_Key)(ETestFlag_Aggregated);

			// A doubled-integer arithmetic select over the nullable column yields an optional: NULL for the
			// null-integer row, and twice the value otherwise. A non-optional result type would fail to map the null row.
			auto NullSum = co_await Database.template f_QueryOne<gc_SelectNullableIntegerDoubledByKey>(NStr::CStr(NullRow.m_Key));
			DMibExpect(bool(fg_Get<0>(NullSum)), ==, false)(ETestFlag_Aggregated);

			auto ValueSum = co_await Database.template f_QueryOne<gc_SelectNullableIntegerDoubledByKey>(NStr::CStr(ValueRow.m_Key));
			DMibExpect(bool(fg_Get<0>(ValueSum)), ==, true)(ETestFlag_Aggregated);
			if (fg_Get<0>(ValueSum))
				DMibExpect(*fg_Get<0>(ValueSum), ==, int64(84))(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Default values") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertDefaultValuesKey>(NStr::CStr("defaults@example.com"));

			auto Actual = co_await fg_TestSqlQuerySingle<gc_SelectDefaultValuesByKey>(&Database, "default row", NStr::CStr("defaults@example.com"));
			DMibExpect(Actual.m_bEnabled, ==, true)(ETestFlag_Aggregated);
			DMibExpect(Actual.m_RetryCount, ==, uint16(7))(ETestFlag_Aggregated);
			DMibExpect(Actual.m_Label, ==, NStr::CStr("default label"))(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Conversion failures") -> NConcurrency::TCFuture<void>
		{
			auto Overflow = fg_TestValueTypesRow("overflow@example.com");
			Overflow.m_UInt64 = uint64(TCLimitsInt<int64>::mc_Max) + 1;

			auto OverflowInsertResult = co_await fg_TestSqlRunExpectedFailure
				(
					[&Database, Overflow = fg_Move(Overflow)]() mutable
					{
						return Database.template f_Insert<gc_InsertValueTypes>(fg_Move(Overflow));
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(OverflowInsertResult), ==, false);

			co_await Database.template f_Insert<gc_InsertConversionFailureKey>(NStr::CStr("bad-default@example.com"));

			auto ReadResult = co_await fg_TestSqlRunExpectedFailure
				(
					[&Database]()
					{
						return fg_TestSqlQueryAll<gc_SelectConversionFailureByKey>(&Database, NStr::CStr("bad-default@example.com"));
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(ReadResult), ==, false);

			co_return {};
		};

		DMibTestCategory("Read transaction select") -> NConcurrency::TCFuture<void>
		{
			if (!Capabilities.m_bReadTransactions)
				co_return {};

			CPersonRow Person;
			Person.m_Email = "read-transaction@example.com";

			co_await Database.f_Insert(gc_PersonTable, fg_Move(Person));

			auto Transaction = co_await Database.f_BeginReadTransaction();
			auto Rows = Transaction.template f_Query<gc_SelectPersonByEmail>(NStr::CStr("read-transaction@example.com"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("read-transaction@example.com"));
				}
			}

			DMibExpect(nRows, ==, umint(1));

			co_await Transaction.f_Commit();

			co_return {};
		};

		DMibTestCategory("Aliased selects into different members are not conflated") -> NConcurrency::TCFuture<void>
		{
			// gc_SelectAliasReuseFirst and gc_SelectAliasReuseSecond alias the same expression into different members of
			// CAliasReuseRow, so they emit identical SQL and hash to the same content QueryID (static_assert in the
			// shared header). Run inside a transaction, where the prepared-statement cache is populated: the second
			// select must decode its value into its OWN member. Before the fix the cache keyed the row mapping by
			// QueryID, so the second select reused the first's mapping and wrote m_First, leaving m_Second at -1.
			auto Row = fg_TestValueTypesRow("alias-reuse@example.com");
			Row.m_Int32 = 12;
			Row.m_Int16 = 30;
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(Row));

			int64 Expected = int64(Row.m_Int32) + int64(Row.m_Int16);

			co_await Database.f_WithTransaction
				(
					[Expected](CSqlTransaction Transaction) -> NConcurrency::TCFuture<void>
					{
						auto First = co_await Transaction.template f_QueryOne<gc_SelectAliasReuseFirst>(NStr::CStr("alias-reuse@example.com"));
						DMibExpect(First.m_First, ==, Expected)(ETestFlag_Aggregated);
						DMibExpect(First.m_Second, ==, int64(-1))(ETestFlag_Aggregated);

						auto Second = co_await Transaction.template f_QueryOne<gc_SelectAliasReuseSecond>(NStr::CStr("alias-reuse@example.com"));
						DMibExpect(Second.m_Second, ==, Expected)(ETestFlag_Aggregated);
						DMibExpect(Second.m_First, ==, int64(-1))(ETestFlag_Aggregated);

						co_return {};
					}
				)
			;

			co_return {};
		};

		co_return {};
	}
}
