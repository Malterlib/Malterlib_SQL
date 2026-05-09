// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseJoins(CTestDatabaseClient *_pDatabase)
	{
		auto &Database = *_pDatabase;

		DMibTestCategory("Inner join") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"join-person@example.com">.m_Str);
			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"join-person-no-profile@example.com">.m_Str);
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("join-person@example.com"), NStr::CStr("Join Profile"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("join-upsert@example.com"), NStr::CStr("Join Upsert Profile"));
			co_await Database.template f_Upsert<gc_UpsertPersonDisplayNameByEmail>(NStr::CStr("join-upsert@example.com"), NStr::CStr("Join Profile"));

			auto Rows = co_await Database.template f_QueryVector<gc_JoinPersonProfilesByEmail>();
			NStorage::TCOptional<NStorage::TCTuple<CPersonRow, CProfileRow>> Match;

			for (auto &&Row : Rows)
			{
				if (fg_Get<0>(Row).m_Email == NStr::CStr("join-person@example.com"))
					Match = Row;
			}

			DMibExpect(bool(Match), ==, true);
			if (Match)
			{
				DMibExpect(fg_Get<0>(*Match).m_Email, ==, NStr::CStr("join-person@example.com"));
				DMibExpect(fg_Get<1>(*Match).m_Email, ==, NStr::CStr("join-person@example.com"));
				DMibExpect(fg_Get<1>(*Match).m_DisplayName, ==, NStr::CStr("Join Profile"));
			}

			auto JoinedStructRows = co_await Database.template f_QueryJoinedVectorAs<gc_JoinPersonProfilesByEmail, CPersonProfileJoin>();
			bool bFoundJoinedStruct = false;

			for (auto const &Row : JoinedStructRows)
			{
				if (Row.m_Person.m_Email == NStr::CStr("join-person@example.com"))
				{
					bFoundJoinedStruct = true;
					DMibExpect(Row.m_Profile.m_DisplayName, ==, NStr::CStr("Join Profile"));
				}
			}

			DMibExpect(bFoundJoinedStruct, ==, true);

			auto ThreeTableStructRows = co_await Database.template f_QueryJoinedVectorAs<gc_JoinPersonProfilesToUpserts, CPersonProfileUpsertJoin>();
			bool bFoundThreeTableStruct = false;
			for (auto const &Row : ThreeTableStructRows)
			{
				if (Row.m_Person.m_Email == NStr::CStr("join-person@example.com"))
				{
					bFoundThreeTableStruct = true;
					// The third joined table must be populated, not left value-initialized.
					DMibExpect(Row.m_Upsert.m_Email, ==, NStr::CStr("join-upsert@example.com"));
				}
			}
			DMibExpect(bFoundThreeTableStruct, ==, true)(ETestFlag_Aggregated);

			auto ChainedRows = co_await Database.template f_QueryVector<gc_JoinPersonProfilesToUpserts>();
			bool bFoundChainedRow = false;
			for (auto const &Row : ChainedRows)
			{
				if (!bFoundChainedRow && fg_Get<0>(Row).m_Email == NStr::CStr("join-person@example.com"))
				{
					DMibExpect(fg_Get<1>(Row).m_DisplayName, ==, NStr::CStr("Join Profile"));
					DMibExpect(fg_Get<2>(Row).m_Email, ==, NStr::CStr("join-upsert@example.com"));
					bFoundChainedRow = true;
				}
			}
			DMibExpect(bFoundChainedRow, ==, true);

			auto LongerChainedRows = co_await Database.template f_QueryVector<gc_JoinPersonProfilesToUpsertsToProfiles>();
			NStr::CStr LongerExpectedProfile("Join Profile");
			NStr::CStr LongerExpectedUpsertEmail("join-upsert@example.com");
			bool bFoundLongerChainedRow = false;
			for (auto const &Row : LongerChainedRows)
			{
				if (!bFoundLongerChainedRow && fg_Get<0>(Row).m_Email == NStr::CStr("join-person@example.com"))
				{
					DMibExpect(fg_Get<1>(Row).m_DisplayName, ==, LongerExpectedProfile);
					DMibExpect(fg_Get<2>(Row).m_Email, ==, LongerExpectedUpsertEmail);
					DMibExpect(fg_Get<3>(Row).m_DisplayName, ==, NStr::CStr("Join Upsert Profile"));
					bFoundLongerChainedRow = true;
					break;
				}
			}
			DMibExpect(bFoundLongerChainedRow, ==, true);

			auto SubqueryRows = co_await Database.template f_QueryVector<gc_SelectPeopleWithProfileEmailSubquery>();
			bool bFoundSubqueryRow = false;
			for (auto const &Row : SubqueryRows)
			{
				if (Row.m_Email == NStr::CStr("join-person@example.com"))
					bFoundSubqueryRow = true;
			}
			DMibExpect(bFoundSubqueryRow, ==, true);

			auto ParameterizedSubqueryRows = co_await Database.template f_QueryVector<gc_SelectPeopleWithProfileDisplayNameSubquery>(NStr::CStr("Join Profile"));
			bool bFoundParameterizedSubqueryRow = false;
			for (auto const &Row : ParameterizedSubqueryRows)
			{
				if (Row.m_Email == NStr::CStr("join-person@example.com"))
					bFoundParameterizedSubqueryRow = true;
			}
			DMibExpect(bFoundParameterizedSubqueryRow, ==, true);

			auto NestedParameterRows = co_await Database.template f_QueryVector<gc_SelectPeopleByEmailLikeWithProfileDisplayNameSubquery>
				(
					NStr::CStr("join-person%")
					, NStr::CStr("Join Profile")
				)
			;
			bool bFoundNestedParameterRow = false;
			for (auto const &Row : NestedParameterRows)
			{
				if (Row.m_Email == NStr::CStr("join-person@example.com"))
					bFoundNestedParameterRow = true;
			}
			DMibExpect(bFoundNestedParameterRow, ==, true);

			auto LimitedNestedParameterRows = co_await Database.template f_QueryVector<gc_SelectPeopleByEmailLikeWithLimitedProfileDisplayNameSubquery>
				(
					NStr::CStr("join-person%")
					, NStr::CStr("Join Profile")
					, int64(1)
					, int64(0)
				)
			;
			bool bFoundLimitedNestedParameterRow = false;
			for (auto const &Row : LimitedNestedParameterRows)
			{
				if (Row.m_Email == NStr::CStr("join-person@example.com"))
					bFoundLimitedNestedParameterRow = true;
			}
			DMibExpect(bFoundLimitedNestedParameterRow, ==, true);

			auto ExistsRows = co_await Database.template f_QueryVector<gc_SelectPeopleWhenProfilesExist>();
			DMibExpect(ExistsRows.f_IsEmpty(), ==, false);

			auto ParameterizedExistsRows = co_await Database.template f_QueryVector<gc_SelectPeopleWhenProfileDisplayNameExists>(NStr::CStr("Join Profile"));
			DMibExpect(ParameterizedExistsRows.f_IsEmpty(), ==, false);

			auto NotExistsRows = co_await Database.template f_QueryVector<gc_SelectPeopleWhenNoLeftJoinChildrenExist>();
			DMibExpect(NotExistsRows.f_IsEmpty(), ==, false);

			auto UnionRows = co_await Database.template f_QueryVector<gc_UnionPersonAndProfileEmails>();
			auto UnionAllRows = co_await Database.template f_QueryVector<gc_UnionAllPersonAndProfileEmails>();
			auto IntersectRows = co_await Database.template f_QueryVector<gc_IntersectPersonAndProfileEmails>();
			auto ExceptRows = co_await Database.template f_QueryVector<gc_ExceptPersonProfileEmails>();
			DMibExpect(UnionAllRows.f_GetLen(), >=, UnionRows.f_GetLen());

			bool bFoundIntersectEmail = false;
			for (auto const &Row : IntersectRows)
			{
				if (fg_Get<0>(Row) == NStr::CStr("join-person@example.com"))
					bFoundIntersectEmail = true;
			}
			DMibExpect(bFoundIntersectEmail, ==, true);

			bool bFoundExceptEmail = false;
			for (auto const &Row : ExceptRows)
			{
				if (fg_Get<0>(Row) == NStr::CStr("join-person-no-profile@example.com"))
					bFoundExceptEmail = true;
			}
			DMibExpect(bFoundExceptEmail, ==, true);
			DMibExpect(co_await Database.template f_Count<gc_ExceptPersonProfileEmails>(), >, umint(0));
			DMibExpect(co_await Database.template f_Exists<gc_ExceptPersonProfileEmails>(), ==, true);

			auto ModifiedSetOperandResult = co_await Database.template f_QueryVector<gc_UnionWithModifiedLeftOperand>().f_Wrap();
			DMibExpect(bool(ModifiedSetOperandResult), ==, false);

			// A modifier on a right-hand operand must be rejected too: the generator does not parenthesize
			// operands, so an ORDER BY on the right operand would otherwise silently apply to the whole UNION.
			auto ModifiedRightSetOperandResult = co_await Database.template f_QueryVector<gc_UnionWithModifiedRightOperand>().f_Wrap();
			DMibExpect(bool(ModifiedRightSetOperandResult), ==, false);

			auto ParameterizedUnionRows = co_await Database.template f_QueryVector<gc_UnionParameterizedPersonAndProfileEmails>(NStr::CStr("join-person%"), NStr::CStr("join-person%"));
			bool bFoundParameterizedUnionEmail = false;
			for (auto const &Row : ParameterizedUnionRows)
			{
				if (fg_Get<0>(Row) == NStr::CStr("join-person@example.com"))
					bFoundParameterizedUnionEmail = true;
			}
			DMibExpect(bFoundParameterizedUnionEmail, ==, true);

			DMibExpect
				(
					co_await Database.template f_Count<gc_UnionParameterizedPersonAndProfileEmails>(NStr::CStr("missing-union-left%"), NStr::CStr("join-person%"))
					, >
					, umint(0)
				)
			;
			DMibExpect
				(
					co_await Database.template f_Exists<gc_UnionParameterizedPersonAndProfileEmails>(NStr::CStr("missing-union-left%"), NStr::CStr("join-person%"))
					, ==
					, true
				)
			;

			auto ParameterizedUnionSubqueryRows = co_await Database.template f_QueryVector<gc_SelectPeopleWithParameterizedUnionSubquery>
				(
					NStr::CStr("join-person%")
					, NStr::CStr("join-person%")
				)
			;
			bool bFoundParameterizedUnionSubqueryEmail = false;
			for (auto const &Row : ParameterizedUnionSubqueryRows)
			{
				if (Row.m_Email == NStr::CStr("join-person@example.com"))
					bFoundParameterizedUnionSubqueryEmail = true;
			}
			DMibExpect(bFoundParameterizedUnionSubqueryEmail, ==, true);

			co_return {};
		};

		DMibTestCategory("Left join") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertLeftJoinParent>(NStr::CStr("left-join-match"));
			co_await Database.template f_Insert<gc_InsertLeftJoinParent>(NStr::CStr("left-join-missing"));
			co_await Database.template f_Insert<gc_InsertLeftJoinChild>(NStr::CStr("left-join-match"), NStr::CStr("child value"));

			auto Rows = co_await Database.template f_QueryVector<gc_LeftJoinParentsToChildren>();
			NStorage::TCOptional<CLeftJoinChildRow> MatchedChild;
			NStorage::TCOptional<CLeftJoinChildRow> MissingChild;

			for (auto &&Row : Rows)
			{
				if (fg_Get<0>(Row).m_Key == NStr::CStr("left-join-match"))
					MatchedChild = fg_Get<1>(Row);
				else if (fg_Get<0>(Row).m_Key == NStr::CStr("left-join-missing"))
					MissingChild = fg_Get<1>(Row);
			}

			DMibExpect(bool(MatchedChild), ==, true);
			if (MatchedChild)
			{
				DMibExpect(bool(MatchedChild->m_Key), ==, true);
				DMibExpect(*MatchedChild->m_Key, ==, NStr::CStr("left-join-match"));
				DMibExpect(bool(MatchedChild->m_Value), ==, true);
				DMibExpect(*MatchedChild->m_Value, ==, NStr::CStr("child value"));
			}

			DMibExpect(bool(MissingChild), ==, true);
			if (MissingChild)
			{
				DMibExpect(bool(MissingChild->m_Key), ==, false);
				DMibExpect(bool(MissingChild->m_Value), ==, false);
			}

			auto ChainedRows = co_await Database.template f_QueryVector<gc_LeftJoinParentsToChildChain>();
			bool bFoundChainedLeftMatch = false;
			bool bFoundChainedLeftMissing = false;
			for (auto const &Row : ChainedRows)
			{
				if (fg_Get<0>(Row).m_Key == NStr::CStr("left-join-match"))
				{
					DMibExpect(bool(fg_Get<1>(Row).m_Key), ==, true);
					DMibExpect(bool(fg_Get<2>(Row).m_Key), ==, true);
					bFoundChainedLeftMatch = true;
				}
				else if (fg_Get<0>(Row).m_Key == NStr::CStr("left-join-missing"))
				{
					DMibExpect(bool(fg_Get<1>(Row).m_Key), ==, false);
					DMibExpect(bool(fg_Get<2>(Row).m_Key), ==, false);
					bFoundChainedLeftMissing = true;
				}
			}
			DMibExpect(bFoundChainedLeftMatch, ==, true);
			DMibExpect(bFoundChainedLeftMissing, ==, true);

			co_return {};
		};

		DMibTestCategory("Multi-predicate non-equality join") -> NConcurrency::TCFuture<void>
		{
			auto Lower = fg_TestValueTypesRow("join-range");
			Lower.m_Int32 = 1;
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(Lower));

			auto Higher = fg_TestValueTypesRow("join-range");
			Higher.m_Int32 = 2;
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(Higher));

			auto OtherKey = fg_TestValueTypesRow("join-range-other");
			OtherKey.m_Int32 = 3;
			co_await Database.template f_Insert<gc_InsertValueTypes>(fg_TempCopy(OtherKey));

			auto Rows = co_await Database.template f_QueryVector<gc_JoinValueTypesByKeyAndInt32Less>();
			bool bFoundRangeJoin = false;
			for (auto const &Row : Rows)
			{
				auto const &Left = fg_Get<0>(Row);
				auto const &Right = fg_Get<1>(Row);
				if (Left.m_Key == NStr::CStr("join-range") && Right.m_Key == NStr::CStr("join-range"))
				{
					DMibExpect(Left.m_Int32, ==, int32(1));
					DMibExpect(Right.m_Int32, ==, int32(2));
					bFoundRangeJoin = true;
				}
			}

			DMibExpect(bFoundRangeJoin, ==, true);

			co_return {};
		};

		DMibTestCategory("Delete with parameterized subquery predicate") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertPerson>(NStr::CStr("delete-subquery@example.com"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("delete-subquery@example.com"), NStr::CStr("Delete Subquery Profile"));

			DMibExpect(co_await Database.template f_Count<gc_SelectPersonByEmail>(NStr::CStr("delete-subquery@example.com")), ==, umint(1))(ETestFlag_Aggregated);

			// The DELETE predicate is an IN subquery carrying LIMIT/OFFSET parameters; all three nested parameters
			// (display_name, limit, offset) must be bound so they line up with the placeholders the generated SQL
			// emits.
			umint nDeleted = co_await Database.template f_Delete<gc_DeletePeopleInLimitedProfileSubquery>(NStr::CStr("Delete Subquery Profile"), int64(10), int64(0));
			DMibExpect(nDeleted, ==, umint(1))(ETestFlag_Aggregated);

			DMibExpect(co_await Database.template f_Count<gc_SelectPersonByEmail>(NStr::CStr("delete-subquery@example.com")), ==, umint(0))(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("QueryOne and QueryOptional accept a compound select") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertPerson>(NStr::CStr("compound-one@example.com"));

			// A compound (union) select has no f_WithLimit(); f_QueryOne and f_QueryOptional must still accept it
			// rather than failing to compile. The union of the person matching the exact email and no profiles is a
			// single row.
			auto One = co_await Database.template f_QueryOne<gc_UnionParameterizedPersonAndProfileEmails>(NStr::CStr("compound-one@example.com"), NStr::CStr("no-such-profile@example.com"));
			DMibExpect(fg_Get<0>(One), ==, NStr::CStr("compound-one@example.com"))(ETestFlag_Aggregated);

			auto Optional = co_await Database.template f_QueryOptional<gc_UnionParameterizedPersonAndProfileEmails>
				(
					NStr::CStr("no-such-person@example.com")
					, NStr::CStr("no-such-profile@example.com")
				)
			;
			DMibExpect(bool(Optional), ==, false)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}
}
