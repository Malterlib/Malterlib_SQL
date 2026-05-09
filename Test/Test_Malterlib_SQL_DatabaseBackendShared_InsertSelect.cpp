// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseInsertAndSelect(CTestDatabaseClient *_pDatabase)
	{
		auto &Database = *_pDatabase;

		DMibTestCategory("Insert and select") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"person@example.com">.m_Str);

			auto Rows = Database.template f_Query<gc_SelectPersonByEmail>(NStr::CStr("person@example.com"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("person@example.com"));
				}
			}

			{
				DMibTestPath("First query");
				DMibExpect(nRows, ==, umint(1));
			}

			auto GreaterEqualActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByEmailGe>(&Database, "greater-equal predicate", NStr::CStr("person@example.com"));
			DMibExpect(GreaterEqualActual.m_Email, ==, NStr::CStr("person@example.com"));

			auto LikeActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByEmailLike>(&Database, "like predicate", NStr::CStr("person%@example.com"));
			DMibExpect(LikeActual.m_Email, ==, NStr::CStr("person@example.com"));

			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"personz@example.com">.m_Str);

			auto OrderedRows = Database.template f_Query<gc_SelectPersonByEmailGeDescending>(NStr::CStr("person@example.com"));
			NContainer::TCVector<NStr::CStr> OrderedEmails;

			for (auto iBatch = co_await fg_Move(OrderedRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
					OrderedEmails.f_InsertLast(pRow->m_Data.m_Email);
			}

			DMibExpect(OrderedEmails.f_GetLen(), ==, umint(2));
			if (OrderedEmails.f_GetLen() == 2)
			{
				DMibExpect(OrderedEmails[0], ==, NStr::CStr("personz@example.com"));
				DMibExpect(OrderedEmails[1], ==, NStr::CStr("person@example.com"));
			}

			auto ConvenienceRows = co_await Database.template f_QueryVector<gc_SelectPersonByEmailGeDescending>(NStr::CStr("person@example.com"));
			DMibExpect(ConvenienceRows.f_GetLen(), ==, umint(2));
			if (ConvenienceRows.f_GetLen() == 2)
			{
				DMibExpect(ConvenienceRows[0].m_Email, ==, NStr::CStr("personz@example.com"));
				DMibExpect(ConvenienceRows[1].m_Email, ==, NStr::CStr("person@example.com"));
			}

			DMibExpect(co_await Database.template f_Count<gc_SelectPersonByEmailGeDescending>(NStr::CStr("person@example.com")), ==, umint(2));

			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("person@example.com")), ==, true);
			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("missing@example.com")), ==, false);

			auto OptionalPerson = co_await Database.template f_QueryOptional<gc_SelectPersonByEmail>(NStr::CStr("person@example.com"));
			DMibExpect(bool(OptionalPerson), ==, true);
			if (OptionalPerson)
				DMibExpect(OptionalPerson->m_Email, ==, NStr::CStr("person@example.com"));

			auto MissingOptionalPerson = co_await Database.template f_QueryOptional<gc_SelectPersonByEmail>(NStr::CStr("missing@example.com"));
			DMibExpect(bool(MissingOptionalPerson), ==, false);

			auto OnePerson = co_await Database.template f_QueryOne<gc_SelectPersonByEmail>(NStr::CStr("person@example.com"));
			DMibExpect(OnePerson.m_Email, ==, NStr::CStr("person@example.com"));

			auto MissingOneResult = co_await Database.template f_QueryOne<gc_SelectPersonByEmail>(NStr::CStr("missing@example.com")).f_Wrap();
			fg_TestExpectSqlError(MissingOneResult, "missing one error", ESqlErrorCategory::mc_MissingRow);

			auto LimitedActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByEmailGeDescendingLimitOffset>
				(
					&Database
					, "limit offset"
					, NStr::CStr("person@example.com")
					, CSqlSelectSettings{.m_nResultRowLimit = 1, .m_nResultRowOffset = 1}
				)
			;
			DMibExpect(LimitedActual.m_Email, ==, NStr::CStr("person@example.com"));

			auto OffsetOnlyActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByEmailGeAscendingOffsetOnly>
				(
					&Database
					, "offset without limit"
					, NStr::CStr("person@example.com")
					, CSqlSelectSettings{.m_nResultRowOffset = 1}
				)
			;
			DMibExpect(OffsetOnlyActual.m_Email, ==, NStr::CStr("personz@example.com"));

			auto RepeatedLimitOffsetActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByEmailGeDescendingRepeatedLimitOffset>
				(
					&Database
					, "repeated limit offset"
					, NStr::CStr("person@example.com")
					, CSqlSelectSettings{.m_nResultRowLimit = 1, .m_nResultRowOffset = 1}
				)
			;
			DMibExpect(RepeatedLimitOffsetActual.m_Email, ==, NStr::CStr("person@example.com"));

			auto DistinctActual = co_await fg_TestSqlQuerySingle<gc_SelectDistinctPersonByEmailGeDescendingFirst>
				(
					&Database
					, "distinct"
					, NStr::CStr("person@example.com")
					, CSqlSelectSettings{.m_nResultRowLimit = 1}
				)
			;
			DMibExpect(DistinctActual.m_Email, ==, NStr::CStr("personz@example.com"));

			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"duplicate@example.com">.m_Str);
			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"duplicate@example.com">.m_Str);

			auto DistinctRows = Database.template f_Query<gc_SelectDistinctPersonEmailOnlyByEmailLike>(NStr::CStr("duplicate%@example.com"));
			NContainer::TCVector<NStr::CStr> DistinctEmails;

			for (auto iBatch = co_await fg_Move(DistinctRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
					DistinctEmails.f_InsertLast(fg_Get<0>(pRow->m_Data));
			}

			DMibExpect(DistinctEmails.f_GetLen(), ==, umint(1));
			if (DistinctEmails.f_GetLen() == 1)
				DMibExpect(DistinctEmails[0], ==, NStr::CStr("duplicate@example.com"));

			DMibExpect(co_await Database.template f_Count<gc_SelectDistinctPersonEmailOnlyByEmailLike>(NStr::CStr("duplicate%@example.com")), ==, umint(1));

			DMibExpect(co_await Database.template f_Exists<gc_SelectDistinctPersonEmailOnlyByEmailLike>(NStr::CStr("duplicate%@example.com")), ==, true);

			auto TooManyOptionalResult = co_await Database.template f_QueryOptional<gc_SelectPersonByEmail>(NStr::CStr("duplicate@example.com")).f_Wrap();
			fg_TestExpectSqlError(TooManyOptionalResult, "too many optional error", ESqlErrorCategory::mc_TooManyRows);

			auto EmailOnlyActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonEmailOnlyByEmail>(&Database, "selected columns", NStr::CStr("personz@example.com"));
			DMibExpect(fg_Get<0>(EmailOnlyActual), ==, NStr::CStr("personz@example.com"));

			auto RepeatedSelectActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonRepeatedSelectByEmail>(&Database, "repeated selected columns", NStr::CStr("personz@example.com"));
			DMibExpect(fg_Get<0>(RepeatedSelectActual), ==, NStr::CStr("personz@example.com"));

			auto AndActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByEmailGeAndLike>(&Database, "and predicate", NStr::CStr("person@example.com"), NStr::CStr("person@example.com"));
			DMibExpect(AndActual.m_Email, ==, NStr::CStr("person@example.com"));

			auto OrRows = Database.template f_Query<gc_SelectPersonByEmailEqOrEq>(NStr::CStr("person@example.com"), NStr::CStr("personz@example.com"));
			NContainer::TCVector<NStr::CStr> OrEmails;

			for (auto iBatch = co_await fg_Move(OrRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
					OrEmails.f_InsertLast(pRow->m_Data.m_Email);
			}

			DMibExpect(OrEmails.f_GetLen(), ==, umint(2));
			if (OrEmails.f_GetLen() == 2)
			{
				DMibExpect(OrEmails[0], ==, NStr::CStr("person@example.com"));
				DMibExpect(OrEmails[1], ==, NStr::CStr("personz@example.com"));
			}

			auto NestedActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByNestedAndOr>
				(
					&Database
					, "nested and/or predicate"
					, NStr::CStr("person@example.com")
					, NStr::CStr("missing@example.com")
					, NStr::CStr("personz@example.com")
				)
			;
			DMibExpect(NestedActual.m_Email, ==, NStr::CStr("personz@example.com"));

			auto NotCompositeActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByNotComposite>
				(
					&Database
					, "not composite predicate"
					, NStr::CStr("person@example.com")
					, NStr::CStr("person@example.com")
					, NStr::CStr("missing@example.com")
				)
			;
			DMibExpect(NotCompositeActual.m_Email, ==, NStr::CStr("personz@example.com"));

			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"update-source@example.com">.m_Str);

			DMibExpect(co_await Database.template f_Update<gc_UpdatePersonEmailByEmail>(NStr::CStr("update-target@example.com"), NStr::CStr("update-source@example.com")), ==, umint(1));
			DMibExpect(co_await Database.template f_Update<gc_UpdatePersonEmailByEmail>(NStr::CStr("update-missing@example.com"), NStr::CStr("update-source@example.com")), ==, umint(0));

			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("update-source@example.com")), ==, false);
			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("update-target@example.com")), ==, true);

			int64 ReturnedID = co_await Database.template f_InsertReturning<&CPersonRow::m_ID, gc_InsertPerson>(NStr::CStr("returning@example.com"));
			DMibExpect(ReturnedID, >, int64(0));

			auto ReturningActual = co_await fg_TestSqlQuerySingle<gc_SelectPersonByEmail>(&Database, "insert returning", NStr::CStr("returning@example.com"));
			DMibExpect(ReturningActual.m_ID, ==, ReturnedID);

			auto PersonByID = co_await Database.template f_GetByID<gc_PersonTable, &CPersonRow::m_ID>(ReturnedID);
			DMibExpect(bool(PersonByID), ==, true);
			if (PersonByID)
				DMibExpect(PersonByID->m_Email, ==, NStr::CStr("returning@example.com"));

			auto MissingPersonByID = co_await Database.template f_GetByID<gc_PersonTable, &CPersonRow::m_ID>(int64(0));
			DMibExpect(bool(MissingPersonByID), ==, false);

			umint nUpdatedByID = co_await Database.template f_UpdateByID<gc_PersonTable, &CPersonRow::m_ID, &CPersonRow::m_Email>(NStr::CStr("returning-updated@example.com"), ReturnedID);
			DMibExpect(nUpdatedByID, ==, umint(1));

			PersonByID = co_await Database.template f_GetByID<gc_PersonTable, &CPersonRow::m_ID>(ReturnedID);
			{
				DMibTestPath("updated by ID");
				DMibExpect(bool(PersonByID), ==, true);
				if (PersonByID)
					DMibExpect(PersonByID->m_Email, ==, NStr::CStr("returning-updated@example.com"));
			}

			umint nDeletedByID = co_await Database.template f_DeleteByID<gc_PersonTable, &CPersonRow::m_ID>(ReturnedID);
			DMibExpect(nDeletedByID, ==, umint(1));

			nDeletedByID = co_await Database.template f_DeleteByID<gc_PersonTable, &CPersonRow::m_ID>(ReturnedID);
			DMibExpect(nDeletedByID, ==, umint(0));

			auto SchemaUserID = co_await Database.template f_InsertReturning<&NMib::NSQL::NTest::CUserRow::m_ID, gc_InsertSchemaUser>
				(
					NStr::CStr("role-owner@example.com")
					, NStorage::TCOptional<NStr::CStr>()
					, false
				)
			;

			co_await Database.template f_Insert<gc_InsertUserRole>(SchemaUserID, NStr::CStr("owner"));

			auto UserRoleByCompositeID = co_await Database.template f_GetByCompositeID
				<
					NMib::NSQL::NTest::gc_UserRoleTable
					, TCSqlCompositeID<&NMib::NSQL::NTest::CUserRoleRow::m_UserID, &NMib::NSQL::NTest::CUserRoleRow::m_Role>
				>
				(SchemaUserID, NStr::CStr("owner"))
			;
			DMibExpect(bool(UserRoleByCompositeID), ==, true);
			if (UserRoleByCompositeID)
				DMibExpect(UserRoleByCompositeID->m_Role, ==, NStr::CStr("owner"));

			auto MissingUserRoleByCompositeID = co_await Database.template f_GetByCompositeID
				<
					NMib::NSQL::NTest::gc_UserRoleTable
					, TCSqlCompositeID<&NMib::NSQL::NTest::CUserRoleRow::m_UserID, &NMib::NSQL::NTest::CUserRoleRow::m_Role>
				>
				(SchemaUserID, NStr::CStr("missing"))
			;
			DMibExpect(bool(MissingUserRoleByCompositeID), ==, false);

			umint nDeletedByCompositeID = co_await Database.template f_DeleteByCompositeID
				<
					NMib::NSQL::NTest::gc_UserRoleTable
					, TCSqlCompositeID<&NMib::NSQL::NTest::CUserRoleRow::m_UserID, &NMib::NSQL::NTest::CUserRoleRow::m_Role>
				>
				(SchemaUserID, NStr::CStr("owner"))
			;
			DMibExpect(nDeletedByCompositeID, ==, umint(1));

			nDeletedByCompositeID = co_await Database.template f_DeleteByCompositeID
				<
					NMib::NSQL::NTest::gc_UserRoleTable
					, TCSqlCompositeID<&NMib::NSQL::NTest::CUserRoleRow::m_UserID, &NMib::NSQL::NTest::CUserRoleRow::m_Role>
				>
				(SchemaUserID, NStr::CStr("owner"))
			;
			DMibExpect(nDeletedByCompositeID, ==, umint(0));

			co_await Database.template f_Insert<gc_InsertCompositeThree>(uint64(7), NStr::CStr("settings"), NStr::CStr("theme"), NStr::CStr("dark"));

			auto DuplicateCompositeInsertResult = co_await Database.template f_Insert<gc_InsertCompositeThree>
				(
					uint64(7)
					, NStr::CStr("settings")
					, NStr::CStr("theme")
					, NStr::CStr("light")
				)
				.f_Wrap()
			;
			DMibExpect(bool(DuplicateCompositeInsertResult), ==, false);

			auto CompositeThreeByID = co_await Database.template f_GetByCompositeID
				<
					gc_CompositeThreeTable
					, TCSqlCompositeID<&CCompositeThreeRow::m_TenantID, &CCompositeThreeRow::m_Category, &CCompositeThreeRow::m_Key>
				>
				(uint64(7), NStr::CStr("settings"), NStr::CStr("theme"))
			;
			DMibExpect(bool(CompositeThreeByID), ==, true);
			if (CompositeThreeByID)
				DMibExpect(CompositeThreeByID->m_Value, ==, NStr::CStr("dark"));

			auto MissingCompositeThreeByID = co_await Database.template f_GetByCompositeID
				<
					gc_CompositeThreeTable
					, TCSqlCompositeID<&CCompositeThreeRow::m_TenantID, &CCompositeThreeRow::m_Category, &CCompositeThreeRow::m_Key>
				>
				(uint64(7), NStr::CStr("settings"), NStr::CStr("missing"))
			;
			DMibExpect(bool(MissingCompositeThreeByID), ==, false);

			umint nDeletedCompositeThree = co_await Database.template f_DeleteByCompositeID
				<
					gc_CompositeThreeTable
					, TCSqlCompositeID<&CCompositeThreeRow::m_TenantID, &CCompositeThreeRow::m_Category, &CCompositeThreeRow::m_Key>
				>
				(uint64(7), NStr::CStr("settings"), NStr::CStr("theme"))
			;
			DMibExpect(nDeletedCompositeThree, ==, umint(1));

			co_await Database.template f_Insert<gc_InsertPerson>(NStr::gc_Str<"delete-source@example.com">.m_Str);

			DMibExpect(co_await Database.template f_Delete<gc_DeletePersonByEmail>(NStr::CStr("delete-source@example.com")), ==, umint(1));
			DMibExpect(co_await Database.template f_Delete<gc_DeletePersonByEmail>(NStr::CStr("delete-missing@example.com")), ==, umint(0));

			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("delete-source@example.com")), ==, false);

			co_return {};
		};


		DMibTestCategory("Multi-parameter query binds pagination settings") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("paged-1@example.com"), NStr::CStr("paged-list"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("paged-2@example.com"), NStr::CStr("paged-list"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("paged-3@example.com"), NStr::CStr("paged-list"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("paged-4@example.com"), NStr::CStr("paged-list"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("paged-5@example.com"), NStr::CStr("paged-list"));

			// A two-parameter paginated select must honour the limit bound through CSqlSelectSettings. Before the
			// settings-accepting multi-parameter overload existed there was no way to bind the limit, so it stayed at
			// its default and the query could not be paged. Two different limits prove the bound value takes effect.
			umint nLimitedToTwo = 0;
			{
				auto Rows = Database.template f_Query<gc_SelectProfileEmailsByDisplayNameAndEmailLikeLimited>
					(
						CSqlSelectSettings{.m_nResultRowLimit = 2}
						, NStr::CStr("paged-list")
						, NStr::CStr("paged-%@example.com")
					)
				;
				for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
					for ([[maybe_unused]] auto const &pRow : *iBatch)
						++nLimitedToTwo;
				}
			}
			DMibExpect(nLimitedToTwo, ==, umint(2))(ETestFlag_Aggregated);

			umint nLimitedToFour = 0;
			{
				auto Rows = Database.template f_Query<gc_SelectProfileEmailsByDisplayNameAndEmailLikeLimited>
					(
						CSqlSelectSettings{.m_nResultRowLimit = 4}
						, NStr::CStr("paged-list")
						, NStr::CStr("paged-%@example.com")
					)
				;
				for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
				{
					for ([[maybe_unused]] auto const &pRow : *iBatch)
						++nLimitedToFour;
				}
			}
			DMibExpect(nLimitedToFour, ==, umint(4))(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("No-parameter projection query") -> NConcurrency::TCFuture<void>
		{
			// A projection query (f_QueryVectorAs) instantiates the parameter-type-list machinery even when the prepared
			// select binds no parameters. The all-rows select below has an empty parameter list, so the projection's
			// TCSqlParameterTypes must handle a zero-length type list rather than declaring a zero-sized array (which is
			// ill-formed under -Werror). This exercises the no-parameter projection path end to end.
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("noparam-projection@example.com"), NStr::CStr("noparam-projection"));

			auto Emails = co_await Database.template f_QueryVectorAs<gc_SelectProfileEmails, NStr::CStr, &CProfileRow::m_Email>();

			bool bFound = false;
			for (auto const &Email : Emails)
			{
				if (Email == NStr::CStr("noparam-projection@example.com"))
					bFound = true;
			}
			DMibExpect(bFound, ==, true)(ETestFlag_Aggregated);

			co_return {};
		};

		DMibTestCategory("Ungrouped aggregate count and exists") -> NConcurrency::TCFuture<void>
		{
			// f_Count / f_Exists over an ungrouped aggregate select operate on the single aggregate result row, not the
			// base rows. A filter that matches no rows still yields one aggregate row (COUNT = 0), so f_Count is 1 and
			// f_Exists is true. Rewriting the projection to SELECT 1 would make f_Count count base rows (0) and f_Exists
			// false.
			umint nNoMatch = co_await Database.template f_Count<gc_SelectPersonCountByEmailLike>(NStr::CStr("aggregate-no-such-prefix-%@example.com"));
			DMibExpect(nNoMatch, ==, umint(1))(ETestFlag_Aggregated);

			bool bNoMatchExists = co_await Database.template f_Exists<gc_SelectPersonCountByEmailLike>(NStr::CStr("aggregate-no-such-prefix-%@example.com"));
			DMibExpect(bNoMatchExists, ==, true)(ETestFlag_Aggregated);

			co_return {};
		};

		co_return {};
	}
}
