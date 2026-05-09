// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseMutationsAndUpsert(CTestDatabaseClient *_pDatabase)
	{
		auto &Database = *_pDatabase;

		DMibTestCategory("Explicit full-table mutations") -> NConcurrency::TCFuture<void>
		{
			co_await Database.f_Insert<gc_InsertFullMutation>(NStr::CStr("full-a"));
			co_await Database.f_Insert<gc_InsertFullMutation>(NStr::CStr("full-b"));

			DMibExpect(co_await Database.template f_Update<gc_UpdateAllFullMutationLabels>(NStr::CStr("full-updated")), ==, umint(2));
			DMibExpect(co_await Database.template f_Delete<gc_DeleteAllFullMutationRows>(), ==, umint(2));

			DMibExpect(co_await Database.template f_Delete<gc_DeleteAllFullMutationRows>(), ==, umint(0));

			co_return {};
		};

		DMibTestCategory("Portable upsert") -> NConcurrency::TCFuture<void>
		{
			DMibExpect(co_await Database.template f_Upsert<gc_UpsertPersonDisplayNameByEmail>(NStr::CStr("upsert@example.com"), NStr::CStr("Initial Name")), ==, umint(1));

			auto Initial = co_await fg_TestSqlQuerySingle<gc_SelectUpsertPersonByEmail>(&Database, "initial upsert", NStr::CStr("upsert@example.com"));
			DMibExpect(Initial.m_DisplayName, ==, NStr::CStr("Initial Name"));

			DMibExpect(co_await Database.template f_Upsert<gc_UpsertPersonDisplayNameByEmail>(NStr::CStr("upsert@example.com"), NStr::CStr("Updated Name")), ==, umint(1));

			auto Updated = co_await fg_TestSqlQuerySingle<gc_SelectUpsertPersonByEmail>(&Database, "conflict upsert", NStr::CStr("upsert@example.com"));
			DMibExpect(Updated.m_DisplayName, ==, NStr::CStr("Updated Name"));

			co_return {};
		};

		DMibTestCategory("Typed mutation returning") -> NConcurrency::TCFuture<void>
		{
			using CStrResult = NConcurrency::TCAsyncResult<NStr::CStr>;

			CStrResult UpsertResult = co_await Database.template f_UpsertReturning<&CUpsertRow::m_DisplayName, gc_UpsertPersonDisplayNameByEmail>
				(
					NStr::CStr("returning@example.com")
					, NStr::CStr("Returning Initial")
				)
				.f_Wrap()
			;
			if (UpsertResult)
			{
				DMibExpect(*UpsertResult, ==, NStr::CStr("Returning Initial"));

				NStr::CStr UpdatedName = co_await Database.template f_UpdateReturning<&CUpsertRow::m_DisplayName, gc_UpdateUpsertPersonDisplayNameByEmail>
					(
						NStr::CStr("Returning Updated")
						, NStr::CStr("returning@example.com")
					)
				;
				DMibExpect(UpdatedName, ==, NStr::CStr("Returning Updated"));

				NStr::CStr DeletedEmail = co_await Database.template f_DeleteReturning<&CUpsertRow::m_Email, gc_DeleteUpsertPersonByEmail>(NStr::CStr("returning@example.com"));
				DMibExpect(DeletedEmail, ==, NStr::CStr("returning@example.com"));

				// A RETURNING target that is a TCOptional member must surface SQL NULL as an empty optional rather than a
				// conversion error. Insert a row whose nullable integer_value is NULL, then delete it returning that column.
				CNullableTypesRow NullReturningRow;
				NullReturningRow.m_Key = "returning-null@example.com";
				co_await Database.template f_Insert<gc_InsertNullableTypes>(fg_TempCopy(NullReturningRow));

				NStorage::TCOptional<int32> NullReturned = co_await Database.template f_DeleteReturning<&CNullableTypesRow::m_Integer, gc_DeleteNullableByKey>
					(
						NStr::CStr("returning-null@example.com")
					)
				;
				DMibExpect(bool(NullReturned), ==, false);

				// A non-NULL integer_value still round-trips through the optional return type.
				CNullableTypesRow ValueReturningRow;
				ValueReturningRow.m_Key = "returning-value@example.com";
				ValueReturningRow.m_Integer = 7;
				co_await Database.template f_Insert<gc_InsertNullableTypes>(fg_TempCopy(ValueReturningRow));

				NStorage::TCOptional<int32> ValueReturned = co_await Database.template f_DeleteReturning<&CNullableTypesRow::m_Integer, gc_DeleteNullableByKey>
					(
						NStr::CStr("returning-value@example.com")
					)
				;
				DMibExpect(bool(ValueReturned), ==, true);
				if (ValueReturned)
					DMibExpect(*ValueReturned, ==, int32(7));
			}
			else
			{
				fg_TestExpectSqlError(UpsertResult, "SQLite typed mutation RETURNING", ESqlErrorCategory::mc_Generic);
			}

			co_return {};
		};

		co_return {};
	}
}
