// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseMutationsAndUpsert(CTestDatabaseClient _Database)
	{
		DMibTestCategory("Explicit full-table mutations") -> NConcurrency::TCFuture<void>
		{
			co_await _Database.f_Insert<gc_InsertFullMutation>(NStr::CStr("full-a"));
			co_await _Database.f_Insert<gc_InsertFullMutation>(NStr::CStr("full-b"));

			DMibExpect(co_await _Database.template f_Update<gc_UpdateAllFullMutationLabels>(NStr::CStr("full-updated")), ==, umint(2));
			DMibExpect(co_await _Database.template f_Delete<gc_DeleteAllFullMutationRows>(), ==, umint(2));

			DMibExpect(co_await _Database.template f_Delete<gc_DeleteAllFullMutationRows>(), ==, umint(0));

			co_return {};
		};

		DMibTestCategory("Portable upsert") -> NConcurrency::TCFuture<void>
		{
			DMibExpect(co_await _Database.template f_Upsert<gc_UpsertPersonDisplayNameByEmail>(NStr::CStr("upsert@example.com"), NStr::CStr("Initial Name")), ==, umint(1));

			auto Initial = co_await fg_TestSqlQuerySingle<gc_SelectUpsertPersonByEmail>(&_Database, "initial upsert", NStr::CStr("upsert@example.com"));
			DMibExpect(Initial.m_DisplayName, ==, NStr::CStr("Initial Name"));

			DMibExpect(co_await _Database.template f_Upsert<gc_UpsertPersonDisplayNameByEmail>(NStr::CStr("upsert@example.com"), NStr::CStr("Updated Name")), ==, umint(1));

			auto Updated = co_await fg_TestSqlQuerySingle<gc_SelectUpsertPersonByEmail>(&_Database, "conflict upsert", NStr::CStr("upsert@example.com"));
			DMibExpect(Updated.m_DisplayName, ==, NStr::CStr("Updated Name"));

			co_return {};
		};

		DMibTestCategory("Typed mutation returning") -> NConcurrency::TCFuture<void>
		{
			using CStrResult = NConcurrency::TCAsyncResult<NStr::CStr>;

			CStrResult UpsertResult = co_await _Database.template f_UpsertReturning<&CUpsertRow::m_DisplayName, gc_UpsertPersonDisplayNameByEmail>
				(
					NStr::CStr("returning@example.com")
					, NStr::CStr("Returning Initial")
				)
				.f_Wrap()
			;
			if (UpsertResult)
			{
				DMibExpect(*UpsertResult, ==, NStr::CStr("Returning Initial"));

				NStr::CStr UpdatedName = co_await _Database.template f_UpdateReturning<&CUpsertRow::m_DisplayName, gc_UpdateUpsertPersonDisplayNameByEmail>
					(
						NStr::CStr("Returning Updated")
						, NStr::CStr("returning@example.com")
					)
				;
				DMibExpect(UpdatedName, ==, NStr::CStr("Returning Updated"));

				NStr::CStr DeletedEmail = co_await _Database.template f_DeleteReturning<&CUpsertRow::m_Email, gc_DeleteUpsertPersonByEmail>(NStr::CStr("returning@example.com"));
				DMibExpect(DeletedEmail, ==, NStr::CStr("returning@example.com"));

				auto MissingUpdateResult = co_await _Database.template f_UpdateReturning<&CUpsertRow::m_DisplayName, gc_UpdateUpsertPersonDisplayNameByEmail>
					(
						NStr::CStr("Returning Missing")
						, NStr::CStr("returning@example.com")
					)
					.f_Wrap()
				;
				fg_TestExpectSqlError(MissingUpdateResult, "update returning without a matching row", ESqlErrorCategory::mc_MissingRow);

				auto MissingDeleteResult = co_await _Database.template f_DeleteReturning<&CUpsertRow::m_Email, gc_DeleteUpsertPersonByEmail>(NStr::CStr("returning@example.com")).f_Wrap();
				fg_TestExpectSqlError(MissingDeleteResult, "delete returning without a matching row", ESqlErrorCategory::mc_MissingRow);

				// SQL NULL in a RETURNING column maps to an empty TCOptional, not a conversion error
				CNullableTypesRow NullReturningRow;
				NullReturningRow.m_Key = "returning-null@example.com";
				co_await _Database.template f_Insert<gc_InsertNullableTypes>(fg_TempCopy(NullReturningRow));

				NStorage::TCOptional<int32> NullReturned = co_await _Database.template f_DeleteReturning<&CNullableTypesRow::m_Integer, gc_DeleteNullableByKey>
					(
						NStr::CStr("returning-null@example.com")
					)
				;
				DMibExpect(bool(NullReturned), ==, false);

				CNullableTypesRow ValueReturningRow;
				ValueReturningRow.m_Key = "returning-value@example.com";
				ValueReturningRow.m_Integer = 7;
				co_await _Database.template f_Insert<gc_InsertNullableTypes>(fg_TempCopy(ValueReturningRow));

				NStorage::TCOptional<int32> ValueReturned = co_await _Database.template f_DeleteReturning<&CNullableTypesRow::m_Integer, gc_DeleteNullableByKey>
					(
						NStr::CStr("returning-value@example.com")
					)
				;
				DMibExpect(bool(ValueReturned), ==, true);
				if (ValueReturned)
					DMibExpect(*ValueReturned, ==, int32(7));

				// A predicate matching several rows fails the mutation without changing any of them
				co_await _Database.f_Insert<gc_InsertFullMutation>(NStr::CStr("returning-multi-a"));
				co_await _Database.f_Insert<gc_InsertFullMutation>(NStr::CStr("returning-multi-b"));

				auto MultiUpdateResult = co_await _Database.template f_UpdateReturning<&CFullMutationRow::m_Label, gc_UpdateAllFullMutationLabels>(NStr::CStr("returning-multi-updated"))
					.f_Wrap()
				;
				DMibExpect(bool(MultiUpdateResult), ==, false);

				auto MultiDeleteResult = co_await _Database.template f_DeleteReturning<&CFullMutationRow::m_Label, gc_DeleteAllFullMutationRows>().f_Wrap();
				DMibExpect(bool(MultiDeleteResult), ==, false);

				CSqlRawResult Labels = co_await _Database.f_QueryRaw(NStr::CStr("SELECT label FROM full_mutation_rows ORDER BY label"));
				DMibAssert(Labels.m_Rows.f_GetLen(), ==, umint(2));
				DMibExpect(Labels.m_Rows[0].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("returning-multi-a"));
				DMibExpect(Labels.m_Rows[1].m_Values[0].f_GetAsType<NStr::CStr>(), ==, NStr::CStr("returning-multi-b"));

				DMibExpect(co_await _Database.template f_Delete<gc_DeleteAllFullMutationRows>(), ==, umint(2));
			}
			else
				fg_TestExpectSqlError(UpsertResult, "SQLite typed mutation RETURNING", ESqlErrorCategory::mc_Generic);

			co_return {};
		};

		co_return {};
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseUpsertIntoTableNamedExcluded(FCreateBackend _fCreateBackend)
	{
		CSqlDatabaseClient Database(_fCreateBackend(&gc_ExcludedNameSchemaVersions));
		co_await Database.f_Open();
		co_await Database.f_ApplySchema();

		co_await Database.template f_Upsert<gc_UpsertExcludedNameByEmail>(NStr::CStr("excluded@example.com"), NStr::CStr("Initial Name"));
		co_await Database.template f_Upsert<gc_UpsertExcludedNameByEmail>(NStr::CStr("excluded@example.com"), NStr::CStr("Updated Name"));

		auto Row = co_await Database.template f_QueryOne<gc_SelectExcludedNameByEmail>(NStr::CStr("excluded@example.com"));
		DMibExpect(Row.m_DisplayName, ==, NStr::CStr("Updated Name"));

		auto Count = co_await Database.f_QueryRaw(NStr::CStr("SELECT count(*) AS value FROM \"excluded\""));
		DMibExpect(Count.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

		co_return {};
	}
}
