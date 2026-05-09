// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTableAndPreparedInserts(CTestDatabaseClient _Database)
	{
		DMibTestCategory("Table insert with implicit columns") -> NConcurrency::TCFuture<void>
		{
			static_assert(!NPrivate::fg_SqlTableInsertValuesMatch<decltype(gc_ProfileTable), ch8 const (&)[26], ch8 const (&)[19]>());
			static_assert(!NPrivate::fg_SqlTableInsertValuesMatch<decltype(gc_ProfileTable), ch8 const (&)[26]>());
			static_assert(NPrivate::fg_SqlPreparedInsertValuesMatch<gc_InsertProfile, ch8 const (&)[26], ch8 const (&)[19]>());

			co_await _Database.template f_Insert<gc_InsertProfile>("profile-table@example.com", "Table Profile Name");

			auto Rows = _Database.template f_Query<gc_SelectProfileByEmail>(NStr::CStr("profile-table@example.com"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("profile-table@example.com"));
					DMibExpect(pRow->m_Data.m_DisplayName, ==, NStr::CStr("Table Profile Name"));
				}
			}

			DMibExpect(nRows, ==, umint(1));

			auto SelectedActual = co_await fg_TestSqlQuerySingle<gc_SelectProfileEmailAndDisplayNameByEmail>(&_Database, "selected profile columns", NStr::CStr("profile-table@example.com"));
			DMibExpect(fg_Get<0>(SelectedActual), ==, NStr::CStr("profile-table@example.com"));
			DMibExpect(fg_Get<1>(SelectedActual), ==, NStr::CStr("Table Profile Name"));

			auto ProjectedActual = co_await _Database.template f_QueryOneAs
				<
					gc_SelectProfileEmailAndDisplayNameByEmail
					, CProfileProjection
					, &CProfileRow::m_Email
					, &CProfileRow::m_DisplayName
				>
				(NStr::CStr("profile-table@example.com"))
			;
			DMibExpect(ProjectedActual.m_Email, ==, NStr::CStr("profile-table@example.com"));
			DMibExpect(ProjectedActual.m_DisplayName, ==, NStr::CStr("Table Profile Name"));

			auto EmailActual = co_await _Database.template f_QueryOneAs<gc_SelectProfileEmailAndDisplayNameByEmail, NStr::CStr, &CProfileRow::m_Email>
				(
					NStr::CStr("profile-table@example.com")
				)
			;
			DMibExpect(EmailActual, ==, NStr::CStr("profile-table@example.com"));

			auto TupleActual = co_await _Database.template f_QueryOneAs
				<
					gc_SelectProfileEmailAndDisplayNameByEmail
					, NStorage::TCTuple<NStr::CStr, NStr::CStr>
					, &CProfileRow::m_Email
					, &CProfileRow::m_DisplayName
				>
				(NStr::CStr("profile-table@example.com"))
			;
			DMibExpect(fg_Get<0>(TupleActual), ==, NStr::CStr("profile-table@example.com"));
			DMibExpect(fg_Get<1>(TupleActual), ==, NStr::CStr("Table Profile Name"));

			co_await _Database.template f_Insert<gc_InsertProfile>(NStr::CStr("profile-order-a@example.com"), NStr::CStr("zeta"));
			co_await _Database.template f_Insert<gc_InsertProfile>(NStr::CStr("profile-order-a@example.com"), NStr::CStr("alpha"));
			co_await _Database.template f_Insert<gc_InsertProfile>(NStr::CStr("profile-order-b@example.com"), NStr::CStr("alpha"));

			auto OrderedRows = _Database.template f_Query<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("profile-order-%"));
			NContainer::TCVector<NStr::CStr> OrderedProfiles;

			for (auto iBatch = co_await fg_Move(OrderedRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					OrderedProfiles.f_InsertLast(pRow->m_Data.m_Email);
					OrderedProfiles.f_InsertLast(pRow->m_Data.m_DisplayName);
				}
			}

			DMibExpect(OrderedProfiles.f_GetLen(), ==, umint(6));
			if (OrderedProfiles.f_GetLen() == 6)
			{
				DMibExpect(OrderedProfiles[0], ==, NStr::CStr("profile-order-a@example.com"));
				DMibExpect(OrderedProfiles[1], ==, NStr::CStr("zeta"));
				DMibExpect(OrderedProfiles[2], ==, NStr::CStr("profile-order-a@example.com"));
				DMibExpect(OrderedProfiles[3], ==, NStr::CStr("alpha"));
				DMibExpect(OrderedProfiles[4], ==, NStr::CStr("profile-order-b@example.com"));
				DMibExpect(OrderedProfiles[5], ==, NStr::CStr("alpha"));
			}

			co_return {};
		};

		DMibTestCategory("Prepared insert with two columns") -> NConcurrency::TCFuture<void>
		{
			CProfileRow Profile;
			Profile.m_Email = "profile@example.com";
			Profile.m_DisplayName = "Profile Name";

			co_await _Database.template f_Insert<gc_InsertProfileEmailAndDisplayName>(Profile.m_Email, Profile.m_DisplayName);

			auto Rows = _Database.template f_Query<gc_SelectProfileByEmail>(NStr::CStr("profile@example.com"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("profile@example.com"));
					DMibExpect(pRow->m_Data.m_DisplayName, ==, NStr::CStr("Profile Name"));
				}
			}

			DMibExpect(nRows, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Prepared insert with reordered columns") -> NConcurrency::TCFuture<void>
		{
			co_await _Database.template f_Insert<gc_InsertProfileDisplayNameAndEmail>(NStr::CStr("Reordered Name"), NStr::CStr("profile-reordered@example.com"));

			auto ReorderedActual = co_await fg_TestSqlQuerySingle<gc_SelectProfileEmailAndDisplayNameByEmail>
				(
					&_Database
					, "reordered insert columns"
					, NStr::CStr("profile-reordered@example.com")
				)
			;
			DMibExpect(fg_Get<0>(ReorderedActual), ==, NStr::CStr("profile-reordered@example.com"));
			DMibExpect(fg_Get<1>(ReorderedActual), ==, NStr::CStr("Reordered Name"));

			co_return {};
		};

		DMibTestCategory("Prepared insert with implicit columns") -> NConcurrency::TCFuture<void>
		{
			CProfileRow Profile;
			Profile.m_Email = "profile-implicit@example.com";
			Profile.m_DisplayName = "Implicit Profile Name";

			co_await _Database.template f_Insert<gc_InsertProfile>(Profile.m_Email, Profile.m_DisplayName);
			co_await _Database.template f_Insert<gc_InsertProfile>(fg_TempCopy(Profile));

			auto Rows = _Database.template f_Query<gc_SelectProfileByEmail>(NStr::CStr("profile-implicit@example.com"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("profile-implicit@example.com"))(ETestFlag_Aggregated);
					DMibExpect(pRow->m_Data.m_DisplayName, ==, NStr::CStr("Implicit Profile Name"))(ETestFlag_Aggregated);
				}
			}

			DMibExpect(nRows, ==, umint(2));

			co_return {};
		};

		DMibTestCategory("Bulk insert through f_InsertMany") -> NConcurrency::TCFuture<void>
		{
			NContainer::TCVector<CProfileRow> Rows;
			for (umint iIteration = 0; iIteration < 10; ++iIteration)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("bulk-{}@example.com") << iIteration;
				Profile.m_DisplayName = NStr::CStr::CFormat("Bulk Profile {}") << iIteration;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await _Database.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows)));
			DMibExpect(nInserted, ==, umint(10));

			umint nCount = co_await _Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("bulk-%"));
			DMibExpect(nCount, ==, umint(10));

			co_return {};
		};

		DMibTestCategory("Bulk insert with reordered columns") -> NConcurrency::TCFuture<void>
		{
			NContainer::TCVector<CProfileRow> Rows;
			for (umint iIteration = 0; iIteration < 5; ++iIteration)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("bulk-reordered-{}@example.com") << iIteration;
				Profile.m_DisplayName = NStr::CStr::CFormat("Bulk Reordered {}") << iIteration;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await _Database.template f_InsertMany<gc_InsertProfileDisplayNameAndEmail>(fg_VectorAsBatchGenerator(fg_Move(Rows)));
			DMibExpect(nInserted, ==, umint(5));

			for (umint iIteration = 0; iIteration < 5; ++iIteration)
			{
				NStr::CStr Email = NStr::CStr::CFormat("bulk-reordered-{}@example.com") << iIteration;
				auto Actual = co_await fg_TestSqlQuerySingle<gc_SelectProfileEmailAndDisplayNameByEmail>
					(
						&_Database
						, NStr::CStr(NStr::CStr::CFormat("bulk reordered insert columns {}") << iIteration)
						, fg_TempCopy(Email)
					)
				;
				DMibExpect(fg_Get<0>(Actual), ==, Email)(ETestFlag_Aggregated);
				DMibExpect(fg_Get<1>(Actual), ==, NStr::CStr(NStr::CStr::CFormat("Bulk Reordered {}") << iIteration))(ETestFlag_Aggregated);
			}

			co_return {};
		};

		DMibTestCategory("Bulk insert in transaction") -> NConcurrency::TCFuture<void>
		{
			NContainer::TCVector<CProfileRow> Rows;
			for (umint iIteration = 0; iIteration < 5; ++iIteration)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("bulk-tx-{}@example.com") << iIteration;
				Profile.m_DisplayName = NStr::CStr::CFormat("Tx Profile {}") << iIteration;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			co_await _Database.f_WithTransaction
				(
					[Rows = fg_Move(Rows)](CSqlTransaction _Transaction) mutable -> NConcurrency::TCFuture<void>
					{
						co_await _Transaction.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows)));

						co_return {};
					}
				)
			;

			umint nCount = co_await _Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("bulk-tx-%"));
			DMibExpect(nCount, ==, umint(5));

			co_return {};
		};

		DMibTestCategory("Failed bulk insert leaves the transaction usable") -> NConcurrency::TCFuture<void>
		{
			co_await _Database.f_WithTransaction
				(
					[](CSqlTransaction _Transaction) mutable -> NConcurrency::TCFuture<void>
					{
						NContainer::TCVector<CProfileRow> Rows;
						for (umint iIteration = 0; iIteration < 3; ++iIteration)
						{
							CProfileRow Profile;
							Profile.m_Email = NStr::CStr::CFormat("bulk-fail-{}@example.com") << iIteration;
							Profile.m_DisplayName = NStr::CStr::CFormat("Bulk Fail {}") << iIteration;
							Rows.f_InsertLast(fg_Move(Profile));
						}

						// The generator streams a few one-row batches - so the client writes Bind/Execute messages - and
						// then fails. On PostgreSQL the bulk pipeline must resynchronise the protocol after this failure;
						// otherwise the follow-up insert below would consume the stale bulk responses (or hang).
						auto BulkResult = co_await _Transaction.template f_InsertMany<gc_InsertProfile>(fg_FailingBatchGenerator(fg_Move(Rows))).f_Wrap();
						DMibExpect(bool(BulkResult), ==, false);

						co_await _Transaction.template f_Insert<gc_InsertProfile>(NStr::CStr("bulk-fail-after@example.com"), NStr::CStr("After Failure"));

						co_return {};
					}
				)
			;

			umint nCount = co_await _Database.template f_Count<gc_SelectProfileByEmail>(NStr::CStr("bulk-fail-after@example.com"));
			DMibExpect(nCount, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Failed bulk insert outside a transaction inserts nothing") -> NConcurrency::TCFuture<void>
		{
			NContainer::TCVector<CProfileRow> Rows;
			for (umint iIteration = 0; iIteration < 3; ++iIteration)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("bulk-fail-standalone-{}@example.com") << iIteration;
				Profile.m_DisplayName = NStr::CStr::CFormat("Bulk Fail Standalone {}") << iIteration;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			auto BulkResult = co_await _Database.template f_InsertMany<gc_InsertProfile>(fg_FailingBatchGenerator(fg_Move(Rows))).f_Wrap();
			DMibExpect(bool(BulkResult), ==, false);

			umint nCount = co_await _Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("bulk-fail-standalone-%"));
			DMibExpect(nCount, ==, umint(0));

			co_return {};
		};

		DMibTestCategory("Bulk insert spanning multiple pipeline windows") -> NConcurrency::TCFuture<void>
		{
			constexpr umint c_nRowsAcrossPipelineWindows = 40;

			NContainer::TCVector<CProfileRow> Rows;
			for (umint iIteration = 0; iIteration < c_nRowsAcrossPipelineWindows; ++iIteration)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("pipeline-window-{}@example.com") << iIteration;
				Profile.m_DisplayName = NStr::CStr::CFormat("Pipeline Window Profile {}") << iIteration;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await _Database.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows), 1));
			DMibExpect(nInserted, ==, c_nRowsAcrossPipelineWindows);

			umint nCount = co_await _Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("pipeline-window-%"));
			DMibExpect(nCount, ==, c_nRowsAcrossPipelineWindows);

			co_return {};
		};

		DMibTestCategory("Bulk insert of a single large batch") -> NConcurrency::TCFuture<void>
		{
			// One large batch occupies a single pipeline slot regardless of its row count
			NContainer::TCVector<CProfileRow> Rows;
			for (umint iIteration = 0; iIteration < 700; ++iIteration)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("large-batch-{}@example.com") << iIteration;
				Profile.m_DisplayName = NStr::CStr::CFormat("Large Batch Profile {}") << iIteration;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await _Database.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows), 700));
			DMibExpect(nInserted, ==, umint(700));

			umint nCount = co_await _Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("large-batch-%"));
			DMibExpect(nCount, ==, umint(700));

			co_return {};
		};

		DMibTestCategory("Bulk insert honors explicit column subset") -> NConcurrency::TCFuture<void>
		{
			// The unselected fields are populated so the test can tell whether the binder ignores them
			NContainer::TCVector<CNullableTypesRow> Rows;
			for (umint iIteration = 0; iIteration < 3; ++iIteration)
			{
				CNullableTypesRow Row;
				Row.m_Key = NStr::CStr::CFormat("subset-{}") << iIteration;
				Row.m_Integer = int32(100 + iIteration);
				Row.m_Text = NStr::CStr("ignored-text");
				Rows.f_InsertLast(fg_Move(Row));
			}

			umint nInserted = co_await _Database.template f_InsertMany<gc_InsertNullableTypesKeyAndInteger>(fg_VectorAsBatchGenerator(fg_Move(Rows)));
			DMibExpect(nInserted, ==, umint(3));

			auto Stored = co_await _Database.template f_QueryOne<gc_SelectNullableTypesByKey>(NStr::CStr("subset-1"));
			DMibExpect(bool(Stored.m_Integer), ==, true)(ETestFlag_Aggregated);
			if (Stored.m_Integer)
				DMibExpect(*Stored.m_Integer, ==, int32(101))(ETestFlag_Aggregated);
			DMibExpect(bool(Stored.m_Text), ==, false)(ETestFlag_Aggregated);

			// The nullable_types table is shared with other categories that count rows by nullability, so remove the
			// rows inserted here.
			co_await _Database.f_ExecuteRaw(NStr::CStr("DELETE FROM nullable_types WHERE key LIKE 'subset-%'"));

			co_return {};
		};

		co_return {};
	}
}
