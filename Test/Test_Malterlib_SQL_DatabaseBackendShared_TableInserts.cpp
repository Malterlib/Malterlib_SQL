// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTableAndPreparedInserts(CTestDatabaseClient *_pDatabase)
	{
		auto &Database = *_pDatabase;

		DMibTestCategory("Table insert with implicit columns") -> NConcurrency::TCFuture<void>
		{
			static_assert(!NPrivate::fg_SqlTableInsertValuesMatch<decltype(gc_ProfileTable), ch8 const (&)[26], ch8 const (&)[19]>());
			static_assert(!NPrivate::fg_SqlTableInsertValuesMatch<decltype(gc_ProfileTable), ch8 const (&)[26]>());
			static_assert(NPrivate::fg_SqlPreparedInsertValuesMatch<gc_InsertProfile, ch8 const (&)[26], ch8 const (&)[19]>());

			co_await Database.template f_Insert<gc_InsertProfile>("profile-table@example.com", "Table Profile Name");

			auto Rows = Database.template f_Query<gc_SelectProfileByEmail>(NStr::CStr("profile-table@example.com"));
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

			auto SelectedActual = co_await fg_TestSqlQuerySingle<gc_SelectProfileEmailAndDisplayNameByEmail>(&Database, "selected profile columns", NStr::CStr("profile-table@example.com"));
			DMibExpect(fg_Get<0>(SelectedActual), ==, NStr::CStr("profile-table@example.com"));
			DMibExpect(fg_Get<1>(SelectedActual), ==, NStr::CStr("Table Profile Name"));

			auto ProjectedActual = co_await Database.template f_QueryOneAs
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

			auto EmailActual = co_await Database.template f_QueryOneAs<gc_SelectProfileEmailAndDisplayNameByEmail, NStr::CStr, &CProfileRow::m_Email>
				(
					NStr::CStr("profile-table@example.com")
				)
			;
			DMibExpect(EmailActual, ==, NStr::CStr("profile-table@example.com"));

			auto TupleActual = co_await Database.template f_QueryOneAs
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

			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("profile-order-a@example.com"), NStr::CStr("zeta"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("profile-order-a@example.com"), NStr::CStr("alpha"));
			co_await Database.template f_Insert<gc_InsertProfile>(NStr::CStr("profile-order-b@example.com"), NStr::CStr("alpha"));

			auto OrderedRows = Database.template f_Query<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("profile-order-%"));
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

			co_await Database.template f_Insert<gc_InsertProfileEmailAndDisplayName>(Profile.m_Email, Profile.m_DisplayName);

			auto Rows = Database.template f_Query<gc_SelectProfileByEmail>(NStr::CStr("profile@example.com"));
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
			// gc_InsertProfileDisplayNameAndEmail lists display_name before email (reverse of the declaration order);
			// the supplied values follow that order. Each value must still land in its own column - if the prepared
			// columns were described in table order instead, email would receive the display name and the lookup below
			// (keyed on the email) would find nothing.
			co_await Database.template f_Insert<gc_InsertProfileDisplayNameAndEmail>(NStr::CStr("Reordered Name"), NStr::CStr("profile-reordered@example.com"));

			auto ReorderedActual = co_await fg_TestSqlQuerySingle<gc_SelectProfileEmailAndDisplayNameByEmail>
				(
					&Database
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

			co_await Database.template f_Insert<gc_InsertProfile>(Profile.m_Email, Profile.m_DisplayName);
			co_await Database.template f_Insert<gc_InsertProfile>(fg_TempCopy(Profile));

			auto Rows = Database.template f_Query<gc_SelectProfileByEmail>(NStr::CStr("profile-implicit@example.com"));
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
			for (umint i = 0; i < 10; ++i)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("bulk-{}@example.com") << i;
				Profile.m_DisplayName = NStr::CStr::CFormat("Bulk Profile {}") << i;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await Database.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows)));
			DMibExpect(nInserted, ==, umint(10));

			umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("bulk-%"));
			DMibExpect(nCount, ==, umint(10));

			co_return {};
		};

		DMibTestCategory("Bulk insert with reordered columns") -> NConcurrency::TCFuture<void>
		{
			// gc_InsertProfileDisplayNameAndEmail lists display_name before email (reverse of the declaration order).
			// f_Describe emits the placeholders in that requested order, so the bulk row binder must read each row's
			// members in the same order - not table-declaration order. If it walked the table instead, every row's
			// email value would bind to the display_name placeholder (and vice versa), so the email column would hold
			// the display name and the per-email lookups below would all miss.
			NContainer::TCVector<CProfileRow> Rows;
			for (umint i = 0; i < 5; ++i)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("bulk-reordered-{}@example.com") << i;
				Profile.m_DisplayName = NStr::CStr::CFormat("Bulk Reordered {}") << i;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await Database.template f_InsertMany<gc_InsertProfileDisplayNameAndEmail>(fg_VectorAsBatchGenerator(fg_Move(Rows)));
			DMibExpect(nInserted, ==, umint(5));

			for (umint i = 0; i < 5; ++i)
			{
				NStr::CStr Email = NStr::CStr::CFormat("bulk-reordered-{}@example.com") << i;
				auto Actual = co_await fg_TestSqlQuerySingle<gc_SelectProfileEmailAndDisplayNameByEmail>
					(
						&Database
						, NStr::CStr(NStr::CStr::CFormat("bulk reordered insert columns {}") << i)
						, fg_TempCopy(Email)
					)
				;
				DMibExpect(fg_Get<0>(Actual), ==, Email)(ETestFlag_Aggregated);
				DMibExpect(fg_Get<1>(Actual), ==, NStr::CStr(NStr::CStr::CFormat("Bulk Reordered {}") << i))(ETestFlag_Aggregated);
			}

			co_return {};
		};

		DMibTestCategory("Bulk insert in transaction") -> NConcurrency::TCFuture<void>
		{
			NContainer::TCVector<CProfileRow> Rows;
			for (umint i = 0; i < 5; ++i)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("bulk-tx-{}@example.com") << i;
				Profile.m_DisplayName = NStr::CStr::CFormat("Tx Profile {}") << i;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			co_await Database.f_WithTransaction
				(
					[Rows = fg_Move(Rows)](CSqlTransaction Transaction) mutable -> NConcurrency::TCFuture<void>
					{
						co_await Transaction.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows)));

						co_return {};
					}
				)
			;

			umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("bulk-tx-%"));
			DMibExpect(nCount, ==, umint(5));

			co_return {};
		};

		DMibTestCategory("Failed bulk insert leaves the transaction usable") -> NConcurrency::TCFuture<void>
		{
			co_await Database.f_WithTransaction
				(
					[](CSqlTransaction Transaction) mutable -> NConcurrency::TCFuture<void>
					{
						NContainer::TCVector<CProfileRow> Rows;
						for (umint i = 0; i < 3; ++i)
						{
							CProfileRow Profile;
							Profile.m_Email = NStr::CStr::CFormat("bulk-fail-{}@example.com") << i;
							Profile.m_DisplayName = NStr::CStr::CFormat("Bulk Fail {}") << i;
							Rows.f_InsertLast(fg_Move(Profile));
						}

						// The generator streams a few one-row batches - so the client writes Bind/Execute messages - and
						// then fails. On PostgreSQL the bulk pipeline must resynchronise the protocol after this failure;
						// otherwise the follow-up insert below would consume the stale bulk responses (or hang).
						auto BulkResult = co_await Transaction.template f_InsertMany<gc_InsertProfile>(fg_FailingBatchGenerator(fg_Move(Rows))).f_Wrap();
						DMibExpect(bool(BulkResult), ==, false);

						co_await Transaction.template f_Insert<gc_InsertProfile>(NStr::CStr("bulk-fail-after@example.com"), NStr::CStr("After Failure"));

						co_return {};
					}
				)
			;

			// The follow-up insert committed, proving the connection was usable after the failed bulk pipeline.
			umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmail>(NStr::CStr("bulk-fail-after@example.com"));
			DMibExpect(nCount, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Bulk insert spanning multiple pipeline windows") -> NConcurrency::TCFuture<void>
		{
			// One row per batch produces far more batches than the pipeline window, so on PostgreSQL the client Flushes
			// and drains backend responses several times instead of sending the whole stream before reading any reply
			// (which would let an unbounded number of commands sit in flight). Every row must still be inserted once.
			NContainer::TCVector<CProfileRow> Rows;
			for (umint i = 0; i < 40; ++i)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("pipeline-window-{}@example.com") << i;
				Profile.m_DisplayName = NStr::CStr::CFormat("Pipeline Window Profile {}") << i;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await Database.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows), 1));
			DMibExpect(nInserted, ==, umint(40));

			umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("pipeline-window-%"));
			DMibExpect(nCount, ==, umint(40));

			co_return {};
		};

		DMibTestCategory("Bulk insert of a single large batch") -> NConcurrency::TCFuture<void>
		{
			// A single batch is held as one in-flight window slot, so a large batch is sent as one set of Bind/Execute
			// commands and drained together (on PostgreSQL, by the final Sync). Insert many rows in one batch and
			// verify every row lands - exercising a large single send and drain rather than many small batches.
			NContainer::TCVector<CProfileRow> Rows;
			for (umint i = 0; i < 700; ++i)
			{
				CProfileRow Profile;
				Profile.m_Email = NStr::CStr::CFormat("large-batch-{}@example.com") << i;
				Profile.m_DisplayName = NStr::CStr::CFormat("Large Batch Profile {}") << i;
				Rows.f_InsertLast(fg_Move(Profile));
			}

			umint nInserted = co_await Database.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows), 700));
			DMibExpect(nInserted, ==, umint(700));

			umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("large-batch-%"));
			DMibExpect(nCount, ==, umint(700));

			co_return {};
		};

		DMibTestCategory("Bulk insert honors explicit column subset") -> NConcurrency::TCFuture<void>
		{
			// A prepared insert that selects an explicit subset of columns must bind only those columns even on the
			// whole-row bulk path. The rows carry values for unselected columns too; those must be ignored (left NULL)
			// rather than binding more values than the prepared INSERT statement has placeholders.
			NContainer::TCVector<CNullableTypesRow> Rows;
			for (umint i = 0; i < 3; ++i)
			{
				CNullableTypesRow Row;
				Row.m_Key = NStr::CStr::CFormat("subset-{}") << i;
				Row.m_Integer = int32(100 + i);
				Row.m_Text = NStr::CStr("ignored-text");
				Rows.f_InsertLast(fg_Move(Row));
			}

			umint nInserted = co_await Database.template f_InsertMany<gc_InsertNullableTypesKeyAndInteger>(fg_VectorAsBatchGenerator(fg_Move(Rows)));
			DMibExpect(nInserted, ==, umint(3));

			auto Stored = co_await Database.template f_QueryOne<gc_SelectNullableTypesByKey>(NStr::CStr("subset-1"));
			DMibExpect(bool(Stored.m_Integer), ==, true)(ETestFlag_Aggregated);
			if (Stored.m_Integer)
				DMibExpect(*Stored.m_Integer, ==, int32(101))(ETestFlag_Aggregated);
			// text_value was not in the explicit column set, so it must have stayed NULL despite the row carrying it.
			DMibExpect(bool(Stored.m_Text), ==, false)(ETestFlag_Aggregated);

			// The nullable_types table is shared with other categories that count rows by nullability, so remove the
			// rows inserted here.
			co_await Database.f_ExecuteRaw(NStr::CStr("DELETE FROM nullable_types WHERE key LIKE 'subset-%'"));

			co_return {};
		};

		co_return {};
	}
}
