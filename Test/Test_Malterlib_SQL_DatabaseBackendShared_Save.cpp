// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseSave(CTestDatabaseClient *_pDatabase)
	{
		auto &Database = *_pDatabase;

		DMibTestCategory("Concurrency-aware save") -> NConcurrency::TCFuture<void>
		{
			auto SaveRepository = Database.template f_Repository<gc_SaveRepository>();

			CSaveRow Row;
			Row.m_Email = "save@example.com";
			Row.m_DisplayName = "Saved";

			auto InsertResult = co_await SaveRepository.f_Save(fg_Move(Row));
			DMibExpect(InsertResult.m_Result, ==, ESqlSaveResult::mc_Inserted);
			DMibExpect(InsertResult.m_Row.m_ID, >, int64(0));
			DMibExpect(InsertResult.m_Row.m_Version, ==, int64(1));

			CSaveRow Updated = InsertResult.m_Row;
			Updated.m_DisplayName = "Updated";

			auto UpdateResult = co_await SaveRepository.f_Save(fg_Move(Updated));
			DMibExpect(UpdateResult.m_Result, ==, ESqlSaveResult::mc_Updated);
			DMibExpect(UpdateResult.m_Row.m_Version, ==, int64(2));

			auto SavedByID = co_await SaveRepository.f_Get(UpdateResult.m_Row.m_ID);
			DMibExpect(bool(SavedByID), ==, true);
			if (SavedByID)
			{
				DMibExpect(SavedByID->m_DisplayName, ==, NStr::CStr("Updated"));
				DMibExpect(SavedByID->m_Version, ==, int64(2));
			}

			CSaveRow Stale = InsertResult.m_Row;
			Stale.m_DisplayName = "Stale";

			auto StaleResult = co_await SaveRepository.f_Save(fg_Move(Stale));
			DMibExpect(StaleResult.m_Result, ==, ESqlSaveResult::mc_StaleOrMissing);
			DMibExpect(StaleResult.m_Row.m_Version, ==, int64(1));

			CSaveRow Missing;
			Missing.m_ID = 999999;
			Missing.m_Version = 1;
			Missing.m_Email = "missing-save@example.com";
			Missing.m_DisplayName = "Missing";

			auto MissingResult = co_await SaveRepository.f_Save(fg_Move(Missing));
			DMibExpect(MissingResult.m_Result, ==, ESqlSaveResult::mc_StaleOrMissing);

			umint nDeleted = co_await SaveRepository.f_Delete(UpdateResult.m_Row.m_ID);
			DMibExpect(nDeleted, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Concurrency-aware save in transaction") -> NConcurrency::TCFuture<void>
		{
			auto Transaction = co_await Database.f_BeginTransaction();
			auto SaveRepository = Transaction.template f_Repository<gc_SaveRepository>();

			CSaveRow Row;
			Row.m_Email = "transaction-save@example.com";
			Row.m_DisplayName = "Transaction Saved";

			auto InsertResult = co_await SaveRepository.f_Save(fg_Move(Row));
			DMibExpect(InsertResult.m_Result, ==, ESqlSaveResult::mc_Inserted);
			DMibExpect(InsertResult.m_Row.m_Version, ==, int64(1));

			CSaveRow Updated = InsertResult.m_Row;
			Updated.m_DisplayName = "Transaction Updated";

			auto UpdateResult = co_await SaveRepository.f_Save(fg_Move(Updated));
			DMibExpect(UpdateResult.m_Result, ==, ESqlSaveResult::mc_Updated);
			DMibExpect(UpdateResult.m_Row.m_Version, ==, int64(2));

			auto SavedByID = co_await SaveRepository.f_Get(UpdateResult.m_Row.m_ID);
			DMibExpect(bool(SavedByID), ==, true);
			if (SavedByID)
				DMibExpect(SavedByID->m_DisplayName, ==, NStr::CStr("Transaction Updated"));

			co_await Transaction.f_Commit();

			co_return {};
		};

		co_return {};
	}
}
