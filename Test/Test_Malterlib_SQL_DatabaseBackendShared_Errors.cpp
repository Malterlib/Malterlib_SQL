// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseErrorModel(CTestDatabaseClient *_pDatabase)
	{
		auto &Database = *_pDatabase;

		DMibTestCategory("Structured SQL errors") -> NConcurrency::TCFuture<void>
		{
			co_await Database.template f_Insert<gc_InsertSchemaUser>(NStr::CStr("sql-error-duplicate@example.com"), NStorage::TCOptional<NStr::CStr>(), false);

			auto DuplicateResult = co_await Database.template f_Insert<gc_InsertSchemaUser>
				(
					NStr::CStr("sql-error-duplicate@example.com")
					, NStorage::TCOptional<NStr::CStr>()
					, false
				)
				.f_Wrap()
			;
			fg_TestExpectSqlError(DuplicateResult, "duplicate key", ESqlErrorCategory::mc_DuplicateKey);

			auto ForeignKeyResult = co_await Database.template f_Insert<gc_InsertUserRole>(uint64(999999), NStr::CStr("missing-user-role")).f_Wrap();
			fg_TestExpectSqlError(ForeignKeyResult, "foreign key", ESqlErrorCategory::mc_ForeignKeyViolation);

			co_return {};
		};

		DMibTestCategory("SQL retry classification")
		{
			CSqlErrorData SerializationError = fg_SqlErrorData(ESqlErrorCategory::mc_SerializationFailure, ESqlErrorRetryClass::mc_RetryTransaction);
			DMibExpect(fg_SqlErrorIsTransient(SerializationError), ==, true);

			CSqlErrorData ConnectionError = fg_SqlErrorData(ESqlErrorCategory::mc_ConnectionLoss, ESqlErrorRetryClass::mc_RetryConnection);
			DMibExpect(fg_SqlErrorIsTransient(ConnectionError), ==, true);

			CSqlErrorData DuplicateError = fg_SqlErrorData(ESqlErrorCategory::mc_DuplicateKey);
			DMibExpect(fg_SqlErrorIsTransient(DuplicateError), ==, false);
		};

		co_return {};
	}
}
