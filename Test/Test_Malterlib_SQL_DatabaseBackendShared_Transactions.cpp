// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	namespace
	{
		NConcurrency::TCFuture<void> fg_TestSqlTransactionScopeRollback(CSqlTransaction _Transaction, NStr::CStr _Email)
		{
			co_await _Transaction.f_Insert<gc_InsertPerson>(fg_Move(_Email));
			co_return DMibErrorInstance("SQL transaction scope rollback test");
		}

		NConcurrency::TCFuture<void> fg_TestSqlNestedTransactionScope
			(
				CSqlTransaction _Transaction
				, NStr::CStr _OuterEmail
				, NStr::CStr _NestedCommitEmail
				, NStr::CStr _NestedRollbackEmail
			)
		{
			co_await _Transaction.f_Insert<gc_InsertPerson>(_OuterEmail);
			co_await _Transaction.f_WithTransaction
				(
					[_NestedCommitEmail](CSqlTransaction _NestedTransaction) mutable
					{
						return _NestedTransaction.f_Insert<gc_InsertPerson>(fg_Move(_NestedCommitEmail));
					}
				)
			;

			auto NestedRollbackResult = co_await _Transaction.f_WithTransaction
				(
					[_NestedRollbackEmail](CSqlTransaction _NestedTransaction) mutable
					{
						return fg_TestSqlTransactionScopeRollback(fg_Move(_NestedTransaction), fg_Move(_NestedRollbackEmail));
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(NestedRollbackResult), ==, false);

			DMibExpect(co_await _Transaction.f_Exists<gc_SelectPersonByEmail>(fg_Move(_OuterEmail)), ==, true);
			DMibExpect
				(
					co_await _Transaction.f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-commit@example.com"))
					, ==
					, true
				)
			;
			DMibExpect
				(
					co_await _Transaction.f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-rollback@example.com"))
					, ==
					, false
				)
			;

			co_return {};
		}

		NConcurrency::TCFuture<void> fg_TestSqlTransactionStreamCancellation(CSqlTransaction _Transaction, NStr::CStr _StreamEmail, NStr::CStr _AfterStreamEmail)
		{
			for (uint64 i = 0; i < 129; ++i)
				co_await _Transaction.f_Insert<gc_InsertPerson>(_StreamEmail);

			{
				CSqlSelectSettings Settings;
				Settings.m_nRowsPerBatch = 16;

				auto Rows = _Transaction.f_Query<gc_SelectPersonByEmail>(_StreamEmail, Settings);
				auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator();
				DMibExpect(bool(iBatch), ==, true);
			}

			co_await _Transaction.f_Insert<gc_InsertPerson>(fg_Move(_AfterStreamEmail));

			co_return {};
		}
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTransactionInsertAndSelect(CTestDatabaseClient *_pDatabase, CSqlDatabaseBackendCapabilities _Capabilities)
	{
		auto &Database = *_pDatabase;

		DMibTestCategory("Transaction insert and select") -> NConcurrency::TCFuture<void>
		{
			auto Transaction = co_await Database.f_BeginTransaction();

			co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction@example.com"));

			int64 TransactionReturnedID = co_await Transaction.template f_InsertReturning<&CPersonRow::m_ID, gc_InsertPerson>(NStr::CStr("transaction-returning@example.com"));
			DMibExpect(TransactionReturnedID, >, int64(0));

			auto TransactionPersonByID = co_await Transaction.template f_GetByID<gc_PersonTable, &CPersonRow::m_ID>(TransactionReturnedID);
			DMibExpect(bool(TransactionPersonByID), ==, true);
			if (TransactionPersonByID)
				DMibExpect(TransactionPersonByID->m_Email, ==, NStr::CStr("transaction-returning@example.com"));

			umint nTransactionUpdatedByID = co_await Transaction.template f_UpdateByID<gc_PersonTable, &CPersonRow::m_ID, &CPersonRow::m_Email>
				(
					NStr::CStr("transaction-returning-updated@example.com")
					, TransactionReturnedID
				)
			;
			DMibExpect(nTransactionUpdatedByID, ==, umint(1));

			nTransactionUpdatedByID = co_await Transaction.template f_UpdateByID<gc_PersonTable, &CPersonRow::m_ID, &CPersonRow::m_Email>
				(
					NStr::CStr("transaction-missing@example.com")
					, int64(0)
				)
			;
			DMibExpect(nTransactionUpdatedByID, ==, umint(0));

			int64 TransactionDeleteID = co_await Transaction.template f_InsertReturning<&CPersonRow::m_ID, gc_InsertPerson>(NStr::CStr("transaction-delete-by-id@example.com"));

			umint nTransactionDeletedByID = co_await Transaction.template f_DeleteByID<gc_PersonTable, &CPersonRow::m_ID>(TransactionDeleteID);
			DMibExpect(nTransactionDeletedByID, ==, umint(1));

			nTransactionDeletedByID = co_await Transaction.template f_DeleteByID<gc_PersonTable, &CPersonRow::m_ID>(TransactionDeleteID);
			DMibExpect(nTransactionDeletedByID, ==, umint(0));

			DMibExpect
				(
					co_await Transaction.template f_Update<gc_UpdatePersonEmailByEmail>(NStr::CStr("transaction-updated@example.com"), NStr::CStr("transaction@example.com"))
					, ==
					, umint(1)
				)
			;

			co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-delete@example.com"));

			DMibExpect(co_await Transaction.template f_Delete<gc_DeletePersonByEmail>(NStr::CStr("transaction-delete@example.com")), ==, umint(1));

			DMibExpect(co_await Transaction.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-delete@example.com")), ==, false);

			auto Rows = Transaction.template f_Query<gc_SelectPersonByEmail>(NStr::CStr("transaction-updated@example.com"));
			umint nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					DMibTestPath("First query");
					++nRows;
					DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("transaction-updated@example.com"));
				}
			}

			{
				DMibTestPath("Second query");
				DMibExpect(nRows, ==, umint(1));
			}

			Rows = Transaction.template f_Query<gc_SelectPersonByEmail>(NStr::CStr("transaction-updated@example.com"));
			nRows = 0;

			for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					DMibTestPath("Second query");
					++nRows;
					DMibExpect(pRow->m_Data.m_Email, ==, NStr::CStr("transaction-updated@example.com"));
				}
			}

			DMibExpect(nRows, ==, umint(1));

			auto ReturningRows = Transaction.template f_Query<gc_SelectPersonByEmail>(NStr::CStr("transaction-returning-updated@example.com"));
			nRows = 0;

			for (auto iBatch = co_await fg_Move(ReturningRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				for (auto const &pRow : *iBatch)
				{
					++nRows;
					DMibExpect(pRow->m_Data.m_ID, ==, TransactionReturnedID);
				}
			}

			{
				DMibTestPath("Insert returning query");
				DMibExpect(nRows, ==, umint(1));
			}

			co_await Transaction.f_Commit();

			co_return {};
		};

		DMibTestCategory("Transaction typed mutation returning") -> NConcurrency::TCFuture<void>
		{
			using CStrResult = NConcurrency::TCAsyncResult<NStr::CStr>;

			auto Transaction = co_await Database.f_BeginTransaction();

			// The transaction wrapper must expose the returning upsert/update/delete mutations (PostgreSQL only, as
			// SQLite does not support typed mutation RETURNING), mirroring the database client.
			CStrResult UpsertResult = co_await Transaction.template f_UpsertReturning<&CUpsertRow::m_DisplayName, gc_UpsertPersonDisplayNameByEmail>
				(
					NStr::CStr("transaction-returning-mutation@example.com")
					, NStr::CStr("Transaction Returning Initial")
				)
				.f_Wrap()
			;
			if (UpsertResult)
			{
				DMibExpect(*UpsertResult, ==, NStr::CStr("Transaction Returning Initial"));

				NStr::CStr UpdatedName = co_await Transaction.template f_UpdateReturning<&CUpsertRow::m_DisplayName, gc_UpdateUpsertPersonDisplayNameByEmail>
					(
						NStr::CStr("Transaction Returning Updated")
						, NStr::CStr("transaction-returning-mutation@example.com")
					)
				;
				DMibExpect(UpdatedName, ==, NStr::CStr("Transaction Returning Updated"));

				NStr::CStr DeletedEmail = co_await Transaction.template f_DeleteReturning<&CUpsertRow::m_Email, gc_DeleteUpsertPersonByEmail>
					(
						NStr::CStr("transaction-returning-mutation@example.com")
					)
				;
				DMibExpect(DeletedEmail, ==, NStr::CStr("transaction-returning-mutation@example.com"));
			}
			else
			{
				fg_TestExpectSqlError(UpsertResult, "SQLite typed mutation RETURNING", ESqlErrorCategory::mc_Generic);
			}

			co_await Transaction.f_Commit();

			co_return {};
		};

		DMibTestCategory("Transaction rejects operations after finish") -> NConcurrency::TCFuture<void>
		{
			{
				DMibTestPath("After commit");

				auto Transaction = co_await Database.f_BeginTransaction();
				co_await Transaction.f_Commit();

				auto InsertResult = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-after-commit@example.com")).f_Wrap();
				DMibExpect(bool(InsertResult), ==, false);

				auto ExistsResult = co_await Transaction.f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-after-commit@example.com")).f_Wrap();
				DMibExpect(bool(ExistsResult), ==, false);

				auto RawResult = co_await Transaction.f_ExecuteRaw(NStr::CStr("INSERT INTO people (email) VALUES ('transaction-raw-after-commit@example.com')")).f_Wrap();
				DMibExpect(bool(RawResult), ==, false);
			}

			{
				DMibTestPath("After rollback");

				auto Transaction = co_await Database.f_BeginTransaction();
				co_await Transaction.f_Rollback();

				auto InsertResult = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-after-rollback@example.com")).f_Wrap();
				DMibExpect(bool(InsertResult), ==, false);
			}

			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-after-commit@example.com")), ==, false);
			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-raw-after-commit@example.com")), ==, false);
			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-after-rollback@example.com")), ==, false);

			co_return {};
		};

		DMibTestCategory("Transaction finish rejects queued operations") -> NConcurrency::TCFuture<void>
		{
			// Operations queued after commit/rollback is initiated, but before it finishes, must be rejected.
			// Otherwise the queued operation waits behind the finish on the transaction sequencer and then runs
			// on the same connection once the transaction has ended (autocommit), letting a write escape the
			// transaction. f_Commit()/f_Rollback() enqueue their actor calls synchronously and ahead of the
			// queued operation, so marking the transaction finished in the synchronous prefix of the finish
			// deterministically rejects the later operation here.

			{
				DMibTestPath("Queued after commit");

				auto Transaction = co_await Database.f_BeginTransaction();

				auto Commit = Transaction.f_Commit();
				auto InsertAfterCommit = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-queued-after-commit@example.com")).f_Wrap();

				DMibExpect(bool(InsertAfterCommit), ==, false);

				co_await fg_Move(Commit);
			}

			{
				DMibTestPath("Queued after rollback");

				auto Transaction = co_await Database.f_BeginTransaction();

				auto Rollback = Transaction.f_Rollback();
				auto InsertAfterRollback = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-queued-after-rollback@example.com")).f_Wrap();

				DMibExpect(bool(InsertAfterRollback), ==, false);

				co_await fg_Move(Rollback);
			}

			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-queued-after-commit@example.com")), ==, false);
			DMibExpect(co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-queued-after-rollback@example.com")), ==, false);

			co_return {};
		};

		DMibTestCategory("Transaction rejects set selects with operand modifiers") -> NConcurrency::TCFuture<void>
		{
			// The non-transaction read paths reject set operands carrying ORDER BY / LIMIT / OFFSET. The
			// transaction read paths must apply the same validation instead of sending an unsupported
			// compound to the backend, which would error or abort the active transaction.
			auto Transaction = co_await Database.f_BeginTransaction();

			auto QueryResult = co_await Transaction.template f_QueryVector<gc_UnionWithModifiedRightOperand>().f_Wrap();
			DMibExpect(bool(QueryResult), ==, false);

			auto CountResult = co_await Transaction.template f_Count<gc_UnionWithModifiedRightOperand>().f_Wrap();
			DMibExpect(bool(CountResult), ==, false);

			auto ExistsResult = co_await Transaction.template f_Exists<gc_UnionWithModifiedRightOperand>().f_Wrap();
			DMibExpect(bool(ExistsResult), ==, false);

			co_await Transaction.f_Commit();

			co_return {};
		};

		DMibTestCategory("Transaction scope helpers") -> NConcurrency::TCFuture<void>
		{
			NStr::CStr CommitEmail("transaction-scope-commit@example.com");
			NStr::CStr RollbackEmail("transaction-scope-rollback@example.com");
			NStr::CStr ReadEmail("transaction-scope-read@example.com");

			co_await Database.template f_Delete<gc_DeletePersonByEmail>(CommitEmail);
			co_await Database.template f_Delete<gc_DeletePersonByEmail>(RollbackEmail);
			co_await Database.template f_Delete<gc_DeletePersonByEmail>(ReadEmail);

			int64 CommitID = co_await Database.f_WithTransaction
				(
					[CommitEmail](CSqlTransaction _Transaction) mutable
					{
						return _Transaction.template f_InsertReturning<&CPersonRow::m_ID, gc_InsertPerson>(fg_Move(CommitEmail));
					}
				)
			;
			DMibExpect(CommitID, >, int64(0));
			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-commit@example.com"))
					, ==
					, true
				)
			;

			auto RollbackResult = co_await Database.f_WithTransaction
				(
					[RollbackEmail](CSqlTransaction _Transaction) mutable
					{
						return fg_TestSqlTransactionScopeRollback(fg_Move(_Transaction), fg_Move(RollbackEmail));
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(RollbackResult), ==, false);
			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-rollback@example.com"))
					, ==
					, false
				)
			;

			co_await Database.template f_Insert<gc_InsertPerson>(NStr::CStr("transaction-scope-read@example.com"));

			bool bReadExists = co_await Database.f_WithReadTransaction
				(
					[ReadEmail](CSqlTransaction _Transaction) mutable
					{
						return _Transaction.template f_Exists<gc_SelectPersonByEmail>(fg_Move(ReadEmail));
					}
				)
			;
			DMibExpect(bReadExists, ==, true);

			NStr::CStr OuterEmail("transaction-scope-nested-outer@example.com");
			NStr::CStr NestedCommitEmail("transaction-scope-nested-commit@example.com");
			NStr::CStr NestedRollbackEmail("transaction-scope-nested-rollback@example.com");

			co_await Database.template f_Delete<gc_DeletePersonByEmail>(OuterEmail);
			co_await Database.template f_Delete<gc_DeletePersonByEmail>(NestedCommitEmail);
			co_await Database.template f_Delete<gc_DeletePersonByEmail>(NestedRollbackEmail);

			co_await Database.f_WithTransaction
				(
					[OuterEmail, NestedCommitEmail, NestedRollbackEmail](CSqlTransaction _Transaction) mutable
					{
						return fg_TestSqlNestedTransactionScope
							(
								fg_Move(_Transaction)
								, fg_Move(OuterEmail)
								, fg_Move(NestedCommitEmail)
								, fg_Move(NestedRollbackEmail)
							)
						;
					}
				)
			;

			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-outer@example.com"))
					, ==
					, true
				)
			;
			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-commit@example.com"))
					, ==
					, true
				)
			;
			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-rollback@example.com"))
					, ==
					, false
				)
			;

			NStr::CStr StreamEmail("transaction-scope-stream@example.com");
			NStr::CStr AfterStreamEmail("transaction-scope-after-stream@example.com");

			co_await Database.template f_Delete<gc_DeletePersonByEmail>(StreamEmail);
			co_await Database.template f_Delete<gc_DeletePersonByEmail>(AfterStreamEmail);

			co_await Database.f_WithTransaction
				(
					[StreamEmail, AfterStreamEmail](CSqlTransaction _Transaction) mutable
					{
						return fg_TestSqlTransactionStreamCancellation(fg_Move(_Transaction), fg_Move(StreamEmail), fg_Move(AfterStreamEmail));
					}
				)
			;

			DMibExpect
				(
					co_await Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-after-stream@example.com"))
					, ==
					, true
				)
			;

			co_return {};
		};

		DMibTestCategory("Transaction isolation settings") -> NConcurrency::TCFuture<void>
		{
			DMibExpect(_Capabilities.m_bIsolationSerializable, ==, true);

			if (_Capabilities.m_bIsolationSerializable)
			{
				CSqlTransactionSettings Settings;
				Settings.m_Isolation = ESqlTransactionIsolation::mc_Serializable;

				{
					auto Transaction = co_await Database.f_BeginTransaction(Settings);
					co_await Transaction.f_Commit();
				}

				{
					auto Transaction = co_await Database.f_BeginReadTransaction(Settings);
					co_await Transaction.f_Commit();
				}
			}

			co_return {};
		};

		co_return {};
	}
}
