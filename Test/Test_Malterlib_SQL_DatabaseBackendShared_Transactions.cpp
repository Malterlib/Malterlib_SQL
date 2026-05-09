// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	namespace
	{
		template <typename t_CValue>
		struct TCTestSqlTransactionCallback
		{
			auto operator () (CSqlTransaction _Transaction) -> NConcurrency::TCFuture<t_CValue>;
		};

		template <typename t_FCallback>
		concept cTestSqlClientScopeAccepts = requires (CSqlDatabaseClient &_Database, t_FCallback _fCallback)
			{
				_Database.f_WithTransaction(fg_Move(_fCallback));
				_Database.f_WithReadTransaction(fg_Move(_fCallback));
			}
		;

		template <typename t_FCallback>
		concept cTestSqlTransactionScopeAccepts = requires (CSqlTransaction &_Transaction, t_FCallback _fCallback)
			{
				_Transaction.f_WithTransaction(fg_Move(_fCallback));
			}
		;

		// The commit after the callback would wait for a row stream of the transaction that only its caller can read
		static_assert(cTestSqlClientScopeAccepts<TCTestSqlTransactionCallback<NStr::CStr>>);
		static_assert(!cTestSqlClientScopeAccepts<TCTestSqlTransactionCallback<CSqlRawStream>>);
		static_assert(!cTestSqlClientScopeAccepts<TCTestSqlTransactionCallback<NConcurrency::TCAsyncGenerator<CSqlRawRowBatch>>>);
		static_assert(cTestSqlTransactionScopeAccepts<TCTestSqlTransactionCallback<NStr::CStr>>);
		static_assert(!cTestSqlTransactionScopeAccepts<TCTestSqlTransactionCallback<CSqlRawStream>>);

		auto fg_TestSqlTransactionScopeInsertAndWait
			(
				CSqlTransaction _Transaction
				, NStr::CStr _Email
				, NConcurrency::TCPromise<void> _Inserted
				, NConcurrency::TCFuture<void> _Continue
				, bool _bFail
			)
			-> NConcurrency::TCFuture<void>
		{
			co_await _Transaction.f_Insert<gc_InsertPerson>(fg_Move(_Email));
			_Inserted.f_SetResult();
			co_await fg_Move(_Continue);

			if (_bFail)
				co_return DMibErrorInstance("SQL transaction scope overlap test");

			co_return {};
		}

		// Runs a scope on its own actor, which the test destroys while the scope's callback waits
		struct CTestSqlScopeRunner : public NConcurrency::CActor
		{
			NConcurrency::TCFuture<void> f_RunScope(CSqlTransaction _Transaction, NStr::CStr _Email, NConcurrency::TCPromise<void> _Inserted, NConcurrency::TCFuture<void> _Continue);
		};

		auto CTestSqlScopeRunner::f_RunScope(CSqlTransaction _Transaction, NStr::CStr _Email, NConcurrency::TCPromise<void> _Inserted, NConcurrency::TCFuture<void> _Continue)
			-> NConcurrency::TCFuture<void>
		{
			co_await _Transaction.f_WithTransaction
				(
					[Email = fg_Move(_Email), Inserted = fg_Move(_Inserted), Continue = fg_Move(_Continue)](CSqlTransaction _ScopeTransaction) mutable
					{
						return fg_TestSqlTransactionScopeInsertAndWait(fg_Move(_ScopeTransaction), fg_Move(Email), fg_Move(Inserted), fg_Move(Continue), false);
					}
				)
			;

			co_return {};
		}

		NConcurrency::TCFuture<void> fg_TestSqlTransactionScopeRollback(CSqlTransaction _Transaction, NStr::CStr _Email)
		{
			co_await _Transaction.f_Insert<gc_InsertPerson>(fg_Move(_Email));
			co_return DMibErrorInstance("SQL transaction scope rollback test");
		}

		NConcurrency::TCFuture<void> fg_TestSqlNestedScopeThrowingBeforeItsFuture(CSqlTransaction _Transaction, NStr::CStr _Email)
		{
			auto NestedResult = co_await _Transaction.f_WithTransaction
				(
					[](CSqlTransaction _NestedTransaction) -> NConcurrency::TCFuture<void>
					{
						DMibError("SQL transaction scope callback throwing before it returns its future");
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(NestedResult), ==, false);

			// The enclosing scope can only end when the nested one was rolled back and released
			co_await _Transaction.f_Insert<gc_InsertPerson>(fg_Move(_Email));

			co_return {};
		}

		NConcurrency::TCFuture<void> fg_TestSqlNestedTransactionScope(CSqlTransaction _Transaction, NStr::CStr _OuterEmail, NStr::CStr _NestedCommitEmail, NStr::CStr _NestedRollbackEmail)
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
			DMibExpect(co_await _Transaction.f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-commit@example.com")), ==, true);
			DMibExpect(co_await _Transaction.f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-rollback@example.com")), ==, false);

			co_return {};
		}

		NConcurrency::TCFuture<void> fg_TestSqlTransactionStreamCancellation(CSqlTransaction _Transaction, NStr::CStr _StreamEmail, NStr::CStr _AfterStreamEmail)
		{
			for (uint64 iIteration = 0; iIteration < 129; ++iIteration)
				co_await _Transaction.f_Insert<gc_InsertPerson>(_StreamEmail);

			{
				CSqlSelectSettings Settings;
				Settings.m_nRowsPerBatch = 16;

				auto Rows = _Transaction.f_Query<gc_SelectPersonByEmail>(_StreamEmail, Settings);
				auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator();
				DMibExpect(bool(iBatch), ==, true);
			}

			co_await _Transaction.f_Insert<gc_InsertPerson>(fg_Move(_AfterStreamEmail)).f_Timeout(gc_StreamHandoffTimeout, "Timed out waiting for the insert after the abandoned stream");

			co_return {};
		}
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTransactionInsertAndSelect(CTestDatabaseClient _Database, CSqlDatabaseBackendCapabilities _Capabilities)
	{
		DMibTestCategory("Finished transaction releases its connection") -> NConcurrency::TCFuture<void>
		{
			auto CommittedTransaction = co_await _Database.f_BeginTransaction();
			co_await CommittedTransaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-release-commit@example.com"));
			co_await CommittedTransaction.f_Commit();

			// The finished handles stay in scope; with a single write connection these would wait for it forever
			co_await _Database.template f_Insert<gc_InsertPerson>(NStr::CStr("transaction-release-after-commit@example.com")).f_Timeout
				(
					10.0
					, "Timed out waiting for the connection of a committed transaction"
				)
			;

			auto RolledBackTransaction = co_await _Database.f_BeginTransaction();
			co_await RolledBackTransaction.f_Rollback();
			co_await _Database.template f_Insert<gc_InsertPerson>(NStr::CStr("transaction-release-after-rollback@example.com")).f_Timeout
				(
					10.0
					, "Timed out waiting for the connection of a rolled back transaction"
				)
			;

			co_return {};
		};

		DMibTestCategory("Transaction ended by raw SQL rejects further statements") -> NConcurrency::TCFuture<void>
		{
			auto Transaction = co_await _Database.f_BeginTransaction();
			co_await Transaction.f_ExecuteRaw(NStr::CStr("ROLLBACK"));

			auto InsertResult = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-raw-rollback@example.com")).f_Wrap();
			DMibExpect(bool(InsertResult), ==, false);

			auto CommitResult = co_await Transaction.f_Commit().f_Wrap();
			DMibExpect(bool(CommitResult), ==, false);
			co_await Transaction.f_Rollback();

			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-raw-rollback@example.com")), ==, false);

			{
				DMibTestPath("operation queued behind the raw statement");

				auto QueuedTransaction = co_await _Database.f_BeginTransaction();
				auto RawRollback = QueuedTransaction.f_ExecuteRaw(NStr::CStr("ROLLBACK"));
				auto QueuedInsert = QueuedTransaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-raw-rollback-queued@example.com"));
				co_await fg_Move(RawRollback);

				auto QueuedInsertResult = co_await fg_Move(QueuedInsert).f_Wrap();
				DMibExpect(bool(QueuedInsertResult), ==, false);
				co_await QueuedTransaction.f_Rollback();

				DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-raw-rollback-queued@example.com")), ==, false);
			}

			{
				DMibTestPath("raw stream");

				auto StreamTransaction = co_await _Database.f_BeginTransaction();
				auto StreamResult = co_await StreamTransaction.f_QueryRawStream(NStr::CStr("ROLLBACK")).f_Wrap();
				if (StreamResult)
				{
					for (auto iBatch = co_await fg_Move(StreamResult->m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
					{
					}
				}

				auto InsertAfterStreamResult = co_await StreamTransaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-raw-rollback-stream@example.com")).f_Wrap();
				DMibExpect(bool(InsertAfterStreamResult), ==, false);
				co_await StreamTransaction.f_Rollback();

				DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-raw-rollback-stream@example.com")), ==, false);
			}

			co_return {};
		};

		DMibTestCategory("Transaction insert and select") -> NConcurrency::TCFuture<void>
		{
			auto Transaction = co_await _Database.f_BeginTransaction();

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

			auto Transaction = co_await _Database.f_BeginTransaction();

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
				fg_TestExpectSqlError(UpsertResult, "SQLite typed mutation RETURNING", ESqlErrorCategory::mc_Generic);

			co_await Transaction.f_Commit();

			co_return {};
		};

		DMibTestCategory("Transaction rejects operations after finish") -> NConcurrency::TCFuture<void>
		{
			{
				DMibTestPath("After commit");

				auto Transaction = co_await _Database.f_BeginTransaction();
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

				auto Transaction = co_await _Database.f_BeginTransaction();
				co_await Transaction.f_Rollback();

				auto InsertResult = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-after-rollback@example.com")).f_Wrap();
				DMibExpect(bool(InsertResult), ==, false);

				auto CommitResult = co_await Transaction.f_Commit().f_Wrap();
				DMibExpect(bool(CommitResult), ==, false);
			}

			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-after-commit@example.com")), ==, false);
			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-raw-after-commit@example.com")), ==, false);
			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-after-rollback@example.com")), ==, false);

			co_return {};
		};

		DMibTestCategory("Transaction finish rejects queued operations") -> NConcurrency::TCFuture<void>
		{
			// The finish future stays unawaited, so each operation is submitted once the finish is queued, without waiting for it

			{
				DMibTestPath("Queued after commit");

				auto Transaction = co_await _Database.f_BeginTransaction();

				auto Commit = Transaction.f_Commit();
				auto InsertAfterCommit = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-queued-after-commit@example.com")).f_Wrap();

				DMibExpect(bool(InsertAfterCommit), ==, false);

				co_await fg_Move(Commit);
			}

			{
				DMibTestPath("Queued after rollback");

				auto Transaction = co_await _Database.f_BeginTransaction();

				auto Rollback = Transaction.f_Rollback();
				auto InsertAfterRollback = co_await Transaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-queued-after-rollback@example.com")).f_Wrap();

				DMibExpect(bool(InsertAfterRollback), ==, false);

				auto CommitAfterRollback = co_await Transaction.f_Commit().f_Wrap();
				DMibExpect(bool(CommitAfterRollback), ==, false);

				co_await fg_Move(Rollback);
			}

			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-queued-after-commit@example.com")), ==, false);
			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-queued-after-rollback@example.com")), ==, false);

			co_return {};
		};

		DMibTestCategory("Transaction rejects set selects with operand modifiers without aborting") -> NConcurrency::TCFuture<void>
		{
			auto Transaction = co_await _Database.f_BeginTransaction();

			auto QueryResult = co_await Transaction.template f_QueryVector<gc_UnionWithModifiedRightOperand>().f_Wrap();
			DMibExpect(bool(QueryResult), ==, false);

			auto CountResult = co_await Transaction.template f_Count<gc_UnionWithModifiedRightOperand>().f_Wrap();
			DMibExpect(bool(CountResult), ==, false);

			auto ExistsResult = co_await Transaction.template f_Exists<gc_UnionWithModifiedRightOperand>().f_Wrap();
			DMibExpect(bool(ExistsResult), ==, false);

			co_await Transaction.f_Commit();

			co_return {};
		};

		DMibTestCategory("Transaction scopes must nest") -> NConcurrency::TCFuture<void>
		{
			{
				DMibTestPath("Scope ending while a later scope is open");

				NStr::CStr FirstEmail("transaction-scope-overlap-first@example.com");
				NStr::CStr SecondEmail("transaction-scope-overlap-second@example.com");

				auto Transaction = co_await _Database.f_BeginTransaction();

				NConcurrency::TCPromise<void> FirstInserted;
				NConcurrency::TCPromise<void> FirstContinue;
				auto FirstInsertedFuture = FirstInserted.f_Future();
				auto FirstScope = Transaction.f_WithTransaction
					(
						[FirstEmail, FirstInserted, Continue = FirstContinue.f_Future()](CSqlTransaction _Transaction) mutable
						{
							return fg_TestSqlTransactionScopeInsertAndWait(fg_Move(_Transaction), fg_Move(FirstEmail), fg_Move(FirstInserted), fg_Move(Continue), false);
						}
					)
				;
				co_await fg_Move(FirstInsertedFuture);

				NConcurrency::TCPromise<void> SecondInserted;
				NConcurrency::TCPromise<void> SecondContinue;
				auto SecondInsertedFuture = SecondInserted.f_Future();
				auto SecondScope = Transaction.f_WithTransaction
					(
						[SecondEmail, SecondInserted, Continue = SecondContinue.f_Future()](CSqlTransaction _Transaction) mutable
						{
							return fg_TestSqlTransactionScopeInsertAndWait(fg_Move(_Transaction), fg_Move(SecondEmail), fg_Move(SecondInserted), fg_Move(Continue), true);
						}
					)
				;
				co_await fg_Move(SecondInsertedFuture);

				// Releasing the first scope's savepoint would also release the second scope's, which could then not roll back
				FirstContinue.f_SetResult();
				auto FirstResult = co_await fg_Move(FirstScope).f_Wrap();
				DMibExpect(bool(FirstResult), ==, false);
				if (!FirstResult)
					DMibExpect(FirstResult.f_GetExceptionStr().f_Find("still open") >= aint(0), ==, true);

				SecondContinue.f_SetResult();
				auto SecondResult = co_await fg_Move(SecondScope).f_Wrap();
				DMibExpect(bool(SecondResult), ==, false);
				if (!SecondResult)
					DMibExpect(SecondResult.f_GetExceptionStr().f_Find("SQL transaction scope overlap test") >= aint(0), ==, true);

				DMibExpect(co_await Transaction.template f_Exists<gc_SelectPersonByEmail>(FirstEmail), ==, true);
				DMibExpect(co_await Transaction.template f_Exists<gc_SelectPersonByEmail>(SecondEmail), ==, false);

				co_await Transaction.f_Rollback();
			}

			{
				DMibTestPath("Scope whose savepoint an enclosing savepoint ended");

				auto Transaction = co_await _Database.f_BeginTransaction();
				NStr::CStr Savepoint = co_await Transaction.f_CreateSavepoint();

				NConcurrency::TCPromise<void> Inserted;
				NConcurrency::TCPromise<void> Continue;
				auto InsertedFuture = Inserted.f_Future();
				auto Scope = Transaction.f_WithTransaction
					(
						[Inserted, ContinueFuture = Continue.f_Future()](CSqlTransaction _Transaction) mutable
						{
							return fg_TestSqlTransactionScopeInsertAndWait
								(
									fg_Move(_Transaction)
									, NStr::CStr("transaction-scope-ended-by-savepoint@example.com")
									, fg_Move(Inserted)
									, fg_Move(ContinueFuture)
									, false
								)
							;
						}
					)
				;
				co_await fg_Move(InsertedFuture);

				co_await Transaction.f_ReleaseSavepoint(Savepoint);

				Continue.f_SetResult();
				auto ScopeResult = co_await fg_Move(Scope).f_Wrap();
				DMibExpect(bool(ScopeResult), ==, false);
				if (!ScopeResult)
					DMibExpect(ScopeResult.f_GetExceptionStr().f_Find("already ended") >= aint(0), ==, true);

				co_await Transaction.f_Rollback();
			}

			co_return {};
		};

		DMibTestCategory("Transaction scope abandoned by its actor rolls back") -> NConcurrency::TCFuture<void>
		{
			NStr::CStr Email("transaction-scope-abandoned@example.com");

			auto Transaction = co_await _Database.f_BeginTransaction();

			NConcurrency::TCActor<CTestSqlScopeRunner> Runner = NConcurrency::fg_ConstructActor<CTestSqlScopeRunner>();
			NConcurrency::TCPromise<void> Inserted;
			NConcurrency::TCPromise<void> Continue;
			auto InsertedFuture = Inserted.f_Future();

			// The outer scope hands its handle to the runner, whose scope inside it is open when the runner goes away
			auto OuterScope = Transaction.f_WithTransaction
				(
					[Runner, Email, Inserted, ContinueFuture = Continue.f_Future()](CSqlTransaction _Transaction) mutable -> NConcurrency::TCFuture<void>
					{
						return Runner(&CTestSqlScopeRunner::f_RunScope, fg_Move(_Transaction), fg_Move(Email), fg_Move(Inserted), fg_Move(ContinueFuture));
					}
				)
			;
			co_await fg_Move(InsertedFuture);

			co_await fg_Move(Runner).f_Destroy();

			auto OuterResult = co_await fg_Move(OuterScope).f_Wrap();
			DMibExpect(bool(OuterResult), ==, false);

			co_await Transaction.f_Commit();

			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(Email), ==, false);

			Continue.f_SetResult();

			co_return {};
		};

		DMibTestCategory("Transaction scope helpers") -> NConcurrency::TCFuture<void>
		{
			NStr::CStr CommitEmail("transaction-scope-commit@example.com");
			NStr::CStr RollbackEmail("transaction-scope-rollback@example.com");
			NStr::CStr ReadEmail("transaction-scope-read@example.com");

			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(CommitEmail);
			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(RollbackEmail);
			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(ReadEmail);

			int64 CommitID = co_await _Database.f_WithTransaction
				(
					[CommitEmail](CSqlTransaction _Transaction) mutable
					{
						return _Transaction.template f_InsertReturning<&CPersonRow::m_ID, gc_InsertPerson>(fg_Move(CommitEmail));
					}
				)
			;
			DMibExpect(CommitID, >, int64(0));
			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-commit@example.com")), ==, true);

			auto RollbackResult = co_await _Database.f_WithTransaction
				(
					[RollbackEmail](CSqlTransaction _Transaction) mutable
					{
						return fg_TestSqlTransactionScopeRollback(fg_Move(_Transaction), fg_Move(RollbackEmail));
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(RollbackResult), ==, false);
			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-rollback@example.com")), ==, false);

			{
				DMibTestPath("nested scope outlives the outer callback's handle");

				// The outer callback is not a coroutine, so its transaction parameter is gone while the nested scope runs
				co_await _Database.f_WithTransaction
					(
						[](CSqlTransaction _Transaction)
						{
							return _Transaction.f_WithTransaction
								(
									[](CSqlTransaction _NestedTransaction)
									{
										return _NestedTransaction.f_Insert<gc_InsertPerson>(NStr::CStr("transaction-scope-nested-handle@example.com"));
									}
								)
							;
						}
					)
				;
				DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-handle@example.com")), ==, true);
			}

			{
				DMibTestPath("nested scope whose callback throws before returning its future");

				co_await _Database.f_WithTransaction
					(
						[](CSqlTransaction _Transaction)
						{
							return _Transaction.f_WithTransaction
								(
									[](CSqlTransaction _ScopeTransaction)
									{
										return fg_TestSqlNestedScopeThrowingBeforeItsFuture(fg_Move(_ScopeTransaction), NStr::CStr("transaction-scope-nested-throw@example.com"));
									}
								)
							;
						}
					)
				;
				DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-throw@example.com")), ==, true);
			}

			co_await _Database.template f_Insert<gc_InsertPerson>(NStr::CStr("transaction-scope-read@example.com"));

			bool bReadExists = co_await _Database.f_WithReadTransaction
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

			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(OuterEmail);
			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(NestedCommitEmail);
			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(NestedRollbackEmail);

			co_await _Database.f_WithTransaction
				(
					[OuterEmail, NestedCommitEmail, NestedRollbackEmail](CSqlTransaction _Transaction) mutable
					{
						return fg_TestSqlNestedTransactionScope(fg_Move(_Transaction), fg_Move(OuterEmail), fg_Move(NestedCommitEmail), fg_Move(NestedRollbackEmail));
					}
				)
			;

			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-outer@example.com")), ==, true);
			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-commit@example.com")), ==, true);
			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-nested-rollback@example.com")), ==, false);

			NStr::CStr StreamEmail("transaction-scope-stream@example.com");
			NStr::CStr AfterStreamEmail("transaction-scope-after-stream@example.com");

			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(StreamEmail);
			co_await _Database.template f_Delete<gc_DeletePersonByEmail>(AfterStreamEmail);

			co_await _Database.f_WithTransaction
				(
					[StreamEmail, AfterStreamEmail](CSqlTransaction _Transaction) mutable
					{
						return fg_TestSqlTransactionStreamCancellation(fg_Move(_Transaction), fg_Move(StreamEmail), fg_Move(AfterStreamEmail));
					}
				)
			;

			DMibExpect(co_await _Database.template f_Exists<gc_SelectPersonByEmail>(NStr::CStr("transaction-scope-after-stream@example.com")), ==, true);

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
					auto Transaction = co_await _Database.f_BeginTransaction(Settings);
					co_await Transaction.f_Commit();
				}

				{
					auto Transaction = co_await _Database.f_BeginReadTransaction(Settings);
					co_await Transaction.f_Commit();
				}
			}

			co_return {};
		};

		co_return {};
	}
}
