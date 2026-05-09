// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	namespace
	{
		NStr::CStr fg_TestSqlInsertParameterPlaceholders(CSqlDatabaseBackendCapabilities const &_Capabilities)
		{
			return _Capabilities.m_bNumberedPlaceholders ? NStr::CStr("$1") : NStr::CStr("?");
		}

		NStr::CStr fg_TestSqlInsertPersonSql(CSqlDatabaseBackendCapabilities const &_Capabilities)
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "INSERT INTO people (email) VALUES (";
				Appender += fg_TestSqlInsertParameterPlaceholders(_Capabilities);
				Appender += ")";
			}

			return Sql;
		}

		NStr::CStr fg_TestSqlSelectPersonByEmailSql(CSqlDatabaseBackendCapabilities const &_Capabilities)
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "SELECT id, email FROM people WHERE email = ";
				Appender += fg_TestSqlInsertParameterPlaceholders(_Capabilities);
			}

			return Sql;
		}

		NStr::CStr fg_TestSqlDeletePersonByEmailSql(CSqlDatabaseBackendCapabilities const &_Capabilities)
		{
			NStr::CStr Sql;
			{
				NStr::CStr::CAppender Appender(Sql);
				Appender += "DELETE FROM people WHERE email = ";
				Appender += fg_TestSqlInsertParameterPlaceholders(_Capabilities);
			}

			return Sql;
		}
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRaw(CTestDatabaseClient _Database, CSqlDatabaseBackendCapabilities _Capabilities)
	{
		DMibTestCategory("Raw SQL execute and query") -> NConcurrency::TCFuture<void>
		{
			NContainer::TCVector<CSqlValue> InsertParameters;
			InsertParameters.f_InsertLast(NStr::CStr("raw-insert@example.com"));
			umint nInserted = co_await _Database.f_ExecuteRaw(fg_TestSqlInsertPersonSql(_Capabilities), fg_Move(InsertParameters));
			DMibExpect(nInserted, ==, umint(1));

			NContainer::TCVector<CSqlValue> SelectParameters;
			SelectParameters.f_InsertLast(NStr::CStr("raw-insert@example.com"));
			CSqlRawResult Result = co_await _Database.f_QueryRaw(fg_TestSqlSelectPersonByEmailSql(_Capabilities), fg_Move(SelectParameters));

			DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));
			DMibExpect(Result.m_Columns.f_GetLen(), ==, umint(2));
			DMibExpect(Result.m_Columns[0].m_Name, ==, NStr::CStr("id"));
			DMibExpect(Result.m_Columns[1].m_Name, ==, NStr::CStr("email"));

			if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values.f_GetLen() == 2)
			{
				auto &EmailValue = Result.m_Rows[0].m_Values[1];
				DMibExpect(EmailValue.f_GetTypeID(), ==, ESqlValueType::mc_Text);
				DMibExpect(EmailValue.f_GetAsType<NStr::CStr>(), ==, NStr::CStr("raw-insert@example.com"));
			}

			NContainer::TCVector<CSqlValue> DeleteParameters;
			DeleteParameters.f_InsertLast(NStr::CStr("raw-insert@example.com"));
			umint nDeleted = co_await _Database.f_ExecuteRaw(fg_TestSqlDeletePersonByEmailSql(_Capabilities), fg_Move(DeleteParameters));
			DMibExpect(nDeleted, ==, umint(1));

			DMibExpect(co_await _Database.f_ExecuteRaw(NStr::CStr("SELECT 1")), ==, umint(0));

			co_return {};
		};

		DMibTestCategory("Raw SQL rejects multiple statements in one call") -> NConcurrency::TCFuture<void>
		{
			auto ExecuteResult = co_await _Database.f_ExecuteRaw(NStr::CStr("SELECT 1; SELECT 2")).f_Wrap();
			DMibExpect(bool(ExecuteResult), ==, false);

			auto QueryResult = co_await _Database.f_QueryRaw(NStr::CStr("SELECT 1; SELECT 2")).f_Wrap();
			DMibExpect(bool(QueryResult), ==, false);

			co_return {};
		};

		DMibTestCategory("Raw transaction-control does not leak an open transaction") -> NConcurrency::TCFuture<void>
		{
			co_await _Database.f_ExecuteRaw(NStr::CStr("BEGIN"));

			auto TransactionResult = co_await _Database.f_WithTransaction
				(
					[](CSqlTransaction _Transaction) -> NConcurrency::TCFuture<void>
					{
						co_return {};
					}
				)
				.f_Wrap()
			;
			DMibExpect(bool(TransactionResult), ==, true);

			co_return {};
		};

		DMibTestCategory("Raw SQL DDL reports success") -> NConcurrency::TCFuture<void>
		{
			// The PostgreSQL tags of CREATE TABLE and DROP TABLE have no row count, which must not turn success into an error
			auto CleanupResult = co_await _Database.f_ExecuteRaw(NStr::CStr("DROP TABLE IF EXISTS raw_ddl_probe")).f_Wrap();
			DMibExpect(bool(CleanupResult), ==, true);

			auto CreateResult = co_await _Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE raw_ddl_probe (id INTEGER NOT NULL)")).f_Wrap();
			DMibExpect(bool(CreateResult), ==, true);

			auto DropResult = co_await _Database.f_ExecuteRaw(NStr::CStr("DROP TABLE raw_ddl_probe")).f_Wrap();
			DMibExpect(bool(DropResult), ==, true);

			co_return {};
		};

		DMibTestCategory("Raw non-DML reports zero affected rows after a mutation") -> NConcurrency::TCFuture<void>
		{
			// SQLite leaves sqlite3_changes() untouched by DDL, so the CREATE TABLE must not report the INSERT's count
			NContainer::TCVector<CSqlValue> InsertParameters;
			InsertParameters.f_InsertLast(NStr::CStr("raw-nondml-affected@example.com"));
			umint nInserted = co_await _Database.f_ExecuteRaw(fg_TestSqlInsertPersonSql(_Capabilities), fg_Move(InsertParameters));
			DMibExpect(nInserted, ==, umint(1));

			co_await _Database.f_ExecuteRaw(NStr::CStr("DROP TABLE IF EXISTS raw_zero_probe"));
			umint nCreated = co_await _Database.f_ExecuteRaw(NStr::CStr("CREATE TABLE raw_zero_probe (id INTEGER NOT NULL)"));
			DMibExpect(nCreated, ==, umint(0));
			co_await _Database.f_ExecuteRaw(NStr::CStr("DROP TABLE raw_zero_probe"));

			NContainer::TCVector<CSqlValue> DeleteParameters;
			DeleteParameters.f_InsertLast(NStr::CStr("raw-nondml-affected@example.com"));
			umint nDeleted = co_await _Database.f_ExecuteRaw(fg_TestSqlDeletePersonByEmailSql(_Capabilities), fg_Move(DeleteParameters));
			DMibExpect(nDeleted, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Raw SQL parameter binding without string concatenation") -> NConcurrency::TCFuture<void>
		{
			NContainer::TCVector<CSqlValue> Parameters;
			Parameters.f_InsertLast(NStr::CStr("with-quote@example.'com"));
			umint nInserted = co_await _Database.f_ExecuteRaw(fg_TestSqlInsertPersonSql(_Capabilities), fg_Move(Parameters));
			DMibExpect(nInserted, ==, umint(1));

			NContainer::TCVector<CSqlValue> SelectParameters;
			SelectParameters.f_InsertLast(NStr::CStr("with-quote@example.'com"));
			CSqlRawResult Result = co_await _Database.f_QueryRaw(fg_TestSqlSelectPersonByEmailSql(_Capabilities), fg_Move(SelectParameters));
			DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));

			NContainer::TCVector<CSqlValue> DeleteParameters;
			DeleteParameters.f_InsertLast(NStr::CStr("with-quote@example.'com"));
			umint nDeleted = co_await _Database.f_ExecuteRaw(fg_TestSqlDeletePersonByEmailSql(_Capabilities), fg_Move(DeleteParameters));
			DMibExpect(nDeleted, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Raw SQL inside transaction") -> NConcurrency::TCFuture<void>
		{
			NStr::CStr InsertSql = fg_TestSqlInsertPersonSql(_Capabilities);
			NStr::CStr SelectSql = fg_TestSqlSelectPersonByEmailSql(_Capabilities);
			NStr::CStr DeleteSql = fg_TestSqlDeletePersonByEmailSql(_Capabilities);

			co_await _Database.f_WithTransaction
				(
					[InsertSql, SelectSql](CSqlTransaction _Transaction) mutable -> NConcurrency::TCFuture<void>
					{
						NContainer::TCVector<CSqlValue> Parameters;
						Parameters.f_InsertLast(NStr::CStr("raw-tx@example.com"));
						umint nInserted = co_await _Transaction.f_ExecuteRaw(fg_Move(InsertSql), fg_Move(Parameters));
						DMibExpect(nInserted, ==, umint(1));

						NContainer::TCVector<CSqlValue> SelectParameters;
						SelectParameters.f_InsertLast(NStr::CStr("raw-tx@example.com"));
						CSqlRawResult Result = co_await _Transaction.f_QueryRaw(fg_Move(SelectSql), fg_Move(SelectParameters));
						DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));

						co_return {};
					}
				)
			;

			NContainer::TCVector<CSqlValue> CleanupParameters;
			CleanupParameters.f_InsertLast(NStr::CStr("raw-tx@example.com"));
			umint nDeleted = co_await _Database.f_ExecuteRaw(fg_Move(DeleteSql), fg_Move(CleanupParameters));
			DMibExpect(nDeleted, ==, umint(1));

			co_return {};
		};

		DMibTestCategory("Raw SQL backend marker enforcement") -> NConcurrency::TCFuture<void>
		{
			if (_Capabilities.m_bNumberedPlaceholders)
			{
				auto Result = co_await _Database.f_ExecuteRaw(fg_SqlSqliteRaw(NStr::CStr("SELECT 1"))).f_Wrap();
				DMibExpect(bool(Result), ==, false);
			}
			else
			{
				auto Result = co_await _Database.f_ExecuteRaw(fg_SqlPostgresRaw(NStr::CStr("SELECT 1"))).f_Wrap();
				DMibExpect(bool(Result), ==, false);
			}

			co_return {};
		};

		DMibTestCategory("Raw SQL streaming yields columns and row batches") -> NConcurrency::TCFuture<void>
		{
			using namespace NMib::NStr;

			NStr::CStr InsertSql = fg_TestSqlInsertPersonSql(_Capabilities);
			for (umint iIteration = 0; iIteration < 5; ++iIteration)
			{
				NContainer::TCVector<CSqlValue> Parameters;
				Parameters.f_InsertLast("raw-stream-{}@example.com"_f << iIteration);
				co_await _Database.f_ExecuteRaw(InsertSql, fg_Move(Parameters));
			}

			CSqlRawOperation Operation;
			Operation.m_Sql = "SELECT id, email FROM people WHERE email LIKE 'raw-stream-%@example.com' ORDER BY email";
			Operation.m_nRowsPerBatch = 2;

			auto Stream = co_await _Database.f_QueryRawStream(fg_Move(Operation));
			DMibExpect(Stream.m_Columns.f_GetLen(), ==, umint(2));
			DMibExpect(Stream.m_Columns[0].m_Name, ==, NStr::CStr("id"));
			DMibExpect(Stream.m_Columns[1].m_Name, ==, NStr::CStr("email"));

			umint nBatches = 0;
			umint nRows = 0;
			for (auto iBatch = co_await fg_Move(Stream.m_Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
			{
				++nBatches;
				nRows += (*iBatch).f_GetLen();
			}

			DMibExpect(nRows, ==, umint(5));
			DMibExpect(nBatches, >=, umint(2));

			co_await _Database.f_ExecuteRaw(NStr::CStr("DELETE FROM people WHERE email LIKE 'raw-stream-%@example.com'"));

			co_return {};
		};

		DMibTestCategory("Raw SQL streaming abandonment releases connection") -> NConcurrency::TCFuture<void>
		{
			using namespace NMib::NStr;

			NStr::CStr InsertSql = fg_TestSqlInsertPersonSql(_Capabilities);
			for (umint iIteration = 0; iIteration < 4; ++iIteration)
			{
				NContainer::TCVector<CSqlValue> Parameters;
				Parameters.f_InsertLast("raw-stream-abandon-{}@example.com"_f << iIteration);
				co_await _Database.f_ExecuteRaw(InsertSql, fg_Move(Parameters));
			}

			{
				CSqlRawOperation Operation;
				Operation.m_Sql = "SELECT id, email FROM people WHERE email LIKE 'raw-stream-abandon-%@example.com' ORDER BY email";
				Operation.m_nRowsPerBatch = 1;

				auto Stream = co_await _Database.f_QueryRawStream(fg_Move(Operation));
				DMibExpect(Stream.m_Columns.f_GetLen(), ==, umint(2));

				auto iBatch = co_await fg_Move(Stream.m_Rows).f_GetSimpleIterator();
				if (iBatch)
				{
					(void)*iBatch;
					co_await fg_Move(iBatch).f_Destroy();
				}
			}

			umint nRemaining = co_await _Database.f_Count<gc_SelectPersonByEmailLike>(NStr::CStr("raw-stream-abandon-%@example.com"));
			DMibExpect(nRemaining, ==, umint(4));

			co_await _Database.f_ExecuteRaw(NStr::CStr("DELETE FROM people WHERE email LIKE 'raw-stream-abandon-%@example.com'"));

			co_return {};
		};

		DMibTestCategory("Raw SQL transaction stream blocks transaction completion") -> NConcurrency::TCFuture<void>
		{
			using namespace NMib::NStr;

			NStr::CStr InsertSql = fg_TestSqlInsertPersonSql(_Capabilities);
			for (umint iIteration = 0; iIteration < 8; ++iIteration)
			{
				NContainer::TCVector<CSqlValue> Parameters;
				Parameters.f_InsertLast("raw-tx-stream-block-{}@example.com"_f << iIteration);
				co_await _Database.f_ExecuteRaw(InsertSql, fg_Move(Parameters));
			}

			{
				auto Transaction = co_await _Database.f_BeginTransaction();

				CSqlRawOperation Operation;
				Operation.m_Sql = "SELECT id, email FROM people WHERE email LIKE 'raw-tx-stream-block-%@example.com' ORDER BY email";
				Operation.m_nRowsPerBatch = 1;

				auto Stream = co_await Transaction.f_QueryRawStream(fg_Move(Operation));
				auto iBatch = co_await fg_Move(Stream.m_Rows).f_GetSimpleIterator();
				DMibExpect(bool(iBatch), ==, true);

				NContainer::TCVector<CSqlValue> InsertAfterParameters;
				InsertAfterParameters.f_InsertLast(NStr::CStr("raw-tx-stream-block-after@example.com"));
				auto InsertAfterStream = Transaction.f_ExecuteRaw(InsertSql, fg_Move(InsertAfterParameters));

				NContainer::TCVector<CSqlValue> SelectAfterParameters;
				SelectAfterParameters.f_InsertLast(NStr::CStr("raw-tx-stream-block-after@example.com"));
				auto SelectAfterStream = Transaction.f_QueryRaw(fg_TestSqlSelectPersonByEmailSql(_Capabilities), fg_Move(SelectAfterParameters));

				co_await fg_Move(iBatch).f_Destroy().f_Timeout(gc_StreamHandoffTimeout, "Timed out destroying the transaction stream");
				DMibExpect(co_await fg_Move(InsertAfterStream).f_Timeout(gc_StreamHandoffTimeout, "Timed out waiting for the insert after the stream"), ==, umint(1));

				CSqlRawResult AfterStreamResult = co_await fg_Move(SelectAfterStream).f_Timeout(gc_StreamHandoffTimeout, "Timed out waiting for the select after the stream");
				DMibExpect(AfterStreamResult.m_Rows.f_GetLen(), ==, umint(1));

				co_await Transaction.f_Commit().f_Timeout(gc_StreamHandoffTimeout, "Timed out committing after the stream");
			}

			co_await _Database.f_ExecuteRaw(NStr::CStr("DELETE FROM people WHERE email LIKE 'raw-tx-stream-block-%@example.com'"));

			co_return {};
		};

		if (_Capabilities.m_bUnrecognizedBackend)
		{
			DMibTestCategory("Raw SQL preserves PostgreSQL unrecognized backend values") -> NConcurrency::TCFuture<void>
			{
				CSqlRawResult Result = co_await _Database.f_QueryRaw(fg_SqlPostgresRaw(NStr::CStr("SELECT 'point-1'::tsvector AS unrecognized")));

				DMibExpect(Result.m_Rows.f_GetLen(), ==, umint(1));
				DMibExpect(Result.m_Columns.f_GetLen(), ==, umint(1));
				DMibExpect(Result.m_Columns[0].m_Name, ==, NStr::CStr("unrecognized"));

				if (Result.m_Rows.f_GetLen() == 1 && Result.m_Rows[0].m_Values.f_GetLen() == 1)
				{
					auto &Value = Result.m_Rows[0].m_Values[0];
					DMibExpect(Value.f_GetTypeID(), ==, ESqlValueType::mc_UnrecognizedBackend);

					if (Value.f_GetTypeID() == ESqlValueType::mc_UnrecognizedBackend)
					{
						auto const &Unrecognized = Value.f_GetAsType<CSqlUnrecognizedBackendValue>();
						DMibExpect(Unrecognized.m_TypeID, !=, uint32(0));
						DMibExpect(Unrecognized.m_Bytes.f_IsEmpty(), ==, false);
					}
				}

				co_return {};
			};
		}

		co_return {};
	}
}
