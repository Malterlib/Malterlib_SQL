// Copyright © 2015 Hansoft AB 
// Distributed under the MIT license, see license text in LICENSE.Malterlib

#include <Mib/SQL/SQL>

using namespace NMib::NStr;
using namespace NMib::NStorage;
using namespace NMib::NContainer;

using NMib::fg_Explicit;

void fg_Malterlib_SQL_MySql_MakeActive();
void fg_Malterlib_SQL_SQLite_MakeActive();

namespace 
{
	class CDB_Tests : public NMib::NTest::CTest
	{
	public:

		enum EFlag
		{
			EFlag_None			= 0,
			EFlag_NoBinding		= DMibBit(0),
			EFlag_MySqlSyntax	= DMibBit(1),
		};
		
		void f_DoSQLConnectionTests(NMib::NSQL::CSQLConnection &_SQLConn, EFlag _Flags)
		{
			NMib::NSQL::CSQLConnection &SQLConn = _SQLConn;

			// Test CREATE TABLE
			TCUniquePointer<NMib::NSQL::CQueryResult> pResults = fg_Explicit(SQLConn.f_ExecuteQuery("CREATE TABLE TestTable (P_Id INTEGER, Name TEXT, PRIMARY KEY (P_Id) )"));
			DMibTest(DMibExpr(pResults)) (ETest_FailAndStop);

			// Test INSERT
			for (int i= 0; i < 100; ++i)
			{
				pResults =	fg_Explicit(SQLConn.f_ExecuteQuery(CStr(CStr(CStr::CFormat("INSERT INTO TestTable VALUES ({}, '{}')") << i << i))));
				DMibTest(DMibExpr(pResults) && DMibExpr(2)) (ETest_FailAndStop) (ETestFlag_Aggregated);
				//					DMibTest(DMibExpr(pResults->f_NumAffectedRows()) == DMibExpr(1)) (ETest_FailAndStop); // SQLite is not obeying the docs here.
			}

			// Test Select
			pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("SELECT * FROM TestTable WHERE P_Id < 50 ORDER BY P_Id ASC"));
			DMibTest(DMibExpr(!pResults.f_IsEmpty())) (ETest_FailAndStop);
			DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(50)) (ETest_FailAndStop);

			int64 Val, Req;
			for (int64 i= 0; i < 50; ++i)
			{
				Req = i;
				Val = pResults->f_GetInt64(i, 0);
				DMibTest(DMibExpr(Val) == DMibExpr(Req)) (ETest_FailAndStop) (ETestFlag_Aggregated);
			}

			// Test DELETE
			pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("DELETE FROM TestTable WHERE P_Id < 50"));
			DMibTest(DMibExpr(!pResults.f_IsEmpty()) && DMibExpr(2)) (ETest_FailAndStop);
			//				DMibTest(DMibExpr(pResults->f_NumAffectedRows()) == DMibExpr(50)) (ETest_FailAndStop);

			// Test results of DELETE:
			pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("SELECT * FROM TestTable ORDER BY P_Id ASC"));
			DMibTest(DMibExpr(!pResults.f_IsEmpty()) && DMibExpr(3)) (ETest_FailAndStop);
			DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(50) && DMibExpr(2)) (ETest_FailAndStop);

			for (int64 i= 0; i < 50; ++i)
			{
				Req = i + 50;
				Val = pResults->f_GetInt64(i, 0);
				DMibTest(DMibExpr(Val) == DMibExpr(Req) && DMibExpr(2)) (ETest_FailAndStop) (ETestFlag_Aggregated);
			}

			// Test Queries with bound params.
			if (!(_Flags & EFlag_NoBinding))
			{
				DMibTestPath("Bound params");
				TCUniquePointer<NMib::NSQL::CQuery> pParamQuery = fg_Explicit(SQLConn.f_CreateQuery("SELECT * FROM TestTable WHERE P_Id < ? ORDER BY P_Id ASC"));
				DMibTest(DMibExpr(pParamQuery)) (ETest_FailAndStop);

				TCUniquePointer<NMib::NSQL::CQueryInstance> pQInst = fg_Explicit(SQLConn.f_CreateQueryInstance(pParamQuery.f_Get()));
				DMibTest(DMibExpr(pQInst)) (ETest_FailAndStop);

				DMibTest(DMibExpr(pQInst->f_BindParameter<int32>(0, 75)) > DMibExpr(0)) (ETest_FailAndStop);

				pResults =	fg_Explicit(SQLConn.f_ExecuteQuery(pQInst.f_Get()));
				DMibTest(DMibExpr(!pResults.f_IsEmpty())) (ETest_FailAndStop);
				DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(25)) (ETest_FailAndStop);
				for (int64 i= 0; i < 25; ++i)
				{
					Req = i + 50;
					Val = pResults->f_GetInt64(i, 0);
					DMibTest(DMibExpr(Val) == DMibExpr(Req)) (ETest_FailAndStop) (ETestFlag_Aggregated);
				}
			}

			// Test failing transaction
			{
				DMibTestPath("Failing transaction");
				TCUniquePointer<NMib::NSQL::CTransaction> pTransaction = fg_Explicit(SQLConn.f_CreateTransaction());
				DMibTest(DMibExpr(pTransaction)) (ETest_FailAndStop);

				// Transaction adding one entry and then deleting one non-existant entry.


				TCUniquePointer<NMib::NSQL::CQuery> pQueryOne = fg_Explicit(SQLConn.f_CreateQuery("INSERT INTO TestTable VALUES (500, '500')"));
				DMibTest(DMibExpr(pQueryOne)) (ETest_FailAndStop);

				DMibTest(DMibExpr(pTransaction->f_AddQuery(pQueryOne.f_Get()))) (ETest_FailAndStop);

				TCUniquePointer<NMib::NSQL::CQuery> pQueryTwo = (_Flags & EFlag_MySqlSyntax) ? fg_Explicit(SQLConn.f_CreateQuery("INSERT INTO TestTable VALUES (500, 'Failure')")) : fg_Explicit(SQLConn.f_CreateQuery("INSERT OR FAIL INTO TestTable VALUES (500, 'Failure')"));
				DMibTest(DMibExpr(pQueryTwo)) (ETest_FailAndStop);

				DMibTest(DMibExpr(pTransaction->f_AddQuery(pQueryTwo.f_Get()))) (ETest_FailAndStop);

				TCUniquePointer<NMib::NSQL::CTransactionResult> pTransactionResult = fg_Explicit(SQLConn.f_CommitTransaction(pTransaction.f_Get()));
				DMibTest(DMibExpr(!pTransactionResult)) (ETest_FailAndStop);

				//					pTransactionResult->

				// Test results of Rolledback transaction:
				pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("SELECT * FROM TestTable WHERE P_Id = 500"));
				DMibTest(DMibExpr(!pResults.f_IsEmpty())) (ETest_FailAndStop);
				DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(0)) (ETest_FailAndStop);
			}

			// Test successful transaction
			{
				DMibTestPath("Successful transaction");
				TCUniquePointer<NMib::NSQL::CTransaction> pTransaction = fg_Explicit(SQLConn.f_CreateTransaction());
				DMibTest(DMibExpr(pTransaction)) (ETest_FailAndStop);

				// Transaction adding one entry and then deleting one non-existant entry.


				TCUniquePointer<NMib::NSQL::CQuery> pQueryOne = fg_Explicit(SQLConn.f_CreateQuery("INSERT INTO TestTable VALUES (500, '500')"));
				DMibTest(DMibExpr(pQueryOne)) (ETest_FailAndStop);

				DMibTest(DMibExpr(pTransaction->f_AddQuery(pQueryOne.f_Get()))) (ETest_FailAndStop);


				TCUniquePointer<NMib::NSQL::CQuery> pQueryTwo = (_Flags & EFlag_MySqlSyntax) ? fg_Explicit(SQLConn.f_CreateQuery("REPLACE INTO TestTable VALUES (500, 'Success')")) : fg_Explicit(SQLConn.f_CreateQuery("INSERT OR REPLACE INTO TestTable VALUES (500, 'Success')"));
				DMibTest(DMibExpr(pQueryTwo)) (ETest_FailAndStop);

				DMibTest(DMibExpr(pTransaction->f_AddQuery(pQueryTwo.f_Get()))) (ETest_FailAndStop);

				TCUniquePointer<NMib::NSQL::CTransactionResult> pTransactionResult = fg_Explicit(SQLConn.f_CommitTransaction(pTransaction.f_Get()));
				DMibTest(DMibExpr(pTransactionResult)) (ETest_FailAndStop);

				//					pTransactionResult->

				// Test results of Rolledback transaction:
				pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("SELECT * FROM TestTable WHERE P_Id = 500"));
				DMibTest(DMibExpr(!pResults.f_IsEmpty()) && DMibExpr(6)) (ETest_FailAndStop);
				DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(1)) (ETest_FailAndStop);

				int64 ID = pResults->f_GetInt64(0, 0);
				int64 ReqID = 500;
				DMibTest(DMibExpr(ID) == DMibExpr(ReqID));

				CStr MsgStr = pResults->f_GetString(0, 1);
				CStr ReqMsgStr = "Success";
				DMibTest(DMibExpr(MsgStr) == DMibExpr(ReqMsgStr));
			}
		}

		void f_DoTests()
		{
			
			///*
			DMibTestSuite("SQLite")
			{
				fg_Malterlib_SQL_SQLite_MakeActive();
				
				NMib::NSQL::CSQLConnection SQLConn;

				// Test create DB (in memory)
				CRegistry_CStr Registry;
				Registry.f_SetValue("Database", ":memory:");
				DMibTest(DMibExpr(SQLConn.f_Create("NMib::NSQL::CDatabaseImplementation_SQLite", Registry, 1))) (ETest_FailAndStop);

				f_DoSQLConnectionTests(SQLConn, EFlag_None);
			};
			//*/

#ifdef HAVE_YOU_SET_UP_A_LOCALHOST_MYSQL_SERVER
			///*
			DMibTestSuite("MySQL")
			{
				fg_Malterlib_SQL_MySql_MakeActive();
				NMib::NSQL::CSQLConnection SQLConn;

				// Connect to local db.
				CRegistry_CStr Registry;
				Registry.f_SetValue("Host", "localhost");
				Registry.f_SetValue("User", "root");
				Registry.f_SetValue("Password", "asdasd");
				DMibTest(DMibExpr(SQLConn.f_Create("NMib::NSQL::CDatabaseImplementation_MySql", Registry, 1))) (ETest_FailAndStop);

				TCUniquePointer<NMib::NSQL::CQueryResult> pResults;

				pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("CREATE DATABASE MalterlibCertifierTestDB"));
				DMibTest(DMibExpr(!pResults.f_IsEmpty())) (ETest_FailAndStop);
				DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(0)) (ETest_FailAndStop);

				pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("USE MalterlibCertifierTestDB"));
				DMibTest(DMibExpr(!pResults.f_IsEmpty()) && DMibExpr(2)) (ETest_FailAndStop);
				DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(0) && DMibExpr(2)) (ETest_FailAndStop);

				f_DoSQLConnectionTests(SQLConn, EFlag_NoBinding | EFlag_MySqlSyntax);

				pResults =	fg_Explicit(SQLConn.f_ExecuteQuery("DROP DATABASE MalterlibCertifierTestDB"));
				DMibTest(DMibExpr(!pResults.f_IsEmpty()) && DMibExpr(3)) (ETest_FailAndStop);
				DMibTest(DMibExpr(pResults->f_NumReturnedRows()) == DMibExpr(0) && DMibExpr(3)) (ETest_FailAndStop);
			};
			//*/
#endif

		}

	};
}
DMibTestRegister(CDB_Tests, Malterlib::SQL);
