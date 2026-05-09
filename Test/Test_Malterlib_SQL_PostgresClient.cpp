// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/PostgresClientProtocol>
#include <Mib/Concurrency/AsyncDestroy>
#include <Mib/Cryptography/RandomID>
#include <Mib/Encoding/Base64>
#include <Mib/Stream/ByteVector>
#include <Mib/Test/Exception>

#include "Test_Malterlib_SQL_PostgresShared.h"

using namespace NMib;
using namespace NMib::NContainer;
using namespace NMib::NConcurrency;
using namespace NMib::NCryptography;
using namespace NMib::NSQL;
using namespace NMib::NSQL::NTest::NPostgres;
using namespace NMib::NStr;

namespace
{
	using CPostgresTestReadStream = NStream::CBinaryStreamMemoryPtr<NStream::CBinaryStreamBigEndian>;
	using CPostgresTestWriteStream = NStream::CBinaryStreamMemory<NStream::CBinaryStreamBigEndian, CIOByteVector>;
	using CPostgresTestWriteRefStream = NStream::CBinaryStreamMemoryRef<NStream::CBinaryStreamBigEndian, CIOByteVector>;

	uint8 fg_ReadByte(CIOByteVector const &_Data, umint _Offset)
	{
		CPostgresTestReadStream Stream;
		Stream.f_OpenRead(_Data);
		Stream.f_AddPosition(_Offset);

		uint8 Value;
		Stream >> Value;

		return Value;
	}

	uint32 fg_ReadBE32(CIOByteVector const &_Data, umint _Offset)
	{
		CPostgresTestReadStream Stream;
		Stream.f_OpenRead(_Data);
		Stream.f_AddPosition(_Offset);

		uint32 Value;
		Stream >> Value;

		return Value;
	}

	bool fg_ContainsCString(CIOByteVector const &_Data, CStr const &_Value)
	{
		CPostgresTestReadStream Stream;
		Stream.f_OpenRead(_Data);

		umint Match = 0;
		while (!Stream.f_IsAtEndOfStream())
		{
			uint8 Byte;
			Stream >> Byte;

			if (Match < _Value.f_GetLen() && Byte == uint8(_Value.f_GetStr()[Match]))
			{
				++Match;
				if (Match == _Value.f_GetLen())
				{
					if (Stream.f_IsAtEndOfStream())
						return false;

					uint8 Terminator;
					Stream >> Terminator;

					if (Terminator == 0)
						return true;

					Match = Terminator == uint8(_Value.f_GetStr()[0]) ? 1 : 0;
				}
			}
			else
				Match = Byte == uint8(_Value.f_GetStr()[0]) ? 1 : 0;
		}

		return false;
	}

	void fg_AppendByte(CIOByteVector &o_Data, uint8 _Value)
	{
		CPostgresTestWriteRefStream Stream(o_Data);
		Stream.f_SetPositionFromEnd(0);
		Stream << _Value;
	}

	void fg_AppendInt16(CIOByteVector &o_Data, uint16 _Value)
	{
		CPostgresTestWriteRefStream Stream(o_Data);
		Stream.f_SetPositionFromEnd(0);
		Stream << _Value;
	}

	void fg_AppendInt32(CIOByteVector &o_Data, uint32 _Value)
	{
		CPostgresTestWriteRefStream Stream(o_Data);
		Stream.f_SetPositionFromEnd(0);
		Stream << _Value;
	}

	void fg_AppendCString(CIOByteVector &o_Data, CStr const &_Value)
	{
		CPostgresTestWriteRefStream Stream(o_Data);
		Stream.f_SetPositionFromEnd(0);
		Stream.f_FeedBytes(_Value.f_GetStr(), _Value.f_GetLen());
		Stream << uint8(0);
	}

	void fg_AppendBytes(CIOByteVector &o_Data, CIOByteVector const &_Bytes)
	{
		CPostgresTestReadStream Source;
		Source.f_OpenRead(_Bytes);

		CPostgresTestWriteRefStream Destination(o_Data);
		Destination.f_SetPositionFromEnd(0);
		Destination.f_FeedFromStream(Source, Source.f_GetLength());
	}

	void fg_AppendStringBytes(CIOByteVector &o_Data, CStr const &_Value)
	{
		CPostgresTestWriteRefStream Stream(o_Data);
		Stream.f_SetPositionFromEnd(0);
		Stream.f_FeedBytes(_Value.f_GetStr(), _Value.f_GetLen());
	}

	CIOByteVector fg_BackendMessage(uint8 _Type, CIOByteVector const &_Payload)
	{
		CPostgresTestWriteStream Stream;
		Stream << _Type;
		Stream << uint32(_Payload.f_GetLen() + 4);

		CPostgresTestReadStream PayloadStream;
		PayloadStream.f_OpenRead(_Payload);
		Stream.f_FeedFromStream(PayloadStream, PayloadStream.f_GetLength());

		return Stream.f_MoveVector();
	}

	CStr fg_BytesToString(CIOByteVector const &_Bytes)
	{
		return CStr((ch8 const *)_Bytes.f_GetArray(), _Bytes.f_GetLen());
	}

	uint64 fg_TestMicrosecondsToFractionInt(uint64 _Microseconds)
	{
		constexpr uint64 c_MicrosecondsPerSecond = 1000000;

		return
			(NTime::NPrivate::CConst::mc_FractionDividend / c_MicrosecondsPerSecond) * _Microseconds
			+ (NTime::NPrivate::CConst::mc_FractionDividend % c_MicrosecondsPerSecond) * _Microseconds / c_MicrosecondsPerSecond
		;
	}

	struct CPostgresIntegrationScenario
	{
		CStr m_TestPath;
		CStr m_DirectoryName;
		uint16 m_Port = 0;
		bool m_bTLS = false;
		bool m_bClientCertificate = false;
	};

	TCFuture<void> fg_RunPostgresIntegrationScenario(CPostgresIntegrationScenario _Scenario)
	{
		DMibTestPath(_Scenario.m_TestPath);
		co_await fg_WithPostgresTestServer
			(
				{
					.m_DirectoryName = _Scenario.m_DirectoryName
					, .m_Port = _Scenario.m_Port
					, .m_bTLS = _Scenario.m_bTLS
					, .m_bClientCertificate = _Scenario.m_bClientCertificate
					, .m_MissingExecutableWarning = "Warning: Failed to find postgres executables, disabling client actor tests\n"
				}
				, g_ActorFunctor / [](CPostgresConnectionSettings _Settings) -> TCFuture<void>
				{
					auto CaptureScope = co_await g_CaptureExceptions;

					auto ExtraClientSettings = _Settings;

					TCActor<CPostgresClientActor> Client = fg_CreatePostgresClient();
					auto CleanupClient = co_await fg_AsyncDestroy(Client);

					co_await Client(&CPostgresClientActor::f_Connect, fg_Move(_Settings)).f_Timeout(gc_Timeout, "Timed out connecting to PostgreSQL test server");
					{
						auto DuplicateConnectResult = co_await Client
							(
								&CPostgresClientActor::f_Connect
								, ExtraClientSettings
							)
							.f_Timeout(gc_Timeout, "Timed out checking duplicate PostgreSQL connection rejection")
							.f_Wrap()
						;

						DMibExpectException
							(
								DuplicateConnectResult.f_Access()
								, DMibErrorInstance("PostgreSQL socket is already connected")
							)
						;
					}
					{
						TCActor<CPostgresClientActor> ClosedClient = fg_CreatePostgresClient();
						auto CleanupClosedClient = co_await fg_AsyncDestroy(ClosedClient);

						co_await ClosedClient
							(
								&CPostgresClientActor::f_Connect
								, ExtraClientSettings
							)
							.f_Timeout(gc_Timeout, "Timed out connecting close-check PostgreSQL client")
						;
						co_await ClosedClient(&CPostgresClientActor::f_Close).f_Timeout(gc_Timeout, "Timed out closing close-check PostgreSQL client");

						auto ClosedExecuteResult = co_await ClosedClient(&CPostgresClientActor::f_Execute, CStr("select 1::int8 as value"))
							.f_Timeout(gc_Timeout, "Timed out checking closed PostgreSQL command rejection")
							.f_Wrap()
						;

						DMibExpectException
							(
								ClosedExecuteResult.f_Access()
								, DMibErrorInstance("PostgreSQL socket is not connected")
							)
						;

						co_await fg_Move(ClosedClient).f_Destroy();
						CleanupClosedClient.f_Clear();
					}
					{
						TCActor<CPostgresClientActor> TimeoutClient = fg_CreatePostgresClient();
						auto CleanupTimeoutClient = co_await fg_AsyncDestroy(TimeoutClient);
						auto TimeoutSettings = ExtraClientSettings;
						TimeoutSettings.m_SocketTimeout = 0.2;
						TimeoutSettings.m_KeepAliveInterval = 0.0;

						co_await TimeoutClient
							(
								&CPostgresClientActor::f_Connect
								, fg_Move(TimeoutSettings)
							)
							.f_Timeout(gc_Timeout, "Timed out connecting timeout-check PostgreSQL client")
						;
						auto TimeoutExecuteResult = co_await TimeoutClient(&CPostgresClientActor::f_Execute, CStr("select pg_sleep(2)"))
							.f_Timeout(gc_Timeout, "Timed out checking PostgreSQL socket timeout")
							.f_Wrap()
						;

						DMibExpectException
							(
								TimeoutExecuteResult.f_Access()
								, DMibErrorInstance("PostgreSQL socket closed")
							)
						;

						co_await fg_Move(TimeoutClient).f_Destroy();
						CleanupTimeoutClient.f_Clear();
					}
					{
						TCActor<CPostgresClientActor> ReconnectClient = fg_CreatePostgresClient();
						auto CleanupReconnectClient = co_await fg_AsyncDestroy(ReconnectClient);
						auto BadSettings = ExtraClientSettings;
						BadSettings.m_Port = uint16(1);
						BadSettings.m_SocketTimeout = 0.2;
						BadSettings.m_KeepAliveInterval = 0.0;

						auto FailedConnectResult = co_await ReconnectClient
							(
								&CPostgresClientActor::f_Connect
								, fg_Move(BadSettings)
							)
							.f_Timeout(gc_Timeout, "Timed out checking failed PostgreSQL connect")
							.f_Wrap()
						;

						DMibExpect(bool(FailedConnectResult), ==, false);

						co_await ReconnectClient
							(
								&CPostgresClientActor::f_Connect
								, ExtraClientSettings
							)
							.f_Timeout(gc_Timeout, "Timed out reconnecting PostgreSQL client after failed connect")
						;

						auto ReconnectResult = co_await ReconnectClient(&CPostgresClientActor::f_Execute, CStr("select 1::int8 as value"))
							.f_Timeout(gc_Timeout, "Timed out executing PostgreSQL query after failed connect")
						;

						DMibExpect(ReconnectResult.m_Rows.f_GetLen(), ==, umint(1));
						DMibExpect(ReconnectResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

						co_await fg_Move(ReconnectClient).f_Destroy();
						CleanupReconnectClient.f_Clear();
					}
					{
						TCActor<CPostgresClientActor> AuthFailureClient = fg_CreatePostgresClient();
						auto CleanupAuthFailureClient = co_await fg_AsyncDestroy(AuthFailureClient);
						auto BadPasswordSettings = ExtraClientSettings;
						BadPasswordSettings.m_Password = "wrong_password";
						BadPasswordSettings.m_KeepAliveInterval = 0.0;

						auto AuthFailureResult = co_await AuthFailureClient
							(
								&CPostgresClientActor::f_Connect
								, fg_Move(BadPasswordSettings)
							)
							.f_Timeout(gc_Timeout, "Timed out checking failed PostgreSQL authentication")
							.f_Wrap()
						;

						DMibExpect(bool(AuthFailureResult), ==, false);

						co_await AuthFailureClient
							(
								&CPostgresClientActor::f_Connect
								, ExtraClientSettings
							)
							.f_Timeout(gc_Timeout, "Timed out reconnecting PostgreSQL client after failed authentication")
						;

						auto AuthFailureReconnectResult = co_await AuthFailureClient(&CPostgresClientActor::f_Execute, CStr("select 1::int8 as value"))
							.f_Timeout(gc_Timeout, "Timed out executing PostgreSQL query after failed authentication")
						;

						DMibExpect(AuthFailureReconnectResult.m_Rows.f_GetLen(), ==, umint(1));
						DMibExpect(AuthFailureReconnectResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

						co_await fg_Move(AuthFailureClient).f_Destroy();
						CleanupAuthFailureClient.f_Clear();
					}

					TCVector<CPostgresValue> QueryParameters;
					QueryParameters.f_InsertLast(int64(42));

					auto QueryResult = co_await Client
						(
							&CPostgresClientActor::f_ExecuteWithParameters
							, CStr("select $1::int8 as value")
							, fg_Move(QueryParameters)
						)
						.f_Timeout(gc_Timeout, "Timed out executing PostgreSQL query")
					;

					DMibExpect(bool(QueryResult.m_RowDescription), ==, true);
					DMibExpect(QueryResult.m_RowDescription->m_Fields.f_GetLen(), ==, umint(1));
					DMibExpect(QueryResult.m_RowDescription->m_Fields[0].m_Name, ==, "value");
					DMibExpect(QueryResult.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(QueryResult.m_Rows[0].m_Values.f_GetLen(), ==, umint(1));
					DMibExpect(QueryResult.m_Rows[0].m_Values[0].f_GetTypeID(), ==, EPostgresValueType::mc_Integer64);
					DMibExpect(QueryResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(42));
					DMibExpect(bool(QueryResult.m_CommandComplete), ==, true);
					DMibExpect(QueryResult.m_CommandComplete->m_Tag, ==, "SELECT 1");
					DMibExpect(QueryResult.m_ReadyStatus, ==, EPostgresReadyForQueryStatus::mc_Idle);

					TCVector<CPostgresValue> CachedQueryParameters;
					CachedQueryParameters.f_InsertLast(int64(43));

					auto CachedResult = co_await Client
						(
							&CPostgresClientActor::f_ExecuteWithParameters
							, CStr("select $1::int8 as value")
							, fg_Move(CachedQueryParameters)
						)
						.f_Timeout(gc_Timeout, "Timed out executing cached PostgreSQL query")
					;

					DMibExpect(CachedResult.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(CachedResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(43));

					auto CachedStatementCount = co_await Client
						(
							&CPostgresClientActor::f_Execute
							, CStr("select count(*)::int8 as value from pg_prepared_statements where statement = 'select $1::int8 as value'")
						)
						.f_Timeout(gc_Timeout, "Timed out checking PostgreSQL prepared statement cache")
					;

					DMibExpect(CachedStatementCount.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(CachedStatementCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

					// A statement that parses but fails at execute (the division by zero is computed from the parameter,
					// so it is a run-time error) must still be cached. Executing the same failing statement twice must
					// leave exactly one prepared statement on the session rather than leaking a fresh one per failure.
					TCVector<CPostgresValue> FailingParameters;
					FailingParameters.f_InsertLast(int64(7));
					auto FirstFailingResult = co_await Client
						(
							&CPostgresClientActor::f_ExecuteWithParameters
							, CStr("select $1::int8 / ($1::int8 - $1::int8) as value")
							, fg_Move(FailingParameters)
						)
						.f_Timeout(gc_Timeout, "Timed out executing failing PostgreSQL query")
						.f_Wrap()
					;
					DMibExpect(bool(FirstFailingResult), ==, false);

					TCVector<CPostgresValue> SecondFailingParameters;
					SecondFailingParameters.f_InsertLast(int64(8));
					auto SecondFailingResult = co_await Client
						(
							&CPostgresClientActor::f_ExecuteWithParameters
							, CStr("select $1::int8 / ($1::int8 - $1::int8) as value")
							, fg_Move(SecondFailingParameters)
						)
						.f_Timeout(gc_Timeout, "Timed out executing failing PostgreSQL query again")
						.f_Wrap()
					;
					DMibExpect(bool(SecondFailingResult), ==, false);

					auto FailingStatementCount = co_await Client
						(
							&CPostgresClientActor::f_Execute
							, CStr("select count(*)::int8 as value from pg_prepared_statements where statement = 'select $1::int8 / ($1::int8 - $1::int8) as value'")
						)
						.f_Timeout(gc_Timeout, "Timed out checking failing PostgreSQL prepared statement cache")
					;

					DMibExpect(FailingStatementCount.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(FailingStatementCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(1));

					TCVector<CPostgresValue> StreamParameters;
					StreamParameters.f_InsertLast(int64(3));

					auto StreamRows = co_await Client
						(
							&CPostgresClientActor::f_ExecuteRows
							, CStr("select generate_series(1, $1::int8)::int8 as value")
							, fg_Move(StreamParameters)
							, umint(2)
						)
						.f_Timeout(gc_Timeout, "Timed out creating PostgreSQL row stream")
					;

					umint nStreamRows = 0;
					umint nStreamBatches = 0;
					int64 StreamSum = 0;
					bool bStreamRowsWellFormed = true;
					bool bStreamBatchSizesCorrect = true;

					for (auto iBatch = co_await fg_Move(StreamRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
					{
						auto StreamBatch = *iBatch;
						bStreamBatchSizesCorrect &= StreamBatch.f_GetLen() == (nStreamBatches == 0 ? 2 : 1);

						for (auto &StreamRow : StreamBatch)
						{
							bStreamRowsWellFormed &= StreamRow.m_Values.f_GetLen() == 1;

							if (StreamRow.m_Values.f_GetLen() == 1)
								StreamSum += StreamRow.m_Values[0].f_GetAsType<int64>();

							++nStreamRows;
						}

						++nStreamBatches;
					}

					DMibExpect(bStreamRowsWellFormed, ==, true);
					DMibExpect(bStreamBatchSizesCorrect, ==, true);
					DMibExpect(nStreamBatches, ==, umint(2));
					DMibExpect(nStreamRows, ==, umint(3));
					DMibExpect(StreamSum, ==, int64(6));

					co_await Client
						(
							&CPostgresClientActor::f_PrepareStatement
							, CStr("malterlib_explicit_prepared")
							, CStr("select $1::int8 + 1 as value")
							, TCVector<EPostgresValueType>{EPostgresValueType::mc_Integer64}
						)
						.f_Timeout(gc_Timeout, "Timed out preparing explicit PostgreSQL statement")
					;

					TCVector<CPostgresValue> ExplicitPreparedParameters;
					ExplicitPreparedParameters.f_InsertLast(int64(44));

					auto ExplicitPreparedResult = co_await Client
						(
							&CPostgresClientActor::f_ExecutePrepared
							, CStr("malterlib_explicit_prepared")
							, fg_Move(ExplicitPreparedParameters)
						)
						.f_Timeout(gc_Timeout, "Timed out executing explicit PostgreSQL prepared statement")
					;

					DMibExpect(ExplicitPreparedResult.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(ExplicitPreparedResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(45));

					co_await Client
						(
							&CPostgresClientActor::f_PrepareStatement
							, CStr("malterlib_explicit_prepared_stream")
							, CStr("select generate_series(1, $1::int8)::int8 as value")
							, TCVector<EPostgresValueType>{EPostgresValueType::mc_Integer64}
						)
						.f_Timeout(gc_Timeout, "Timed out preparing explicit PostgreSQL streaming statement")
					;

					TCVector<CPostgresValue> ExplicitPreparedStreamParameters;
					ExplicitPreparedStreamParameters.f_InsertLast(int64(3));

					auto ExplicitPreparedStreamRows = co_await Client
						(
							&CPostgresClientActor::f_ExecutePreparedRows
							, CStr("malterlib_explicit_prepared_stream")
							, fg_Move(ExplicitPreparedStreamParameters)
							, umint(2)
						)
						.f_Timeout(gc_Timeout, "Timed out creating explicit PostgreSQL prepared row stream")
					;

					umint nExplicitPreparedStreamRows = 0;
					umint nExplicitPreparedStreamBatches = 0;
					int64 ExplicitPreparedStreamSum = 0;
					bool bExplicitPreparedStreamRowsWellFormed = true;
					bool bExplicitPreparedStreamBatchSizesCorrect = true;

					for (auto iBatch = co_await fg_Move(ExplicitPreparedStreamRows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
					{
						auto ExplicitPreparedStreamBatch = *iBatch;
						bExplicitPreparedStreamBatchSizesCorrect &= ExplicitPreparedStreamBatch.f_GetLen() == (nExplicitPreparedStreamBatches == 0 ? 2 : 1);

						for (auto &ExplicitPreparedStreamRow : ExplicitPreparedStreamBatch)
						{
							bExplicitPreparedStreamRowsWellFormed &= ExplicitPreparedStreamRow.m_Values.f_GetLen() == 1;

							if (ExplicitPreparedStreamRow.m_Values.f_GetLen() == 1)
								ExplicitPreparedStreamSum += ExplicitPreparedStreamRow.m_Values[0].f_GetAsType<int64>();

							++nExplicitPreparedStreamRows;
						}

						++nExplicitPreparedStreamBatches;
					}

					DMibExpect(bExplicitPreparedStreamRowsWellFormed, ==, true);
					DMibExpect(bExplicitPreparedStreamBatchSizesCorrect, ==, true);
					DMibExpect(nExplicitPreparedStreamBatches, ==, umint(2));
					DMibExpect(nExplicitPreparedStreamRows, ==, umint(3));
					DMibExpect(ExplicitPreparedStreamSum, ==, int64(6));

					co_await Client
						(
							&CPostgresClientActor::f_DeallocatePrepared
							, CStr("malterlib_explicit_prepared_stream")
						)
						.f_Timeout(gc_Timeout, "Timed out deallocating explicit PostgreSQL streaming statement")
					;

					co_await Client
						(
							&CPostgresClientActor::f_DeallocatePrepared
							, CStr("malterlib_explicit_prepared")
						)
						.f_Timeout(gc_Timeout, "Timed out deallocating explicit PostgreSQL prepared statement")
					;

					auto ExplicitPreparedCount = co_await Client
						(
							&CPostgresClientActor::f_Execute
							, CStr("select count(*)::int8 as value from pg_prepared_statements where name = 'malterlib_explicit_prepared'")
						)
						.f_Timeout(gc_Timeout, "Timed out checking explicit PostgreSQL prepared statement deallocation")
					;

					DMibExpect(ExplicitPreparedCount.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(ExplicitPreparedCount.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(0));

					co_await Client(&CPostgresClientActor::f_Execute, CStr("drop table if exists malterlib_concurrency_guard"))
						.f_Timeout(gc_Timeout, "Timed out dropping existing PostgreSQL concurrency guard table")
					;

					co_await Client(&CPostgresClientActor::f_Execute, CStr("create table malterlib_concurrency_guard(value int8)"))
						.f_Timeout(gc_Timeout, "Timed out creating PostgreSQL concurrency guard table")
					;

					co_await Client(&CPostgresClientActor::f_BeginTransaction, false, CSqlTransactionSettings())
						.f_Timeout(gc_Timeout, "Timed out beginning PostgreSQL concurrency guard transaction")
					;

					co_await Client(&CPostgresClientActor::f_Execute, CStr("lock table malterlib_concurrency_guard in access exclusive mode"))
						.f_Timeout(gc_Timeout, "Timed out locking PostgreSQL concurrency guard table")
					;

					TCActor<CPostgresClientActor> BlockedClient = fg_CreatePostgresClient();
					auto CleanupBlockedClient = co_await fg_AsyncDestroy(BlockedClient);

					co_await BlockedClient(&CPostgresClientActor::f_Connect, ExtraClientSettings).f_Timeout(gc_Timeout, "Timed out connecting blocked PostgreSQL test client");

					auto BlockedQuery = BlockedClient
						(
							&CPostgresClientActor::f_Execute
							, CStr("select 11::int8 as value from malterlib_concurrency_guard")
						)
						.f_Timeout(gc_Timeout, "Timed out executing blocked PostgreSQL query")
					;
					auto ConcurrentQuery = BlockedClient
						(
							&CPostgresClientActor::f_Execute
							, CStr("select 12::int8 as value")
						)
						.f_Timeout(gc_Timeout, "Timed out executing queued PostgreSQL query")
					;

					co_await Client(&CPostgresClientActor::f_RollbackTransaction).f_Timeout(gc_Timeout, "Timed out releasing PostgreSQL concurrency guard lock");

					auto BlockedResult = co_await fg_Move(BlockedQuery);
					auto ConcurrentResult = co_await fg_Move(ConcurrentQuery);

					DMibExpect(BlockedResult.m_Rows.f_GetLen(), ==, umint(0));
					DMibExpect(ConcurrentResult.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(ConcurrentResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(12));

					co_await fg_Move(BlockedClient).f_Destroy();
					CleanupBlockedClient.f_Clear();

					co_await Client(&CPostgresClientActor::f_Execute, CStr("drop table malterlib_concurrency_guard"))
						.f_Timeout(gc_Timeout, "Timed out dropping PostgreSQL concurrency guard table")
					;

					TCVector<CPostgresValue> TypedParameters;
					TypedParameters.f_InsertLast(fp64(3.5));
					TypedParameters.f_InsertLast(CStr("hello"));
					TypedParameters.f_InsertLast(true);

					CIOByteVector ByteParameter;
					fg_AppendByte(ByteParameter, uint8(0));
					fg_AppendByte(ByteParameter, uint8(1));
					fg_AppendByte(ByteParameter, uint8(2));

					CPostgresValue ByteParameterValue;
					ByteParameterValue.f_SetAsType<CIOByteVector>(fg_Move(ByteParameter));
					TypedParameters.f_InsertLast(fg_Move(ByteParameterValue));

					CPostgresValue IntervalParameterValue;
					IntervalParameterValue.f_SetAsType<CPostgresInterval>
						(
							CPostgresInterval
								{
									.m_Months = 2
									, .m_Days = 3
									, .m_Time = NTime::CTimeSpanConvert::fs_CreateSpanFromSeconds(14706, 0.789)
								}
						)
					;
					TypedParameters.f_InsertLast(fg_Move(IntervalParameterValue));

					CPostgresValue IntegerArrayParameterValue;
					TCPostgresArray<int64> IntegerArrayParameter;
					IntegerArrayParameter.m_Values.f_InsertLast(int64(1));
					IntegerArrayParameter.m_Values.f_InsertLast(NStorage::TCOptional<int64>());
					IntegerArrayParameter.m_Values.f_InsertLast(int64(3));

					IntegerArrayParameterValue.f_SetAsType<TCPostgresArray<int64>>(fg_Move(IntegerArrayParameter));
					TypedParameters.f_InsertLast(fg_Move(IntegerArrayParameterValue));

					TypedParameters.f_InsertLast(fp32(1.25));

					CPostgresValue Float32ArrayParameterValue;
					TCPostgresArray<fp32> Float32ArrayParameter;
					Float32ArrayParameter.m_Values.f_InsertLast(fp32(2.5));
					Float32ArrayParameter.m_Values.f_InsertLast(NStorage::TCOptional<fp32>());
					Float32ArrayParameter.m_Values.f_InsertLast(fp32(3.75));

					Float32ArrayParameterValue.f_SetAsType<TCPostgresArray<fp32>>(fg_Move(Float32ArrayParameter));
					TypedParameters.f_InsertLast(fg_Move(Float32ArrayParameterValue));

					TypedParameters.f_InsertLast(int16(-12));
					TypedParameters.f_InsertLast(int32(123456));

					CPostgresValue Integer16ArrayParameterValue;
					TCPostgresArray<int16> Integer16ArrayParameter;
					Integer16ArrayParameter.m_Values.f_InsertLast(int16(4));
					Integer16ArrayParameter.m_Values.f_InsertLast(NStorage::TCOptional<int16>());
					Integer16ArrayParameter.m_Values.f_InsertLast(int16(6));

					Integer16ArrayParameterValue.f_SetAsType<TCPostgresArray<int16>>(fg_Move(Integer16ArrayParameter));
					TypedParameters.f_InsertLast(fg_Move(Integer16ArrayParameterValue));

					CPostgresValue Integer32ArrayParameterValue;
					TCPostgresArray<int32> Integer32ArrayParameter;
					Integer32ArrayParameter.m_Values.f_InsertLast(int32(4000));
					Integer32ArrayParameter.m_Values.f_InsertLast(NStorage::TCOptional<int32>());
					Integer32ArrayParameter.m_Values.f_InsertLast(int32(6000));

					Integer32ArrayParameterValue.f_SetAsType<TCPostgresArray<int32>>(fg_Move(Integer32ArrayParameter));
					TypedParameters.f_InsertLast(fg_Move(Integer32ArrayParameterValue));

					auto TypedResult = co_await Client
						(
							&CPostgresClientActor::f_ExecuteWithParameters
							, CStr
								(
									"select $1::float8 as float_value, $2::text as text_value, $3::bool as bool_value, null::text as null_value"
									", $4::bytea as bytes_value, date '2020-01-02' as date_value"
									", time '03:04:05.123' as time_value, timestamp '2020-01-02 03:04:05.123' as timestamp_value"
									", timestamptz '2020-01-02 03:04:05.123+00' as timestamptz_value"
									", '{\"a\":1,\"b\":[true]}'::json as json_value, '{\"b\":2,\"a\":1}'::jsonb as jsonb_value"
									", $5::interval as interval_value"
									", $6::int8[] as integer_array_value, array[['a','b'],['c',null]]::text[] as text_array_value"
									", $7::float4 as float32_value, $8::float4[] as float32_array_value"
									", $9::int2 as integer16_value, $10::int4 as integer32_value"
									", $11::int2[] as integer16_array_value, $12::int4[] as integer32_array_value, point(1, 2) as unrecognized_value"
								)
							, fg_Move(TypedParameters)
						)
						.f_Timeout(gc_Timeout, "Timed out executing PostgreSQL typed value query")
					;
					DMibExpect(TypedResult.m_Rows.f_GetLen(), ==, umint(1));

					auto const &TypedValues = TypedResult.m_Rows[0].m_Values;

					DMibExpect(TypedValues.f_GetLen(), ==, umint(21));
					DMibExpect(TypedValues[0].f_GetTypeID(), ==, EPostgresValueType::mc_Float64);
					DMibExpect(TypedValues[0].f_GetAsType<fp64>(), ==, fp64(3.5));
					DMibExpect(TypedValues[1].f_GetTypeID(), ==, EPostgresValueType::mc_Text);
					DMibExpect(TypedValues[1].f_GetAsType<CStr>(), ==, "hello");
					DMibExpect(TypedValues[2].f_GetTypeID(), ==, EPostgresValueType::mc_Boolean);
					DMibExpect(TypedValues[2].f_GetAsType<bool>(), ==, true);
					DMibExpect(TypedValues[3].f_GetTypeID(), ==, EPostgresValueType::mc_Null);
					DMibExpect(TypedValues[4].f_GetTypeID(), ==, EPostgresValueType::mc_Bytes);
					DMibExpect(fg_BytesToString(TypedValues[4].f_GetAsType<CIOByteVector>()), ==, CStr("\0\1\2", 3));
					DMibExpect(TypedValues[5].f_GetTypeID(), ==, EPostgresValueType::mc_Date);
					DMibExpect(NTime::fg_GetFullTimeStr(TypedValues[5].f_GetAsType<CPostgresDate>().m_Time), ==, "2020-01-02 00:00:00.000");
					DMibExpect(TypedValues[6].f_GetTypeID(), ==, EPostgresValueType::mc_Time);
					DMibExpect(NTime::fg_GetFullTimeStr(TypedValues[6].f_GetAsType<CPostgresTime>().m_Time), ==, "1970-01-01 03:04:05.123");
					DMibExpect(TypedValues[7].f_GetTypeID(), ==, EPostgresValueType::mc_Timestamp);
					DMibExpect(NTime::fg_GetFullTimeStr(TypedValues[7].f_GetAsType<CPostgresTimestamp>().m_Time), ==, "2020-01-02 03:04:05.123");
					DMibExpect(TypedValues[8].f_GetTypeID(), ==, EPostgresValueType::mc_TimestampTz);
					DMibExpect(NTime::fg_GetFullTimeStr(TypedValues[8].f_GetAsType<CPostgresTimestampTz>().m_Time), ==, "2020-01-02 03:04:05.123");
					DMibExpect(TypedValues[9].f_GetTypeID(), ==, EPostgresValueType::mc_Json);
					DMibExpect(TypedValues[9].f_GetAsType<NEncoding::CJsonOrdered>()["a"].f_Integer(), ==, int64(1));
					DMibExpect(TypedValues[9].f_GetAsType<NEncoding::CJsonOrdered>()["b"].f_Array().f_GetLen(), ==, umint(1));
					DMibExpect(TypedValues[10].f_GetTypeID(), ==, EPostgresValueType::mc_Jsonb);
					DMibExpect(TypedValues[10].f_GetAsType<NEncoding::CJsonSorted>()["a"].f_Integer(), ==, int64(1));
					DMibExpect(TypedValues[10].f_GetAsType<NEncoding::CJsonSorted>()["b"].f_Integer(), ==, int64(2));
					DMibExpect(TypedValues[11].f_GetTypeID(), ==, EPostgresValueType::mc_Interval);
					DMibExpect(TypedValues[11].f_GetAsType<CPostgresInterval>().m_Months, ==, int32(2));
					DMibExpect(TypedValues[11].f_GetAsType<CPostgresInterval>().m_Days, ==, int32(3));
					DMibExpect(TypedValues[11].f_GetAsType<CPostgresInterval>().m_Time.f_GetSeconds(), ==, int64(14706));
					DMibExpect(TypedValues[11].f_GetAsType<CPostgresInterval>().m_Time.f_GetFractionInt(), ==, fg_TestMicrosecondsToFractionInt(789000));

					DMibExpect(TypedValues[12].f_GetTypeID(), ==, EPostgresValueType::mc_Array_Integer64);
					auto const &Integer64Array = TypedValues[12].f_GetAsType<TCPostgresArray<int64>>();

					DMibExpect(Integer64Array.m_Dimensions.f_GetLen(), ==, umint(1));
					DMibExpect(Integer64Array.m_Dimensions[0].m_Length, ==, int32(3));
					DMibExpect(Integer64Array.m_Dimensions[0].m_LowerBound, ==, int32(1));
					DMibExpect(Integer64Array.m_Values.f_GetLen(), ==, umint(3));
					DMibExpect(*Integer64Array.m_Values[0], ==, int64(1));
					DMibExpect(bool(Integer64Array.m_Values[1]), ==, false);
					DMibExpect(*Integer64Array.m_Values[2], ==, int64(3));

					DMibExpect(TypedValues[13].f_GetTypeID(), ==, EPostgresValueType::mc_Array_Text);
					auto const &TextArray = TypedValues[13].f_GetAsType<TCPostgresArray<CStr>>();

					DMibExpect(TextArray.m_Dimensions.f_GetLen(), ==, umint(2));
					DMibExpect(TextArray.m_Dimensions[0].m_Length, ==, int32(2));
					DMibExpect(TextArray.m_Dimensions[1].m_Length, ==, int32(2));
					DMibExpect(TextArray.m_Values.f_GetLen(), ==, umint(4));
					DMibExpect(*TextArray.m_Values[0], ==, "a");
					DMibExpect(*TextArray.m_Values[1], ==, "b");
					DMibExpect(*TextArray.m_Values[2], ==, "c");
					DMibExpect(bool(TextArray.m_Values[3]), ==, false);

					DMibExpect(TypedValues[14].f_GetTypeID(), ==, EPostgresValueType::mc_Float32);
					DMibExpect(TypedValues[14].f_GetAsType<fp32>(), ==, fp32(1.25));

					DMibExpect(TypedValues[15].f_GetTypeID(), ==, EPostgresValueType::mc_Array_Float32);
					auto const &Float32Array = TypedValues[15].f_GetAsType<TCPostgresArray<fp32>>();

					DMibExpect(Float32Array.m_Dimensions.f_GetLen(), ==, umint(1));
					DMibExpect(Float32Array.m_Dimensions[0].m_Length, ==, int32(3));
					DMibExpect(Float32Array.m_Dimensions[0].m_LowerBound, ==, int32(1));
					DMibExpect(Float32Array.m_Values.f_GetLen(), ==, umint(3));
					DMibExpect(*Float32Array.m_Values[0], ==, fp32(2.5));
					DMibExpect(bool(Float32Array.m_Values[1]), ==, false);
					DMibExpect(*Float32Array.m_Values[2], ==, fp32(3.75));

					DMibExpect(TypedValues[16].f_GetTypeID(), ==, EPostgresValueType::mc_Integer16);
					DMibExpect(TypedValues[16].f_GetAsType<int16>(), ==, int16(-12));
					DMibExpect(TypedValues[17].f_GetTypeID(), ==, EPostgresValueType::mc_Integer32);
					DMibExpect(TypedValues[17].f_GetAsType<int32>(), ==, int32(123456));

					DMibExpect(TypedValues[18].f_GetTypeID(), ==, EPostgresValueType::mc_Array_Integer16);
					auto const &Integer16Array = TypedValues[18].f_GetAsType<TCPostgresArray<int16>>();

					DMibExpect(Integer16Array.m_Dimensions.f_GetLen(), ==, umint(1));
					DMibExpect(Integer16Array.m_Dimensions[0].m_Length, ==, int32(3));
					DMibExpect(Integer16Array.m_Dimensions[0].m_LowerBound, ==, int32(1));
					DMibExpect(Integer16Array.m_Values.f_GetLen(), ==, umint(3));
					DMibExpect(*Integer16Array.m_Values[0], ==, int16(4));
					DMibExpect(bool(Integer16Array.m_Values[1]), ==, false);
					DMibExpect(*Integer16Array.m_Values[2], ==, int16(6));

					DMibExpect(TypedValues[19].f_GetTypeID(), ==, EPostgresValueType::mc_Array_Integer32);
					auto const &Integer32Array = TypedValues[19].f_GetAsType<TCPostgresArray<int32>>();

					DMibExpect(Integer32Array.m_Dimensions.f_GetLen(), ==, umint(1));
					DMibExpect(Integer32Array.m_Dimensions[0].m_Length, ==, int32(3));
					DMibExpect(Integer32Array.m_Dimensions[0].m_LowerBound, ==, int32(1));
					DMibExpect(Integer32Array.m_Values.f_GetLen(), ==, umint(3));
					DMibExpect(*Integer32Array.m_Values[0], ==, int32(4000));
					DMibExpect(bool(Integer32Array.m_Values[1]), ==, false);
					DMibExpect(*Integer32Array.m_Values[2], ==, int32(6000));

					DMibExpect(TypedValues[20].f_GetTypeID(), ==, EPostgresValueType::mc_Unrecognized);
					auto const &UnrecognizedValue = TypedValues[20].f_GetAsType<CPostgresUnrecognizedValue>();

					DMibExpect(UnrecognizedValue.m_TypeOID, ==, uint32(600));
					DMibExpect(UnrecognizedValue.m_Bytes.f_GetLen(), ==, umint(16));

					{
						DMibTestPath("pre-epoch date with time component");

						// A date before the 2000-01-01 PostgreSQL epoch carrying a non-midnight time must encode to the
						// correct calendar day. With truncating division 1999-12-31 12:00 became day 0 (2000-01-01); the
						// encoder must floor toward negative infinity so it stays 1999-12-31.
						CPostgresValue PreEpochDateValue;
						PreEpochDateValue.f_SetAsType<CPostgresDate>(CPostgresDate{.m_Time = NTime::CTimeConvert::fs_CreateTime(1999, 12, 31, 12, 0, 0)});

						TCVector<CPostgresValue> PreEpochDateParameters;
						PreEpochDateParameters.f_InsertLast(fg_Move(PreEpochDateValue));

						auto PreEpochDateResult = co_await Client
							(
								&CPostgresClientActor::f_ExecuteWithParameters
								, CStr("select $1::date as value")
								, fg_Move(PreEpochDateParameters)
							)
							.f_Timeout(gc_Timeout, "Timed out executing pre-epoch date query")
						;

						DMibExpect(PreEpochDateResult.m_Rows.f_GetLen(), ==, umint(1));
						DMibExpect(PreEpochDateResult.m_Rows[0].m_Values[0].f_GetTypeID(), ==, EPostgresValueType::mc_Date);
						DMibExpect(NTime::fg_GetFullTimeStr(PreEpochDateResult.m_Rows[0].m_Values[0].f_GetAsType<CPostgresDate>().m_Time), ==, "1999-12-31 00:00:00.000");
					}

					{
						DMibTestPath("missing table error");

						auto ErrorResult = co_await Client(&CPostgresClientActor::f_Execute, CStr("select * from malterlib_missing_table"))
							.f_Timeout(gc_Timeout, "Timed out executing failing PostgreSQL query")
							.f_Wrap()
						;

						DMibExpectException
							(
								ErrorResult.f_Access()
								, DMibErrorSqlInstance("PostgreSQL query failed (42P01): relation \"malterlib_missing_table\" does not exist", {})
							)
						;

						auto Error = fg_TryGetSqlErrorData(ErrorResult.f_ExceptionPointer());
						DMibExpect(bool(Error), ==, true);

						if (Error)
						{
							DMibExpect(Error->m_Category, ==, ESqlErrorCategory::mc_Generic);
							DMibExpect(Error->m_Backend, ==, CStr("postgres"));
							DMibExpect(Error->m_BackendCode, ==, CStr("42P01"));
							DMibExpect(Error->m_BackendMessage, ==, CStr("relation \"malterlib_missing_table\" does not exist"));
						}
					}

					co_await Client(&CPostgresClientActor::f_BeginTransaction, false, CSqlTransactionSettings())
						.f_Timeout(gc_Timeout, "Timed out beginning PostgreSQL transaction")
					;

					auto TransactionResult = co_await Client(&CPostgresClientActor::f_Execute, CStr("select 7::int8 as value"))
						.f_Timeout(gc_Timeout, "Timed out executing PostgreSQL transaction query")
					;

					DMibExpect(TransactionResult.m_ReadyStatus, ==, EPostgresReadyForQueryStatus::mc_InTransaction);
					DMibExpect(TransactionResult.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(TransactionResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(7));

					co_await Client(&CPostgresClientActor::f_CommitTransaction).f_Timeout(gc_Timeout, "Timed out committing PostgreSQL transaction");

					co_await Client(&CPostgresClientActor::f_BeginTransaction, true, CSqlTransactionSettings())
						.f_Timeout(gc_Timeout, "Timed out beginning read-only PostgreSQL transaction")
					;

					auto ReadOnlyResult = co_await Client(&CPostgresClientActor::f_Execute, CStr("select 8::int8 as value"))
						.f_Timeout(gc_Timeout, "Timed out executing read-only PostgreSQL transaction query")
					;

					DMibExpect(ReadOnlyResult.m_ReadyStatus, ==, EPostgresReadyForQueryStatus::mc_InTransaction);
					DMibExpect(ReadOnlyResult.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(ReadOnlyResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(8));

					co_await Client(&CPostgresClientActor::f_RollbackTransaction).f_Timeout(gc_Timeout, "Timed out rolling back PostgreSQL transaction");

					co_await Client(&CPostgresClientActor::f_BeginTransaction, false, CSqlTransactionSettings())
						.f_Timeout(gc_Timeout, "Timed out beginning failed PostgreSQL transaction test")
					;

					{
						DMibTestPath("transaction missing table error");

						auto TransactionErrorResult = co_await Client
							(
								&CPostgresClientActor::f_Execute
								, CStr("select * from malterlib_missing_table_in_transaction")
							)
							.f_Timeout(gc_Timeout, "Timed out executing failing PostgreSQL transaction query")
							.f_Wrap()
						;

						DMibExpectException
							(
								TransactionErrorResult.f_Access()
								, DMibErrorSqlInstance("PostgreSQL query failed (42P01): relation \"malterlib_missing_table_in_transaction\" does not exist", {})
							)
						;

						auto Error = fg_TryGetSqlErrorData(TransactionErrorResult.f_ExceptionPointer());
						DMibExpect(bool(Error), ==, true);

						if (Error)
						{
							DMibExpect(Error->m_Category, ==, ESqlErrorCategory::mc_Generic);
							DMibExpect(Error->m_Backend, ==, CStr("postgres"));
							DMibExpect(Error->m_BackendCode, ==, CStr("42P01"));
							DMibExpect(Error->m_BackendMessage, ==, CStr("relation \"malterlib_missing_table_in_transaction\" does not exist"));
						}
					}

					{
						DMibTestPath("aborted transaction error");

						auto FailedTransactionResult = co_await Client(&CPostgresClientActor::f_Execute, CStr("select 9::int8 as value"))
							.f_Timeout(gc_Timeout, "Timed out checking failed PostgreSQL transaction state")
							.f_Wrap()
						;

						DMibExpectException
							(
								FailedTransactionResult.f_Access()
								, DMibErrorSqlInstance("PostgreSQL query failed (25P02): current transaction is aborted, commands ignored until end of transaction block", {})
							)
						;

						auto Error = fg_TryGetSqlErrorData(FailedTransactionResult.f_ExceptionPointer());
						DMibExpect(bool(Error), ==, true);

						if (Error)
						{
							DMibExpect(Error->m_Category, ==, ESqlErrorCategory::mc_Generic);
							DMibExpect(Error->m_Backend, ==, CStr("postgres"));
							DMibExpect(Error->m_BackendCode, ==, CStr("25P02"));
							DMibExpect(Error->m_BackendMessage, ==, CStr("current transaction is aborted, commands ignored until end of transaction block"));
						}
					}

					co_await Client(&CPostgresClientActor::f_RollbackTransaction).f_Timeout(gc_Timeout, "Timed out rolling back failed PostgreSQL transaction");

					auto RecoveredResult = co_await Client(&CPostgresClientActor::f_Execute, CStr("select 10::int8 as value"))
						.f_Timeout(gc_Timeout, "Timed out checking PostgreSQL recovery after rollback")
					;

					DMibExpect(RecoveredResult.m_ReadyStatus, ==, EPostgresReadyForQueryStatus::mc_Idle);
					DMibExpect(RecoveredResult.m_Rows.f_GetLen(), ==, umint(1));
					DMibExpect(RecoveredResult.m_Rows[0].m_Values[0].f_GetAsType<int64>(), ==, int64(10));

					co_await Client(&CPostgresClientActor::f_BeginTransaction, true, CSqlTransactionSettings())
						.f_Timeout(gc_Timeout, "Timed out beginning read-only failure PostgreSQL transaction")
					;

					{
						DMibTestPath("read-only write error");

						auto ReadOnlyWriteResult = co_await Client
							(
								&CPostgresClientActor::f_Execute
								, CStr("create temporary table malterlib_read_only_failure(value int8)")
							)
							.f_Timeout(gc_Timeout, "Timed out checking read-only PostgreSQL transaction failure")
							.f_Wrap()
						;

						DMibExpectException
							(
								ReadOnlyWriteResult.f_Access()
								, DMibErrorSqlInstance("PostgreSQL query failed (25006): cannot execute CREATE TABLE in a read-only transaction", {})
							)
						;

						auto Error = fg_TryGetSqlErrorData(ReadOnlyWriteResult.f_ExceptionPointer());
						DMibExpect(bool(Error), ==, true);

						if (Error)
						{
							DMibExpect(Error->m_Category, ==, ESqlErrorCategory::mc_Generic);
							DMibExpect(Error->m_Backend, ==, CStr("postgres"));
							DMibExpect(Error->m_BackendCode, ==, CStr("25006"));
							DMibExpect(Error->m_BackendMessage, ==, CStr("cannot execute CREATE TABLE in a read-only transaction"));
						}
					}

					co_await Client(&CPostgresClientActor::f_RollbackTransaction).f_Timeout(gc_Timeout, "Timed out rolling back read-only failure PostgreSQL transaction");

					co_await fg_Move(Client).f_Destroy();
					CleanupClient.f_Clear();

					co_return {};
				}
			)
		;

		co_return {};
	}

	TCFuture<void> fg_RunPostgresIntegrationSuite(CPostgresIntegrationScenario _Scenario)
	{
		co_await fg_RunPostgresIntegrationScenario(fg_Move(_Scenario));

		co_return {};
	}
}

struct CPostgresClient_Tests : public NMib::NTest::CTest
{
	void f_DoTests()
	{
		DMibTestSuite("Postgres client protocol") -> NConcurrency::TCFuture<void>
		{
			{
				DMibTestPath("SSL request");
				auto Message = fg_PostgresBuildSSLRequest();

				DMibExpect(Message.f_GetLen(), ==, umint(8));
				DMibExpect(fg_ReadBE32(Message, 0), ==, uint32(8));
				DMibExpect(fg_ReadBE32(Message, 4), ==, uint32(80877103));
			}

			{
				DMibTestPath("Startup message");
				CPostgresConnectionSettings Settings;
				Settings.m_User = "test_user";
				Settings.m_Database = "test_database";
				Settings.m_ApplicationName = "MalterlibTest";

				auto Message = fg_PostgresBuildStartupMessage(Settings);

				DMibExpect(fg_ReadBE32(Message, 0), ==, uint32(Message.f_GetLen()));
				DMibExpect(fg_ReadBE32(Message, 4), ==, uint32(196608));
				DMibExpect(fg_ReadByte(Message, Message.f_GetLen() - 1), ==, uint8(0));
				DMibExpect(fg_ContainsCString(Message, "user"), ==, true);
				DMibExpect(fg_ContainsCString(Message, "test_user"), ==, true);
				DMibExpect(fg_ContainsCString(Message, "database"), ==, true);
				DMibExpect(fg_ContainsCString(Message, "test_database"), ==, true);
				DMibExpect(fg_ContainsCString(Message, "client_encoding"), ==, true);
				DMibExpect(fg_ContainsCString(Message, "UTF8"), ==, true);
			}

			{
				DMibTestPath("Frontend/backend frame");
				CIOByteVector Payload;
				fg_AppendByte(Payload, uint8('o'));
				fg_AppendByte(Payload, uint8('k'));
				auto FrontendMessage = fg_PostgresBuildFrontendMessage(uint8('Q'), Payload);

				DMibExpect(fg_ReadByte(FrontendMessage, 0), ==, uint8('Q'));
				DMibExpect(fg_ReadBE32(FrontendMessage, 1), ==, uint32(6));

				auto BackendMessage = co_await fg_PostgresReadBackendMessage(fg_Move(FrontendMessage));

				DMibExpect(BackendMessage.m_Type, ==, uint8('Q'));
				DMibExpect(BackendMessage.m_Payload.f_GetLen(), ==, Payload.f_GetLen());
				DMibExpect(fg_ReadByte(BackendMessage.m_Payload, 0), ==, uint8('o'));
				DMibExpect(fg_ReadByte(BackendMessage.m_Payload, 1), ==, uint8('k'));
			}

			{
				DMibTestPath("SASL frontend messages");
				auto Initial = fg_PostgresBuildSASLInitialResponse("SCRAM-SHA-256", "n,,n=user,r=nonce");

				DMibExpect(fg_ReadByte(Initial, 0), ==, uint8('p'));
				DMibExpect(fg_ReadBE32(Initial, 1), ==, uint32(39));
				DMibExpect(fg_ContainsCString(Initial, "SCRAM-SHA-256"), ==, true);
				DMibExpect(fg_ReadBE32(Initial, 19), ==, uint32(17));

				auto Response = fg_PostgresBuildSASLResponse("c=biws,r=nonce,p=proof");

				DMibExpect(fg_ReadByte(Response, 0), ==, uint8('p'));
				DMibExpect(fg_ReadBE32(Response, 1), ==, uint32(26));
			}

			{
				DMibTestPath("Extended query frontend messages");
				TCVector<uint32> ParameterOIDs;
				ParameterOIDs.f_InsertLast(23);
				auto Parse = fg_PostgresBuildParse("stmt", "select $1", ParameterOIDs);

				DMibExpect(fg_ReadByte(Parse, 0), ==, uint8('P'));
				DMibExpect(fg_ReadBE32(Parse, 1), ==, uint32(25));
				DMibExpect(fg_ContainsCString(Parse, "stmt"), ==, true);
				DMibExpect(fg_ContainsCString(Parse, "select $1"), ==, true);

				TCVector<CPostgresValue> Parameters;
				Parameters.f_InsertLast(int64(42));
				Parameters.f_InsertLast(CPostgresValue());
				TCVector<uint16> ResultFormats;
				ResultFormats.f_InsertLast(uint16(0));
				auto Bind = co_await fg_PostgresBuildBind("portal", "stmt", Parameters, ResultFormats);

				DMibExpect(fg_ReadByte(Bind, 0), ==, uint8('B'));
				DMibExpect(fg_ReadBE32(Bind, 1), ==, uint32(44));

				auto Describe = fg_PostgresBuildDescribe(EPostgresDescribeTarget::mc_Portal, "portal");

				DMibExpect(fg_ReadByte(Describe, 0), ==, uint8('D'));
				DMibExpect(fg_ReadByte(Describe, 5), ==, uint8('P'));

				auto Execute = fg_PostgresBuildExecute("portal", 25);

				DMibExpect(fg_ReadByte(Execute, 0), ==, uint8('E'));
				DMibExpect(fg_ReadBE32(Execute, 12), ==, uint32(25));

				auto Sync = fg_PostgresBuildSync();

				DMibExpect(fg_ReadByte(Sync, 0), ==, uint8('S'));
				DMibExpect(fg_ReadBE32(Sync, 1), ==, uint32(4));

				auto Terminate = fg_PostgresBuildTerminate();

				DMibExpect(fg_ReadByte(Terminate, 0), ==, uint8('X'));
				DMibExpect(fg_ReadBE32(Terminate, 1), ==, uint32(4));
			}

			{
				DMibTestPath("Authentication request decoding");
				CIOByteVector Payload;
				fg_AppendInt32(Payload, 10);
				fg_AppendCString(Payload, "SCRAM-SHA-256");
				fg_AppendCString(Payload, "SCRAM-SHA-256-PLUS");
				fg_AppendByte(Payload, uint8(0));

				auto Request = co_await fg_PostgresDecodeAuthenticationRequest(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('R', Payload)));

				DMibExpect(Request.m_Type, ==, EPostgresAuthenticationRequestType::mc_SASL);
				DMibExpect(Request.m_SASLMechanisms.f_GetLen(), ==, umint(2));
				DMibExpect(Request.m_SASLMechanisms[0], ==, "SCRAM-SHA-256");
				DMibExpect(Request.m_SASLMechanisms[1], ==, "SCRAM-SHA-256-PLUS");
			}

			{
				DMibTestPath("Startup backend decoding");
				CIOByteVector ParameterPayload;
				fg_AppendCString(ParameterPayload, "server_version");
				fg_AppendCString(ParameterPayload, "18.3");

				auto ParameterStatus = co_await fg_PostgresDecodeParameterStatus(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('S', ParameterPayload)));

				DMibExpect(ParameterStatus.m_Name, ==, "server_version");
				DMibExpect(ParameterStatus.m_Value, ==, "18.3");

				CIOByteVector KeyPayload;
				fg_AppendInt32(KeyPayload, 1234);
				fg_AppendInt32(KeyPayload, 5678);

				auto KeyData = co_await fg_PostgresDecodeBackendKeyData(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('K', KeyPayload)));

				DMibExpect(KeyData.m_ProcessID, ==, uint32(1234));
				DMibExpect(KeyData.m_SecretKey, ==, uint32(5678));

				CIOByteVector ReadyPayload;
				fg_AppendByte(ReadyPayload, uint8('I'));
				DMibExpect(co_await fg_PostgresDecodeReadyForQuery(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('Z', ReadyPayload))), ==, EPostgresReadyForQueryStatus::mc_Idle);
			}

			{
				DMibTestPath("Result backend decoding");
				co_await fg_PostgresDecodeParseComplete(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('1', {})));
				co_await fg_PostgresDecodeBindComplete(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('2', {})));
				co_await fg_PostgresDecodeNoData(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('n', {})));

				CIOByteVector CommandPayload;
				fg_AppendCString(CommandPayload, "SELECT 1");

				auto Command = co_await fg_PostgresDecodeCommandComplete(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('C', CommandPayload)));

				DMibExpect(Command.m_Tag, ==, "SELECT 1");

				CIOByteVector ErrorPayload;
				fg_AppendByte(ErrorPayload, uint8('S'));
				fg_AppendCString(ErrorPayload, "ERROR");
				fg_AppendByte(ErrorPayload, uint8('C'));
				fg_AppendCString(ErrorPayload, "42601");
				fg_AppendByte(ErrorPayload, uint8('M'));
				fg_AppendCString(ErrorPayload, "syntax error");
				fg_AppendByte(ErrorPayload, uint8(0));

				auto Error = co_await fg_PostgresDecodeErrorResponse(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('E', ErrorPayload)));

				DMibExpect(Error.m_Severity, ==, "ERROR");
				DMibExpect(Error.m_Code, ==, "42601");
				DMibExpect(Error.m_Message, ==, "syntax error");
			}

			{
				DMibTestPath("Row backend decoding");
				CIOByteVector RowDescriptionPayload;
				fg_AppendInt16(RowDescriptionPayload, 2);
				fg_AppendCString(RowDescriptionPayload, "value");
				fg_AppendInt32(RowDescriptionPayload, 0);
				fg_AppendInt16(RowDescriptionPayload, 0);
				fg_AppendInt32(RowDescriptionPayload, 23);
				fg_AppendInt16(RowDescriptionPayload, 4);
				fg_AppendInt32(RowDescriptionPayload, 0xffffffff);
				fg_AppendInt16(RowDescriptionPayload, 1);
				fg_AppendCString(RowDescriptionPayload, "unknown");
				fg_AppendInt32(RowDescriptionPayload, 0);
				fg_AppendInt16(RowDescriptionPayload, 0);
				fg_AppendInt32(RowDescriptionPayload, 99999);
				fg_AppendInt16(RowDescriptionPayload, 4);
				fg_AppendInt32(RowDescriptionPayload, 0xffffffff);
				fg_AppendInt16(RowDescriptionPayload, 1);

				auto RowDescription = co_await fg_PostgresDecodeRowDescription(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('T', RowDescriptionPayload)));

				DMibExpect(RowDescription.m_Fields.f_GetLen(), ==, umint(2));
				DMibExpect(RowDescription.m_Fields[0].m_Name, ==, "value");
				DMibExpect(RowDescription.m_Fields[0].m_TypeOID, ==, uint32(23));
				DMibExpect(RowDescription.m_Fields[1].m_Name, ==, "unknown");
				DMibExpect(RowDescription.m_Fields[1].m_TypeOID, ==, uint32(99999));

				CIOByteVector DataRowPayload;
				fg_AppendInt16(DataRowPayload, 2);
				fg_AppendInt32(DataRowPayload, 4);
				fg_AppendInt32(DataRowPayload, 42);
				fg_AppendInt32(DataRowPayload, 3);
				fg_AppendByte(DataRowPayload, uint8(1));
				fg_AppendByte(DataRowPayload, uint8(2));
				fg_AppendByte(DataRowPayload, uint8(3));

				auto DataRow = co_await fg_PostgresDecodeDataRow(co_await fg_PostgresReadBackendMessage(fg_BackendMessage('D', DataRowPayload)), RowDescription);

				DMibExpect(DataRow.m_Values.f_GetLen(), ==, umint(2));
				DMibExpect(DataRow.m_Values[0].f_GetTypeID(), ==, EPostgresValueType::mc_Integer32);
				DMibExpect(DataRow.m_Values[0].f_GetAsType<int32>(), ==, int32(42));
				DMibExpect(DataRow.m_Values[1].f_GetTypeID(), ==, EPostgresValueType::mc_Unrecognized);

				auto const &Unrecognized = DataRow.m_Values[1].f_GetAsType<CPostgresUnrecognizedValue>();

				DMibExpect(Unrecognized.m_TypeOID, ==, uint32(99999));
				DMibExpect(fg_BytesToString(Unrecognized.m_Bytes), ==, CStr("\1\2\3", 3));
			}

			{
				DMibTestPath("SCRAM message flow");
				auto ClientFirst = fg_PostgresScramBuildClientFirstMessage("user,name=1", "clientnonce");

				DMibExpect(ClientFirst.m_ClientNonce, ==, "clientnonce");
				DMibExpect(ClientFirst.m_BareMessage, ==, "n=user=2Cname=3D1,r=clientnonce");
				DMibExpect(ClientFirst.m_Message, ==, "n,,n=user=2Cname=3D1,r=clientnonce");

				auto ServerFirst = co_await fg_PostgresScramParseServerFirstMessage("r=clientnonceservernonce,s=c2FsdA==,i=1", ClientFirst.m_ClientNonce);

				DMibExpect(ServerFirst.m_Nonce, ==, "clientnonceservernonce");
				DMibExpect(ServerFirst.m_Salt.f_GetLen(), ==, umint(4));
				DMibExpect(ServerFirst.m_Iterations, ==, uint32(1));

				CStrSecure Password = "password";
				auto ClientFinal = fg_PostgresScramBuildClientFinalMessage(Password, ClientFirst, ServerFirst, {});

				DMibExpect(ClientFinal.m_WithoutProof, ==, "c=biws,r=clientnonceservernonce");
				DMibExpect(ClientFinal.m_Message.f_StartsWith("c=biws,r=clientnonceservernonce,p="), ==, true);
				DMibExpect(ClientFinal.m_ServerSignature.f_GetLen(), ==, umint(32));

				CStr ServerFinalMessage = "v=";
				ServerFinalMessage += NEncoding::fg_Base64Encode(ClientFinal.m_ServerSignature);
				auto ServerFinal = co_await fg_PostgresScramParseServerFinalMessage(ServerFinalMessage);

				DMibExpect(fg_PostgresScramVerifyServerFinalMessage(ClientFinal, ServerFinal), ==, true);

				ServerFinal.m_ServerSignature[0] ^= 0x01;
				DMibExpect(fg_PostgresScramVerifyServerFinalMessage(ClientFinal, ServerFinal), ==, false);
			}
			{
				DMibTestPath("SCRAM channel binding message flow");
				auto ClientFirst = fg_PostgresScramBuildClientFirstMessage("user", "clientnonce", "p=tls-server-end-point,,");

				DMibExpect(ClientFirst.m_GS2Header, ==, "p=tls-server-end-point,,");
				DMibExpect(ClientFirst.m_BareMessage, ==, "n=user,r=clientnonce");
				DMibExpect(ClientFirst.m_Message, ==, "p=tls-server-end-point,,n=user,r=clientnonce");

				auto ServerFirst = co_await fg_PostgresScramParseServerFirstMessage("r=clientnonceservernonce,s=c2FsdA==,i=1", ClientFirst.m_ClientNonce);

				CPostgresWriteStream ChannelBinding;
				ChannelBinding.f_FeedBytes(ClientFirst.m_GS2Header.f_GetStr(), ClientFirst.m_GS2Header.f_GetLen());
				ChannelBinding.f_FeedBytes("abc", 3);
				CStr ExpectedWithoutProof = "c=";
				ExpectedWithoutProof += NEncoding::fg_Base64Encode(ChannelBinding.f_MoveVector());
				ExpectedWithoutProof += ",r=clientnonceservernonce";

				CStrSecure Password = "password";
				auto ClientFinal = fg_PostgresScramBuildClientFinalMessage(Password, ClientFirst, ServerFirst, CByteVector((uint8 const *)"abc", 3));

				DMibExpect(ClientFinal.m_WithoutProof, ==, ExpectedWithoutProof);
				DMibExpect(ClientFinal.m_Message.f_StartsWith(ExpectedWithoutProof), ==, true);
				DMibExpect(ClientFinal.m_ServerSignature.f_GetLen(), ==, umint(32));
			}

			co_return {};
		};

		DMibTestSuite("Postgres client actor integration no TLS") -> NConcurrency::TCFuture<void>
		{
			co_await fg_RunPostgresIntegrationSuite
				(
					{.m_TestPath = "connects without TLS", .m_DirectoryName = "PostgresClient_NoTLS", .m_Port = 24010, .m_bTLS = false}
				)
			;

			co_return {};
		};

		DMibTestSuite("Postgres client actor integration TLS") -> NConcurrency::TCFuture<void>
		{
			co_await fg_RunPostgresIntegrationSuite
				(
					{.m_TestPath = "connects with TLS", .m_DirectoryName = "PostgresClient_TLS", .m_Port = 24011, .m_bTLS = true}
				)
			;

			co_return {};
		};

		DMibTestSuite("Postgres client actor integration TLS client certificate") -> NConcurrency::TCFuture<void>
		{
			co_await fg_RunPostgresIntegrationSuite
				(
					{
						.m_TestPath = "connects with TLS client certificate"
						, .m_DirectoryName = "PostgresClient_TLSClientCertificate"
						, .m_Port = 24012
						, .m_bTLS = true
						, .m_bClientCertificate = true
					}
				)
			;

			co_return {};
		};
	}
};

DMibTestRegister(CPostgresClient_Tests, Malterlib::SQL);
