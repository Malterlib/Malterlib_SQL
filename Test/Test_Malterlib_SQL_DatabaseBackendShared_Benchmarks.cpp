// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.h"

#include <Mib/Concurrency/ConcurrencyManager>
#include <Mib/Test/Performance>

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	using namespace NMib::NStr;

	namespace
	{
		constexpr umint gc_nBenchmarkRepetitions = 5;
		constexpr fp64 gc_BenchmarkTolerance = 0.10;

		NContainer::TCVector<NMib::NTest::CTestPerformanceMeasure> fg_CreateMeasures(NContainer::TCVector<CSqlBenchmarkBackend> const &_Backends)
		{
			NContainer::TCVector<NMib::NTest::CTestPerformanceMeasure> Measures;
			Measures.f_Reserve(_Backends.f_GetLen());
			for (auto const &Backend : _Backends)
				Measures.f_InsertLast(NMib::NTest::CTestPerformanceMeasure(Backend.m_Name));

			return Measures;
		}

		void fg_RegisterMeasures(NMib::NTest::CTestPerformance &_PerfTest, NContainer::TCVector<NMib::NTest::CTestPerformanceMeasure> &_Measures)
		{
			for (auto &Measure : _Measures)
				_PerfTest.f_Add(Measure);
		}
	}

	NConcurrency::TCFuture<void> fg_TestSqlDatabaseBenchmarks(NContainer::TCVector<CSqlBenchmarkBackend> _Backends, umint _nRows)
	{
		DMibTestCategory("Bulk insert") -> NConcurrency::TCFuture<void>
		{
			NMib::NTest::CTestPerformance PerfTest(gc_BenchmarkTolerance);
			auto Measures = fg_CreateMeasures(_Backends);

			for (umint iBackend = 0; iBackend < _Backends.f_GetLen(); ++iBackend)
			{
				auto &Database = *_Backends[iBackend].m_pDatabase;
				auto &Measure = Measures[iBackend];

				for (umint iRep = 0; iRep < gc_nBenchmarkRepetitions; ++iRep)
				{
					NContainer::TCVector<CProfileRow> Rows;
					Rows.f_Reserve(_nRows);
					for (umint i = 0; i < _nRows; ++i)
					{
						CProfileRow Row;
						Row.m_Email = NStr::CStr::CFormat("benchmark-bulk-{}-{}@example.com") << iRep << i;
						Row.m_DisplayName = NStr::CStr::CFormat("Benchmark Bulk {} {}") << iRep << i;
						Rows.f_InsertLast(fg_Move(Row));
					}

					Measure.f_Start();
					co_await Database.template f_InsertMany<gc_InsertProfile>(fg_VectorAsBatchGenerator(fg_Move(Rows)));
					Measure.f_Stop(_nRows);
				}

				DMibTestPath(_Backends[iBackend].m_Name);
				umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("benchmark-bulk-%"));
				DMibExpect(nCount, ==, _nRows * gc_nBenchmarkRepetitions);
			}

			fg_RegisterMeasures(PerfTest, Measures);
			DMibExpectTrue(PerfTest);

			co_return {};
		};

		DMibTestCategory("Per-row insert in transaction") -> NConcurrency::TCFuture<void>
		{
			NMib::NTest::CTestPerformance PerfTest(gc_BenchmarkTolerance);
			auto Measures = fg_CreateMeasures(_Backends);

			for (umint iBackend = 0; iBackend < _Backends.f_GetLen(); ++iBackend)
			{
				auto &Database = *_Backends[iBackend].m_pDatabase;
				auto &Measure = Measures[iBackend];

				for (umint iRep = 0; iRep < gc_nBenchmarkRepetitions; ++iRep)
				{
					Measure.f_Start();
					co_await Database.f_WithTransaction
						(
							[_nRows, iRep](CSqlTransaction Transaction) mutable -> NConcurrency::TCFuture<void>
							{
								for (umint i = 0; i < _nRows; ++i)
								{
									CProfileRow Row;
									Row.m_Email = NStr::CStr::CFormat("benchmark-row-{}-{}@example.com") << iRep << i;
									Row.m_DisplayName = NStr::CStr::CFormat("Benchmark Row {} {}") << iRep << i;
									co_await Transaction.template f_Insert<gc_InsertProfile>(fg_Move(Row));
								}

								co_return {};
							}
						)
					;
					Measure.f_Stop(_nRows);
				}

				DMibTestPath(_Backends[iBackend].m_Name);
				umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("benchmark-row-%"));
				DMibExpect(nCount, ==, _nRows * gc_nBenchmarkRepetitions);
			}

			fg_RegisterMeasures(PerfTest, Measures);
			DMibExpectTrue(PerfTest);

			co_return {};
		};

		DMibTestCategory("Prepared select reuse") -> NConcurrency::TCFuture<void>
		{
			NMib::NTest::CTestPerformance PerfTest(gc_BenchmarkTolerance);
			auto Measures = fg_CreateMeasures(_Backends);

			for (umint iBackend = 0; iBackend < _Backends.f_GetLen(); ++iBackend)
			{
				auto &Database = *_Backends[iBackend].m_pDatabase;
				auto &Measure = Measures[iBackend];

				umint nTotalFound = 0;
				for (umint iRep = 0; iRep < gc_nBenchmarkRepetitions; ++iRep)
				{
					Measure.f_Start();
					for (umint i = 0; i < _nRows; ++i)
					{
						NStr::CStr Email = NStr::CStr::CFormat("benchmark-bulk-0-{}@example.com") << i;
						auto Result = co_await Database.template f_QueryOptional<gc_SelectProfileByEmail>(fg_Move(Email));
						if (Result)
							++nTotalFound;
					}
					Measure.f_Stop(_nRows);
				}

				DMibTestPath(_Backends[iBackend].m_Name);
				DMibExpect(nTotalFound, ==, _nRows * gc_nBenchmarkRepetitions);
			}

			fg_RegisterMeasures(PerfTest, Measures);
			DMibExpectTrue(PerfTest);

			co_return {};
		};

		DMibTestCategory("Streaming query") -> NConcurrency::TCFuture<void>
		{
			NMib::NTest::CTestPerformance PerfTest(gc_BenchmarkTolerance);
			auto Measures = fg_CreateMeasures(_Backends);

			umint const nExpected = _nRows * gc_nBenchmarkRepetitions;
			for (umint iBackend = 0; iBackend < _Backends.f_GetLen(); ++iBackend)
			{
				auto &Database = *_Backends[iBackend].m_pDatabase;
				auto &Measure = Measures[iBackend];

				umint nStreamedTotal = 0;
				for (umint iRep = 0; iRep < gc_nBenchmarkRepetitions * 10; ++iRep)
				{
					Measure.f_Start();
					umint nStreamed = 0;
					auto Rows = Database.template f_Query<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("benchmark-bulk-%"));
					for (auto iBatch = co_await fg_Move(Rows).f_GetPipelinedIterator(); iBatch; co_await ++iBatch)
						nStreamed += (*iBatch).f_GetLen();

					Measure.f_Stop(nStreamed);
					nStreamedTotal += nStreamed;
				}
				DMibTestPath(_Backends[iBackend].m_Name);
				DMibExpect(nStreamedTotal, ==, nExpected * gc_nBenchmarkRepetitions * 10);
			}

			fg_RegisterMeasures(PerfTest, Measures);
			DMibExpectTrue(PerfTest);

			co_return {};
		};

		DMibTestCategory("Streaming query parallel") -> NConcurrency::TCFuture<void>
		{
			NMib::NTest::CTestPerformance PerfTest(gc_BenchmarkTolerance);
			auto Measures = fg_CreateMeasures(_Backends);

			umint const nConcurrency = NConcurrency::fg_ConcurrencyManager().f_GetConcurrency();
			umint const nExpectedPerWorker = _nRows * gc_nBenchmarkRepetitions;
			for (umint iBackend = 0; iBackend < _Backends.f_GetLen(); ++iBackend)
			{
				auto &Database = *_Backends[iBackend].m_pDatabase;
				auto &Measure = Measures[iBackend];

				umint nStreamedTotal = 0;
				for (umint iRep = 0; iRep < gc_nBenchmarkRepetitions * 10; ++iRep)
				{
					Measure.f_Start();

					NConcurrency::TCFutureVector<umint> Results;
					for (umint iWorker = 0; iWorker < nConcurrency; ++iWorker)
					{
						NConcurrency::g_Dispatch(NConcurrency::fg_OtherConcurrentActor()) / [&Database]() -> NConcurrency::TCFuture<umint>
							{
								umint nStreamed = 0;
								auto Rows = Database.template f_Query<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("benchmark-bulk-%"));
								for (auto iBatch = co_await fg_Move(Rows).f_GetPipelinedIterator(); iBatch; co_await ++iBatch)
									nStreamed += (*iBatch).f_GetLen();

								co_return nStreamed;
							}
							> Results
						;
					}

					auto WorkerResults = co_await fg_AllDone(Results);

					umint nStreamedThisRep = 0;
					for (umint nStreamed : WorkerResults)
						nStreamedThisRep += nStreamed;

					Measure.f_Stop(nStreamedThisRep, uint32(nConcurrency));
					nStreamedTotal += nStreamedThisRep;
				}
				DMibTestPath(_Backends[iBackend].m_Name);
				DMibExpect(nStreamedTotal, ==, nExpectedPerWorker * gc_nBenchmarkRepetitions * 10 * nConcurrency);
			}

			fg_RegisterMeasures(PerfTest, Measures);
			DMibExpectTrue(PerfTest);

			co_return {};
		};

		DMibTestCategory("Transaction overhead") -> NConcurrency::TCFuture<void>
		{
			umint nTransactionsPerRep = _nRows / 10;
			if (nTransactionsPerRep == 0)
				nTransactionsPerRep = 1;

			NMib::NTest::CTestPerformance PerfTest(gc_BenchmarkTolerance);
			auto Measures = fg_CreateMeasures(_Backends);

			for (umint iBackend = 0; iBackend < _Backends.f_GetLen(); ++iBackend)
			{
				auto &Database = *_Backends[iBackend].m_pDatabase;
				auto &Measure = Measures[iBackend];

				for (umint iRep = 0; iRep < gc_nBenchmarkRepetitions; ++iRep)
				{
					Measure.f_Start();
					for (umint i = 0; i < nTransactionsPerRep; ++i)
					{
						co_await Database.f_WithTransaction
							(
								[iRep, i](CSqlTransaction Transaction) mutable -> NConcurrency::TCFuture<void>
								{
									CProfileRow Row;
									Row.m_Email = NStr::CStr::CFormat("benchmark-tx-{}-{}@example.com") << iRep << i;
									Row.m_DisplayName = NStr::CStr::CFormat("Benchmark Tx {} {}") << iRep << i;
									co_await Transaction.template f_Insert<gc_InsertProfile>(fg_Move(Row));

									co_return {};
								}
							)
						;
					}
					Measure.f_Stop(nTransactionsPerRep);
				}

				DMibTestPath(_Backends[iBackend].m_Name);
				umint nCount = co_await Database.template f_Count<gc_SelectProfileByEmailLikeOrderEmailDisplay>(NStr::CStr("benchmark-tx-%"));
				DMibExpect(nCount, ==, nTransactionsPerRep * gc_nBenchmarkRepetitions);
			}

			fg_RegisterMeasures(PerfTest, Measures);
			DMibExpectTrue(PerfTest);

			co_return {};
		};

		co_return {};
	}
}
