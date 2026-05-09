// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include "Test_Malterlib_SQL_DatabaseBackendShared.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	using CTestDatabaseClient = CSqlDatabaseClient;

	template <typename t_CRow>
	auto fg_VectorAsBatchGenerator(NContainer::TCVector<t_CRow> _Rows, umint _nRowsPerBatch = 64)
		-> NConcurrency::TCAsyncGenerator<NContainer::TCVector<t_CRow>>
	{
		if (_nRowsPerBatch == 0)
			_nRowsPerBatch = _Rows.f_GetLen() != 0 ? _Rows.f_GetLen() : 1;

		for (umint i = 0; i < _Rows.f_GetLen(); i += _nRowsPerBatch)
		{
			umint nEnd = i + _nRowsPerBatch;
			if (nEnd > _Rows.f_GetLen())
				nEnd = _Rows.f_GetLen();

			NContainer::TCVector<t_CRow> Batch;
			Batch.f_Reserve(nEnd - i);
			for (umint j = i; j < nEnd; ++j)
				Batch.f_InsertLast(fg_Move(_Rows[j]));

			co_yield fg_Move(Batch);
		}

		co_return {};
	}

	// Streams the rows as one-row batches and then fails, mimicking a parameter generator that throws partway through a
	// bulk insert (for example a later row failing value conversion) after Bind/Execute messages have already been sent.
	template <typename t_CRow>
	auto fg_FailingBatchGenerator(NContainer::TCVector<t_CRow> _Rows)
		-> NConcurrency::TCAsyncGenerator<NContainer::TCVector<t_CRow>>
	{
		for (umint i = 0; i < _Rows.f_GetLen(); ++i)
		{
			NContainer::TCVector<t_CRow> Batch;
			Batch.f_InsertLast(fg_Move(_Rows[i]));

			co_yield fg_Move(Batch);
		}

		co_return DMibErrorInstance("Injected bulk parameter generator failure");
	}

	NContainer::CIOByteVector fg_TestSqlByteVector();
	NTime::CTime fg_TestSqlTime();
	CValueTypesRow fg_TestValueTypesRow(NStr::CStr _Key);
	void fg_TestExpectByteVector(NContainer::CIOByteVector const &_Actual, NContainer::CIOByteVector const &_Expected);
	template <typename t_CResult>
	void fg_TestExpectSqlError
		(
			NConcurrency::TCAsyncResult<t_CResult> const &_Result
			, NStr::CStr const &_TestPath
			, ESqlErrorCategory _Category
			, ESqlErrorRetryClass _RetryClass = ESqlErrorRetryClass::mc_Permanent
			, NStr::CStr const &_Backend = {}
			, NStr::CStr const &_BackendCode = {}
		)
	;

	void fg_TestSqlDatabaseStorageAndMappings();
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseInsertAndSelect(CTestDatabaseClient *_pDatabase);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseErrorModel(CTestDatabaseClient *_pDatabase);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTransactionInsertAndSelect(CTestDatabaseClient *_pDatabase, CSqlDatabaseBackendCapabilities _Capabilities);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseSave(CTestDatabaseClient *_pDatabase);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseMutationsAndUpsert(CTestDatabaseClient *_pDatabase);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseJoins(CTestDatabaseClient *_pDatabase);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTableAndPreparedInserts(CTestDatabaseClient *_pDatabase);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseValueRoundTrips(CTestDatabaseClient *_pDatabase, CSqlDatabaseBackendCapabilities _Capabilities);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRaw(CTestDatabaseClient *_pDatabase, CSqlDatabaseBackendCapabilities _Capabilities);
}

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.hpp"
