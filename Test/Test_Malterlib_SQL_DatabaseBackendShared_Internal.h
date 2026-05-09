// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include "Test_Malterlib_SQL_DatabaseBackendShared.h"

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	using CTestDatabaseClient = CSqlDatabaseClient;

	constexpr fp64 gc_StreamHandoffTimeout = 30.0; // Bounds the waits on an abandoned stream's handoff, which a regression turns into a hang

	template <typename tf_CRow>
	auto fg_VectorAsBatchGenerator(NContainer::TCVector<tf_CRow> _Rows, umint _nRowsPerBatch = 64) -> NConcurrency::TCAsyncGenerator<NContainer::TCVector<tf_CRow>>;

	template <typename tf_CRow>
	auto fg_FailingBatchGenerator(NContainer::TCVector<tf_CRow> _Rows) -> NConcurrency::TCAsyncGenerator<NContainer::TCVector<tf_CRow>>;

	NContainer::CIOByteVector fg_TestSqlByteVector();
	NTime::CTime fg_TestSqlTime();
	CValueTypesRow fg_TestValueTypesRow(NStr::CStr &&_Key);
	void fg_TestExpectByteVector(NContainer::CIOByteVector const &_Actual, NContainer::CIOByteVector const &_Expected);
	template <typename tf_CResult>
	void fg_TestExpectSqlError
		(
			NConcurrency::TCAsyncResult<tf_CResult> const &_Result
			, NStr::CStr const &_TestPath
			, ESqlErrorCategory _Category
			, ESqlErrorRetryClass _RetryClass = ESqlErrorRetryClass::mc_Permanent
			, NStr::CStr const &_Backend = {}
			, NStr::CStr const &_BackendCode = {}
		)
	;

	void fg_TestSqlDatabaseStorageAndMappings();
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseInsertAndSelect(CTestDatabaseClient _Database);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseErrorModel(CTestDatabaseClient _Database);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTransactionInsertAndSelect(CTestDatabaseClient _Database, CSqlDatabaseBackendCapabilities _Capabilities);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseSave(CTestDatabaseClient _Database);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseMutationsAndUpsert(CTestDatabaseClient _Database);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseJoins(CTestDatabaseClient _Database);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseTableAndPreparedInserts(CTestDatabaseClient _Database);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseValueRoundTrips(CTestDatabaseClient _Database, CSqlDatabaseBackendCapabilities _Capabilities);
	NConcurrency::TCFuture<void> fg_TestSqlDatabaseRaw(CTestDatabaseClient _Database, CSqlDatabaseBackendCapabilities _Capabilities);
}

#include "Test_Malterlib_SQL_DatabaseBackendShared_Internal.hpp"
