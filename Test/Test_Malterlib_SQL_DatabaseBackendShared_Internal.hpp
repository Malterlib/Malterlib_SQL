// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NTest::NDatabaseBackend
{
	template <typename tf_CRow>
	auto fg_VectorAsBatchGenerator(NContainer::TCVector<tf_CRow> _Rows, umint _nRowsPerBatch) -> NConcurrency::TCAsyncGenerator<NContainer::TCVector<tf_CRow>>
	{
		if (_nRowsPerBatch == 0)
			_nRowsPerBatch = _Rows.f_GetLen() != 0 ? _Rows.f_GetLen() : 1;

		for (umint iBatchStart = 0; iBatchStart < _Rows.f_GetLen(); iBatchStart += _nRowsPerBatch)
		{
			umint nEnd = iBatchStart + _nRowsPerBatch;
			if (nEnd > _Rows.f_GetLen())
				nEnd = _Rows.f_GetLen();

			NContainer::TCVector<tf_CRow> Batch;
			Batch.f_Reserve(nEnd - iBatchStart);
			for (umint iRow = iBatchStart; iRow < nEnd; ++iRow)
				Batch.f_InsertLast(fg_Move(_Rows[iRow]));

			co_yield fg_Move(Batch);
		}

		co_return {};
	}

	// Streams the rows as one-row batches and then fails, mimicking a parameter generator that throws partway through a
	// bulk insert (for example a later row failing value conversion) after Bind/Execute messages have already been sent.
	template <typename tf_CRow>
	auto fg_FailingBatchGenerator(NContainer::TCVector<tf_CRow> _Rows) -> NConcurrency::TCAsyncGenerator<NContainer::TCVector<tf_CRow>>
	{
		for (umint iRow = 0; iRow < _Rows.f_GetLen(); ++iRow)
		{
			NContainer::TCVector<tf_CRow> Batch;
			Batch.f_InsertLast(fg_Move(_Rows[iRow]));

			co_yield fg_Move(Batch);
		}

		co_return DMibErrorInstance("Injected bulk parameter generator failure");
	}

	template <typename tf_CResult>
	void fg_TestExpectSqlError
		(
			NConcurrency::TCAsyncResult<tf_CResult> const &_Result
			, NStr::CStr const &_TestPath
			, ESqlErrorCategory _Category
			, ESqlErrorRetryClass _RetryClass
			, NStr::CStr const &_Backend
			, NStr::CStr const &_BackendCode
		)
	{
		DMibTestPath(_TestPath);

		DMibExpect(bool(_Result), ==, false);

		auto Error = fg_TryGetSqlErrorData(_Result.f_ExceptionPointer());
		DMibExpect(bool(Error), ==, true);
		if (!Error)
			return;

		DMibExpect(Error->m_Category, ==, _Category);
		DMibExpect(Error->m_RetryClass, ==, _RetryClass);
		DMibExpect(fg_SqlErrorIsTransient(*Error), ==, _RetryClass != ESqlErrorRetryClass::mc_Permanent);

		if (!_Backend.f_IsEmpty())
			DMibExpect(Error->m_Backend, ==, _Backend);

		if (!_BackendCode.f_IsEmpty())
			DMibExpect(Error->m_BackendCode, ==, _BackendCode);
	}

	template <auto &tf_PreparedSelect, typename tf_CDatabase, typename tf_CParam>
	auto fg_TestSqlQuerySingle(tf_CDatabase *_pDatabase, NStr::CStr _TestPath, tf_CParam _Param, CSqlSelectSettings _Settings = {})
		-> NConcurrency::TCFuture<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		CRow Result;
		umint nRows = 0;
		auto Rows = _pDatabase->template f_Query<tf_PreparedSelect>(fg_Move(_Param), _Settings);
		for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
		{
			for (auto const &pRow : *iBatch)
			{
				Result = pRow->m_Data;
				++nRows;
			}
		}

		{
			DMibTestPath(_TestPath);
			DMibExpect(nRows, ==, umint(1));
		}

		co_return Result;
	}

	template <auto &tf_PreparedSelect, typename tf_CDatabase, typename ...tfp_CParams>
	auto fg_TestSqlQuerySingle(tf_CDatabase *_pDatabase, NStr::CStr _TestPath, tfp_CParams ...p_Params)
		-> NConcurrency::TCFuture<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
		requires (sizeof...(tfp_CParams) > 1)
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		CRow Result;
		umint nRows = 0;
		auto Rows = _pDatabase->template f_Query<tf_PreparedSelect>(fg_Move(p_Params)...);
		for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
		{
			for (auto const &pRow : *iBatch)
			{
				Result = pRow->m_Data;
				++nRows;
			}
		}

		{
			DMibTestPath(_TestPath);
			DMibExpect(nRows, ==, umint(1));
		}

		co_return Result;
	}

	template <auto &tf_PreparedSelect, typename tf_CDatabase>
	auto fg_TestSqlQuerySingle(tf_CDatabase *_pDatabase, NStr::CStr _TestPath) -> NConcurrency::TCFuture<typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow>
	{
		using CRow = typename NTraits::TCDecay<decltype(tf_PreparedSelect)>::CRow;

		CRow Result;
		umint nRows = 0;
		auto Rows = _pDatabase->template f_Query<tf_PreparedSelect>();
		for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
		{
			for (auto const &pRow : *iBatch)
			{
				Result = pRow->m_Data;
				++nRows;
			}
		}

		{
			DMibTestPath(_TestPath);
			DMibExpect(nRows, ==, umint(1));
		}

		co_return Result;
	}

	template <typename tf_FOperation>
	NConcurrency::TCFuture<void> fg_TestSqlRunExpectedFailure(tf_FOperation _fOperation)
	{
		auto CaptureScope = co_await NConcurrency::g_CaptureExceptions;
		co_await _fOperation();

		co_return {};
	}

	template <auto &tf_PreparedSelect, typename tf_CDatabase, typename tf_CParam>
	NConcurrency::TCFuture<void> fg_TestSqlQueryAll(tf_CDatabase *_pDatabase, tf_CParam _Param)
	{
		auto Rows = _pDatabase->template f_Query<tf_PreparedSelect>(fg_Move(_Param));
		for (auto iBatch = co_await fg_Move(Rows).f_GetSimpleIterator(); iBatch; co_await ++iBatch)
		{
			for (auto const &pRow : *iBatch)
				(void)pRow;
		}

		co_return {};
	}
}
