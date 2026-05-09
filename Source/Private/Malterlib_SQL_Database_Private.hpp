// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NPrivate
{
	inline NException::CExceptionPointer fg_SqlConversionError(NStr::CStr const &_ColumnName, NStr::CStr const &_Message)
	{
		return DMibErrorDatabaseInstance(NStr::CStr::CFormat("SQL column '{}' conversion failed: {}") << _ColumnName << _Message);
	}

	template <typename tf_CValue>
	CSqlValue fg_SqlPrimitiveValue(tf_CValue &&_Value)
	{
		using CValue = NTraits::TCRemoveReferenceAndQualifiers<tf_CValue>;

		if constexpr (NTraits::cIsSame<CValue, bool>)
			return bool(_Value);
		else if constexpr (NTraits::cIsSame<CValue, int8>)
			return int8(_Value);
		else if constexpr (NTraits::cIsSame<CValue, int16>)
			return int16(_Value);
		else if constexpr (NTraits::cIsSame<CValue, int32>)
			return int32(_Value);
		else if constexpr (NTraits::cIsSame<CValue, int64>)
			return int64(_Value);
		else if constexpr (NTraits::cIsSame<CValue, uint8>)
			return uint8(_Value);
		else if constexpr (NTraits::cIsSame<CValue, uint16>)
			return uint16(_Value);
		else if constexpr (NTraits::cIsSame<CValue, uint32>)
			return uint32(_Value);
		else if constexpr (NTraits::cIsSame<CValue, uint64>)
			return uint64(_Value);
		else if constexpr (NTraits::cIsSame<CValue, fp32>)
			return fp32(_Value);
		else if constexpr (NTraits::cIsSame<CValue, fp64>)
			return fp64(_Value);
		else if constexpr (NTraits::cIsSame<CValue, NStr::CStr>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, NContainer::CIOByteVector>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, NTime::CTime>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, NCryptography::CUniversallyUniqueIdentifier>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, CSqlDate>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, CSqlTimeOfDay>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, CSqlTimestamp>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, CSqlTimestampTz>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, CSqlInterval>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, NEncoding::CJsonOrdered>)
			return fg_Forward<tf_CValue>(_Value);
		else if constexpr (NTraits::cIsSame<CValue, NEncoding::CJsonSorted>)
			return fg_Forward<tf_CValue>(_Value);
		else
		{
			static_assert(TCSqlTypeTraits<CValue>::mc_bSupported, "Unsupported SQL value storage type");

			return fg_Forward<tf_CValue>(_Value);
		}
	}

	template <typename t_CStoredValue, typename tf_CValue>
	CSqlValue fg_SqlValueForStoredType(tf_CValue &&_Value)
	{
		using CStoredValue = NTraits::TCRemoveReferenceAndQualifiers<t_CStoredValue>;
		using CValue = NTraits::TCRemoveReferenceAndQualifiers<tf_CValue>;
		using CTraits = TCSqlTypeTraits<CStoredValue>;
		using CSqlType = typename CTraits::CSqlType;

		static_assert(CTraits::mc_bSupported, "Unsupported SQL value type");

		if constexpr (requires { CTraits::fs_ToSqlStorage(fg_Forward<tf_CValue>(_Value)); })
			return fg_SqlPrimitiveValue(CTraits::fs_ToSqlStorage(fg_Forward<tf_CValue>(_Value)));
		else if constexpr (NTraits::cIsSame<CValue, CStoredValue> && (NTraits::cIsRValueReference<tf_CValue> || !NTraits::cIsReference<tf_CValue>))
			return fg_SqlPrimitiveValue(fg_Forward<tf_CValue>(_Value));
		else if constexpr (NTraits::cIsConstructibleWith<CStoredValue, tf_CValue &&>)
			return fg_SqlValueForStoredType<CStoredValue>(CStoredValue(fg_Forward<tf_CValue>(_Value)));
		else if constexpr (NTraits::cIsConstructibleWith<CSqlType, tf_CValue &&>)
			return fg_SqlPrimitiveValue(CSqlType(fg_Forward<tf_CValue>(_Value)));
		else
			static_assert(CTraits::mc_bSupported, "SQL value cannot be converted to the column storage type");
	}

	template <typename t_CColumnValue, typename tf_CValue>
	CSqlValue fg_SqlCoercedValue(tf_CValue &&_Value)
	{
		using CColumnValue = NTraits::TCRemoveReferenceAndQualifiers<t_CColumnValue>;
		using CValue = NTraits::TCRemoveReferenceAndQualifiers<tf_CValue>;

		if constexpr (NStorage::cIsOptional<CColumnValue>)
		{
			using CStoredValue = NStorage::TCOptionalType<CColumnValue>;
			if constexpr (NStorage::cIsOptional<CValue>)
			{
				if (!_Value)
					return {};

				if constexpr (NTraits::cIsRValueReference<tf_CValue> || !NTraits::cIsReference<tf_CValue>)
					return fg_SqlValueForStoredType<CStoredValue>(fg_Move(*_Value));
				else
					return fg_SqlValueForStoredType<CStoredValue>(*_Value);
			}
			else
				return fg_SqlValueForStoredType<CStoredValue>(fg_Forward<tf_CValue>(_Value));
		}
		else
			return fg_SqlValueForStoredType<CColumnValue>(fg_Forward<tf_CValue>(_Value));
	}

	template <typename tf_CValue>
	CSqlValue fg_SqlValue(tf_CValue &&_Value)
	{
		return fg_SqlCoercedValue<NTraits::TCRemoveReferenceAndQualifiers<tf_CValue>>(fg_Forward<tf_CValue>(_Value));
	}

	template <typename t_CType>
	struct TCSqlValueType
	{
		static constexpr ESqlValueType mc_Type = TCSqlTypeTraits<NStorage::TCOptionalType<NTraits::TCRemoveReferenceAndQualifiers<t_CType>>>::mc_ValueType;
	};

	inline NConcurrency::TCWrapped<int64> fg_SqlSignedIntegerFromValue(CSqlValue const &_Value, NStr::CStr const &_ColumnName)
	{
		switch (_Value.f_GetTypeID())
		{
		case ESqlValueType::mc_Integer8:
			return int64(_Value.f_GetAsType<int8>());
		case ESqlValueType::mc_Integer16:
			return int64(_Value.f_GetAsType<int16>());
		case ESqlValueType::mc_Integer32:
			return int64(_Value.f_GetAsType<int32>());
		case ESqlValueType::mc_Integer64:
			return _Value.f_GetAsType<int64>();
		case ESqlValueType::mc_UnsignedInteger8:
			return int64(_Value.f_GetAsType<uint8>());
		case ESqlValueType::mc_UnsignedInteger16:
			return int64(_Value.f_GetAsType<uint16>());
		case ESqlValueType::mc_UnsignedInteger32:
			return int64(_Value.f_GetAsType<uint32>());
		case ESqlValueType::mc_UnsignedInteger64:
			{
				uint64 Value = _Value.f_GetAsType<uint64>();
				if (Value > uint64(TCLimitsInt<int64>::mc_Max))
					return fg_SqlConversionError(_ColumnName, "unsigned integer exceeds signed 64-bit database range");

				return int64(Value);
			}
		default:
			return fg_SqlConversionError(_ColumnName, "expected integer value");
		}
	}

	inline NConcurrency::TCWrapped<uint64> fg_SqlUnsignedIntegerFromValue(CSqlValue const &_Value, NStr::CStr const &_ColumnName)
	{
		switch (_Value.f_GetTypeID())
		{
		case ESqlValueType::mc_Integer8:
			{
				int8 Value = _Value.f_GetAsType<int8>();
				if (Value < 0)
					return fg_SqlConversionError(_ColumnName, "negative integer cannot be assigned to unsigned field");

				return uint64(uint8(Value));
			}
		case ESqlValueType::mc_Integer16:
			{
				int16 Value = _Value.f_GetAsType<int16>();
				if (Value < 0)
					return fg_SqlConversionError(_ColumnName, "negative integer cannot be assigned to unsigned field");

				return uint64(uint16(Value));
			}
		case ESqlValueType::mc_Integer32:
			{
				int32 Value = _Value.f_GetAsType<int32>();
				if (Value < 0)
					return fg_SqlConversionError(_ColumnName, "negative integer cannot be assigned to unsigned field");

				return uint64(uint32(Value));
			}
		case ESqlValueType::mc_Integer64:
			{
				int64 Value = _Value.f_GetAsType<int64>();
				if (Value < 0)
					return fg_SqlConversionError(_ColumnName, "negative integer cannot be assigned to unsigned field");

				return uint64(Value);
			}
		case ESqlValueType::mc_UnsignedInteger8:
			return uint64(_Value.f_GetAsType<uint8>());
		case ESqlValueType::mc_UnsignedInteger16:
			return uint64(_Value.f_GetAsType<uint16>());
		case ESqlValueType::mc_UnsignedInteger32:
			return uint64(_Value.f_GetAsType<uint32>());
		case ESqlValueType::mc_UnsignedInteger64:
			return _Value.f_GetAsType<uint64>();
		default:
			return fg_SqlConversionError(_ColumnName, "expected integer value");
		}
	}

	template <typename t_CInteger>
	constexpr int64 fg_SqlSignedIntegerMin()
	{
		static_assert(NTraits::cIsSigned<t_CInteger>);
		constexpr uint64 c_SignBit = uint64(1) << (sizeof(t_CInteger)*8 - 1);

		return -int64(c_SignBit - 1) - 1;
	}

	template <typename t_CInteger>
	constexpr int64 fg_SqlSignedIntegerMax()
	{
		static_assert(NTraits::cIsSigned<t_CInteger>);
		constexpr uint64 c_SignBit = uint64(1) << (sizeof(t_CInteger)*8 - 1);

		return int64(c_SignBit - 1);
	}

	template <typename t_CInteger>
	constexpr uint64 fg_SqlUnsignedIntegerMax()
	{
		static_assert(!NTraits::cIsSigned<t_CInteger>);
		if constexpr (sizeof(t_CInteger) == sizeof(uint64))
			return uint64(-1);
		else
			return (uint64(1) << (sizeof(t_CInteger)*8)) - 1;
	}

	template <typename t_CInteger>
	NConcurrency::TCWrapped<t_CInteger> fg_SqlIntegerStorageFromValue(CSqlValue const &_Value, NStr::CStr const &_ColumnName)
	{
		if constexpr (NTraits::cIsSigned<t_CInteger>)
		{
			auto WrappedValue = fg_SqlSignedIntegerFromValue(_Value, _ColumnName);
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			int64 Value = *WrappedValue;
			if (Value < fg_SqlSignedIntegerMin<t_CInteger>() || Value > fg_SqlSignedIntegerMax<t_CInteger>())
				return fg_SqlConversionError(_ColumnName, "integer is outside target signed type range");

			return t_CInteger(Value);
		}
		else
		{
			auto WrappedValue = fg_SqlUnsignedIntegerFromValue(_Value, _ColumnName);
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			uint64 Value = *WrappedValue;
			if (Value > fg_SqlUnsignedIntegerMax<t_CInteger>())
				return fg_SqlConversionError(_ColumnName, "integer is outside target unsigned type range");

			return t_CInteger(Value);
		}
	}

	template <typename t_CSqlType>
	NConcurrency::TCWrapped<t_CSqlType> fg_SqlStorageFromValue(CSqlValue &&_Value, NStr::CStr const &_ColumnName)
	{
		if constexpr (NTraits::cIsSame<t_CSqlType, bool>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Boolean)
				return fg_SqlConversionError(_ColumnName, "expected boolean value");

			return _Value.f_GetAsType<bool>();
		}
		else if constexpr (NTraits::cIsInteger<t_CSqlType>)
			return fg_SqlIntegerStorageFromValue<t_CSqlType>(_Value, _ColumnName);
		else if constexpr (NTraits::cIsFloat<t_CSqlType>)
		{
			switch (_Value.f_GetTypeID())
			{
			case ESqlValueType::mc_Float32:
				return t_CSqlType(_Value.f_GetAsType<fp32>());
			case ESqlValueType::mc_Float64:
				{
					fp64 Value = _Value.f_GetAsType<fp64>();
					if constexpr (NTraits::cIsSame<t_CSqlType, fp32>)
					{
						if (!Value.f_IsNan() && !Value.f_IsInfinity() && (Value < fp64(fp32::fs_LimitMin()) || Value > fp64(fp32::fs_LimitMax())))
							return fg_SqlConversionError(_ColumnName, "floating point value is outside target fp32 range");
					}

					return t_CSqlType(Value);
				}
			default:
				return fg_SqlConversionError(_ColumnName, "expected floating point value");
			}
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, NStr::CStr>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Text)
				return fg_SqlConversionError(_ColumnName, "expected text value");

			return fg_Move(_Value.f_GetAsType<NStr::CStr>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, NContainer::CIOByteVector>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Blob)
				return fg_SqlConversionError(_ColumnName, "expected blob value");

			return fg_Move(_Value.f_GetAsType<NContainer::CIOByteVector>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, NTime::CTime>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Time)
				return fg_SqlConversionError(_ColumnName, "expected time value");

			return fg_Move(_Value.f_GetAsType<NTime::CTime>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, NCryptography::CUniversallyUniqueIdentifier>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_UUID)
				return fg_SqlConversionError(_ColumnName, "expected UUID value");

			return fg_Move(_Value.f_GetAsType<NCryptography::CUniversallyUniqueIdentifier>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, CSqlDate>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Date)
				return fg_SqlConversionError(_ColumnName, "expected PostgreSQL date value");

			return fg_Move(_Value.f_GetAsType<CSqlDate>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, CSqlTimeOfDay>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_TimeOfDay)
				return fg_SqlConversionError(_ColumnName, "expected PostgreSQL time value");

			return fg_Move(_Value.f_GetAsType<CSqlTimeOfDay>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, CSqlTimestamp>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Timestamp)
				return fg_SqlConversionError(_ColumnName, "expected PostgreSQL timestamp value");

			return fg_Move(_Value.f_GetAsType<CSqlTimestamp>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, CSqlTimestampTz>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_TimestampTz)
				return fg_SqlConversionError(_ColumnName, "expected PostgreSQL timestamptz value");

			return fg_Move(_Value.f_GetAsType<CSqlTimestampTz>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, CSqlInterval>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Interval)
				return fg_SqlConversionError(_ColumnName, "expected PostgreSQL interval value");

			return fg_Move(_Value.f_GetAsType<CSqlInterval>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, NEncoding::CJsonOrdered>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Json)
				return fg_SqlConversionError(_ColumnName, "expected PostgreSQL JSON value");

			return fg_Move(_Value.f_GetAsType<NEncoding::CJsonOrdered>());
		}
		else if constexpr (NTraits::cIsSame<t_CSqlType, NEncoding::CJsonSorted>)
		{
			if (_Value.f_GetTypeID() != ESqlValueType::mc_Jsonb)
				return fg_SqlConversionError(_ColumnName, "expected PostgreSQL JSONB value");

			return fg_Move(_Value.f_GetAsType<NEncoding::CJsonSorted>());
		}
		else
		{
			using CTraits = TCSqlTypeTraits<t_CSqlType>;
			static_assert(CTraits::mc_bSupported, "Unsupported SQL storage type");
			if (_Value.f_GetTypeID() != CTraits::mc_ValueType)
				return fg_SqlConversionError(_ColumnName, "unexpected SQL value type");

			return fg_Move(_Value.f_GetAsType<t_CSqlType>());
		}
	}

	template <typename t_CValue>
	NConcurrency::TCWrapped<t_CValue> fg_SqlMappedValueFromValue(CSqlValue &&_Value, NStr::CStr const &_ColumnName)
	{
		using CValue = NTraits::TCRemoveReferenceAndQualifiers<t_CValue>;
		using CTraits = TCSqlTypeTraits<CValue>;
		using CSqlType = typename CTraits::CSqlType;

		static_assert(CTraits::mc_bSupported, "Unsupported SQL row field type");

		auto WrappedStorage = fg_SqlStorageFromValue<CSqlType>(fg_Move(_Value), _ColumnName);
		if (!WrappedStorage)
			return fg_Move(WrappedStorage).f_GetException();

		return CTraits::fs_FromSqlStorage(fg_Move(*WrappedStorage));
	}

	template <typename t_CMember>
	NException::CExceptionPointer fg_SqlSetRowFieldValue(void *_pMember, CSqlValue &&_Value, NStr::CStr const &_ColumnName)
	{
		using CMember = NTraits::TCRemoveReferenceAndQualifiers<t_CMember>;

		if constexpr (NStorage::cIsOptional<CMember>)
		{
			auto &Member = *reinterpret_cast<CMember *>(_pMember);
			if (_Value.f_GetTypeID() == ESqlValueType::mc_Null)
			{
				Member.f_Clear();
				return {};
			}

			auto WrappedValue = fg_SqlMappedValueFromValue<NStorage::TCOptionalType<CMember>>(fg_Move(_Value), _ColumnName);
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			Member = fg_Move(*WrappedValue);
			return {};
		}
		else
		{
			if (_Value.f_GetTypeID() == ESqlValueType::mc_Null)
				return fg_SqlConversionError(_ColumnName, "NULL cannot be assigned to non-nullable field");

			auto WrappedValue = fg_SqlMappedValueFromValue<CMember>(fg_Move(_Value), _ColumnName);
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			*reinterpret_cast<CMember *>(_pMember) = fg_Move(*WrappedValue);
			return {};
		}
	}

	constexpr CSqlQueryID fg_SqlMixQueryID(CSqlQueryID _QueryID, ESqlValueType _Type)
	{
		_QueryID.m_Value ^= uint8(_Type);
		_QueryID.m_Value *= 1099511628211ull;

		return _QueryID;
	}

	constexpr CSqlQueryID fg_SqlMixQueryIDs(CSqlQueryID _Left, CSqlQueryID _Right)
	{
		for (umint i = 0; i < sizeof(_Right.m_Value); ++i)
		{
			_Left.m_Value ^= uint8(_Right.m_Value >> (i * 8));
			_Left.m_Value *= 1099511628211ull;
		}

		return _Left;
	}

	template <typename tf_CRow>
	CSqlRowDataPointer fg_SqlCreateRowData()
	{
		return NStorage::TCUniquePointer<ICRowData>(fg_Construct<TCRowData<tf_CRow>>());
	}

	template <typename tf_CRow, typename tf_CMember>
	umint fg_SqlMemberOffset(tf_CMember tf_CRow::*_pMember)
	{
		TCRowData<tf_CRow> Row;

		return umint(reinterpret_cast<uint8 *>(&(Row.m_Data.*_pMember)) - reinterpret_cast<uint8 *>(static_cast<ICRowData *>(&Row)));
	}

	template <typename tf_CProjectionRow, umint tf_iMember>
	umint fg_SqlProjectionMemberOffset()
	{
		TCRowData<tf_CProjectionRow> Row;
		if constexpr (requires { fg_Get<tf_iMember>(Row.m_Data); })
		{
			return umint
				(
					reinterpret_cast<uint8 *>(&fg_Get<tf_iMember>(Row.m_Data))
					- reinterpret_cast<uint8 *>(static_cast<ICRowData *>(&Row))
				)
			;
		}
		else
		{
			static_assert(tf_iMember == 0, "SQL scalar projection row can only map one selected member");

			return umint
				(
					reinterpret_cast<uint8 *>(&Row.m_Data)
					- reinterpret_cast<uint8 *>(static_cast<ICRowData *>(&Row))
				)
			;
		}
	}

	template <typename tf_CJoinedRow, umint tf_iRow, typename tf_CRow, typename tf_CMember>
	umint fg_SqlJoinedRowMemberOffset(tf_CMember tf_CRow::*_pMember)
	{
		TCRowData<tf_CJoinedRow> Row;

		return umint
			(
				reinterpret_cast<uint8 *>(&(fg_Get<tf_iRow>(Row.m_Data).*_pMember))
				- reinterpret_cast<uint8 *>(static_cast<ICRowData *>(&Row))
			)
		;
	}

	template <typename tf_CRow, typename tf_CMember>
	CSqlRowFieldMapping fg_SqlRowFieldMapping(TCSqlColumn<tf_CRow, tf_CMember> const &_Column)
	{
		using CMember = typename TCSqlColumn<tf_CRow, tf_CMember>::CMember;
		using CStoredMember = typename TCSqlColumn<tf_CRow, tf_CMember>::CStoredMember;
		using CTraits = TCSqlTypeTraits<CStoredMember>;

		static_assert(CTraits::mc_bSupported, "Unsupported SQL row field type");

		return
			{
				.m_ColumnName = _Column.f_Name()
				, .m_ValueType = CTraits::mc_ValueType
				, .m_bNullable = _Column.f_IsNullable()
				, .m_Offset = fg_SqlMemberOffset(_Column.m_pMember)
				, .m_fSetValue = &fg_SqlSetRowFieldValue<CMember>
			}
		;
	}

	template <typename tf_CTable>
	CSqlRowMapping fg_SqlRowMapping(tf_CTable const &_Table)
	{
		using CRow = typename tf_CTable::CRow;

		CSqlRowMapping Mapping;
		Mapping.m_fCreateRow = &fg_SqlCreateRowData<CRow>;
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							Mapping.m_Fields.f_InsertLast(fg_SqlRowFieldMapping(p_Columns));
						}
						()
						, ...
					);
				}
			)
		;

		return Mapping;
	}

	template <typename tf_CTable, typename tf_CMember>
	NStr::CStr fg_SqlColumnNameForMember(tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember);

	template <typename tf_CProjectionRow, umint tf_iProjectionMember, typename tf_CTable, typename tf_CMember>
	void fg_SqlAppendProjectionColumn(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember)
	{
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == _pMember; })
							{
								if (p_Columns.m_pMember == _pMember)
								{
									using CProjectionMember = typename TCSqlProjectionMemberType<tf_CProjectionRow, tf_iProjectionMember>::CType;

									auto Field = fg_SqlRowFieldMapping(p_Columns);
									Field.m_Offset = fg_SqlProjectionMemberOffset<tf_CProjectionRow, tf_iProjectionMember>();
									Field.m_fSetValue = &fg_SqlSetRowFieldValue<CProjectionMember>;

									_Description.m_SelectColumns.f_InsertLast(p_Columns.f_Name());
									_Description.m_RowMapping.m_Fields.f_InsertLast(fg_Move(Field));
								}
							}
						}
						()
						, ...
					);
				}
			)
		;
	}

	inline CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Count, nullptr> const &)
	{
		return {.m_Type = ESqlSelectExpressionType::mc_Count};
	}

	template <typename tf_CTable>
	CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(tf_CTable const &, TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Count, nullptr> const &)
	{
		return fg_SqlSelectExpressionDescription(TCSqlAggregateExpression<ESqlSelectExpressionType::mc_Count, nullptr>{});
	}

	template <auto tf_pMember, typename tf_CTable>
	CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(tf_CTable const &_Table, TCSqlColumnExpression<tf_pMember> const &)
	{
		return
			{
				.m_Type = ESqlSelectExpressionType::mc_Column
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
			}
		;
	}

	template <ESqlSelectExpressionType tf_Type, auto tf_pMember, typename tf_CTable>
	CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(tf_CTable const &_Table, TCSqlAggregateExpression<tf_Type, tf_pMember> const &)
	{
		// m_ResultType carries the value type the expression evaluates to so the backend generator can emit a
		// type-aware cast where a backend would otherwise widen the result (SUM over a bigint column becomes
		// NUMERIC in PostgreSQL). For an integer SUM this resolves to mc_Integer64.
		return
			{
				.m_Type = tf_Type
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
				, .m_ResultType = TCSqlValueType<typename TCSqlExpressionResultType<TCSqlAggregateExpression<tf_Type, tf_pMember>>::CType>::mc_Type
			}
		;
	}

	template <ESqlSelectExpressionType tf_Type, auto tf_pLeftMember, auto tf_pRightMember, typename tf_CTable>
	CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(tf_CTable const &_Table, TCSqlBinaryColumnExpression<tf_Type, tf_pLeftMember, tf_pRightMember> const &)
	{
		return
			{
				.m_Type = tf_Type
				, .m_LeftColumnName = fg_SqlColumnNameForMember(_Table, tf_pLeftMember)
				, .m_RightColumnName = fg_SqlColumnNameForMember(_Table, tf_pRightMember)
			}
		;
	}

	template <ESqlSelectExpressionType tf_Type, auto tf_pMember, typename tf_CTable>
	CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(tf_CTable const &_Table, TCSqlUnaryColumnExpression<tf_Type, tf_pMember> const &)
	{
		return
			{
				.m_Type = tf_Type
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
			}
		;
	}

	template <auto &tf_FunctionName, auto tf_pMember, typename tf_CTable>
	CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(tf_CTable const &_Table, TCSqlBackendFunctionExpression<tf_FunctionName, tf_pMember> const &)
	{
		return
			{
				.m_Type = ESqlSelectExpressionType::mc_BackendFunction
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
				, .m_FunctionName = tf_FunctionName.m_Str
			}
		;
	}

	template <auto tf_pResultMember, typename tf_CExpression, typename tf_CTable>
	CSqlSelectExpressionDescription fg_SqlSelectExpressionDescription(tf_CTable const &_Table, TCSqlAliasedExpression<tf_pResultMember, tf_CExpression> const &_Expression)
	{
		return fg_SqlSelectExpressionDescription(_Table, _Expression.m_Expression);
	}

	template <typename tf_CProjectionRow, umint tf_iProjectionMember>
	void fg_SqlAppendAggregateExpression(CSqlPreparedSelectStatementDescription &_Description, CSqlSelectExpressionDescription _ExpressionDescription)
	{
		using CProjectionMember = typename TCSqlProjectionMemberType<tf_CProjectionRow, tf_iProjectionMember>::CType;

		CSqlRowFieldMapping Field;
		Field.m_ColumnName = _ExpressionDescription.m_ColumnName;
		Field.m_ValueType = TCSqlValueType<CProjectionMember>::mc_Type;
		Field.m_bNullable = NStorage::cIsOptional<CProjectionMember>;
		Field.m_Offset = fg_SqlProjectionMemberOffset<tf_CProjectionRow, tf_iProjectionMember>();
		Field.m_fSetValue = &fg_SqlSetRowFieldValue<CProjectionMember>;

		_Description.m_SelectExpressions.f_InsertLast(fg_Move(_ExpressionDescription));
		_Description.m_RowMapping.m_Fields.f_InsertLast(fg_Move(Field));
	}

	template <typename tf_CProjectionRow, umint tf_iProjectionMember, typename tf_CTable, typename tf_CExpression>
	void fg_SqlAppendSelectExpression(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, tf_CExpression const &_Expression)
	{
		fg_SqlAppendAggregateExpression<tf_CProjectionRow, tf_iProjectionMember>(_Description, fg_SqlSelectExpressionDescription(_Table, _Expression));
	}

	template <typename tf_CProjectionRow, umint tf_iProjectionMember, typename tf_CTable, auto tf_pResultMember, typename tf_CExpression>
	auto fg_SqlAppendSelectExpression
		(
			CSqlPreparedSelectStatementDescription &_Description
			, tf_CTable const &_Table
			, TCSqlAliasedExpression<tf_pResultMember, tf_CExpression> const &_Expression
		)
		-> void
	{
		using CProjectionMember = typename TCSqlMemberPointerTraits<tf_pResultMember>::CMember;

		CSqlRowFieldMapping Field;
		Field.m_ValueType = TCSqlValueType<CProjectionMember>::mc_Type;
		Field.m_bNullable = NStorage::cIsOptional<CProjectionMember>;
		Field.m_Offset = fg_SqlMemberOffset(tf_pResultMember);
		Field.m_fSetValue = &fg_SqlSetRowFieldValue<CProjectionMember>;

		_Description.m_SelectExpressions.f_InsertLast(fg_SqlSelectExpressionDescription(_Table, _Expression.m_Expression));
		_Description.m_RowMapping.m_Fields.f_InsertLast(fg_Move(Field));
	}

	template <typename tf_CJoinedRow, umint tf_iTable, typename tf_CTable, typename tf_CMember>
	void fg_SqlAppendJoinedRowColumn(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember)
	{
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == _pMember; })
							{
								if (p_Columns.m_pMember == _pMember)
								{
									auto Field = fg_SqlRowFieldMapping(p_Columns);
									Field.m_Offset = fg_SqlJoinedRowMemberOffset<tf_CJoinedRow, tf_iTable>(_pMember);

									_Description.m_QualifiedSelectColumns.f_InsertLast({.m_iTable = tf_iTable, .m_ColumnName = p_Columns.f_Name()});
									_Description.m_RowMapping.m_Fields.f_InsertLast(fg_Move(Field));
								}
							}
						}
						()
						, ...
					);
				}
			)
		;
	}

	template <typename tf_CJoinedRow, umint tf_iTable, typename tf_CTable>
	void fg_SqlApplyJoinedRowTableColumns(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table)
	{
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						fg_SqlAppendJoinedRowColumn<tf_CJoinedRow, tf_iTable>(_Description, _Table, p_Columns.m_pMember)
						, ...
					);
				}
			)
		;
	}

	template <typename tf_CJoinedRow, typename tf_CLeftTable, typename tf_CRightTable>
	void fg_SqlApplyJoinedRowColumns(CSqlPreparedSelectStatementDescription &_Description, tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable)
	{
		_Description.m_SelectColumns.f_Clear();
		_Description.m_QualifiedSelectColumns.f_Clear();
		_Description.m_SelectExpressions.f_Clear();
		_Description.m_RowMapping.m_Fields.f_Clear();
		_Description.m_RowMapping.m_fCreateRow = &fg_SqlCreateRowData<tf_CJoinedRow>;
		fg_SqlApplyJoinedRowTableColumns<tf_CJoinedRow, 0>(_Description, _LeftTable);
		fg_SqlApplyJoinedRowTableColumns<tf_CJoinedRow, 1>(_Description, _RightTable);
	}

	template <typename tf_CJoinedRow, typename tf_CLeftTable, typename tf_CMiddleTable, typename tf_CRightTable>
	auto fg_SqlApplyJoinedRowColumns
		(
			CSqlPreparedSelectStatementDescription &_Description
			, tf_CLeftTable const &_LeftTable
			, tf_CMiddleTable const &_MiddleTable
			, tf_CRightTable const &_RightTable
		)
		-> void
	{
		_Description.m_SelectColumns.f_Clear();
		_Description.m_QualifiedSelectColumns.f_Clear();
		_Description.m_SelectExpressions.f_Clear();
		_Description.m_RowMapping.m_Fields.f_Clear();
		_Description.m_RowMapping.m_fCreateRow = &fg_SqlCreateRowData<tf_CJoinedRow>;
		fg_SqlApplyJoinedRowTableColumns<tf_CJoinedRow, 0>(_Description, _LeftTable);
		fg_SqlApplyJoinedRowTableColumns<tf_CJoinedRow, 1>(_Description, _MiddleTable);
		fg_SqlApplyJoinedRowTableColumns<tf_CJoinedRow, 2>(_Description, _RightTable);
	}

	template <typename tf_CJoinedRow, typename tf_CTables, umint ...tfp_iTables>
	void fg_SqlApplyJoinedRowColumns(CSqlPreparedSelectStatementDescription &_Description, tf_CTables const &_Tables, std::index_sequence<tfp_iTables...>)
	{
		_Description.m_SelectColumns.f_Clear();
		_Description.m_QualifiedSelectColumns.f_Clear();
		_Description.m_SelectExpressions.f_Clear();
		_Description.m_RowMapping.m_Fields.f_Clear();
		_Description.m_RowMapping.m_fCreateRow = &fg_SqlCreateRowData<tf_CJoinedRow>;
		(
			fg_SqlApplyJoinedRowTableColumns<tf_CJoinedRow, tfp_iTables>(_Description, fg_Get<tfp_iTables>(_Tables))
			, ...
		);
	}

	template <typename tf_CJoinedRow, typename ...tfp_CTables>
	void fg_SqlApplyJoinedRowColumns(CSqlPreparedSelectStatementDescription &_Description, NStorage::TCTuple<tfp_CTables const &...> const &_Tables)
	{
		fg_SqlApplyJoinedRowColumns<tf_CJoinedRow>(_Description, _Tables, std::index_sequence_for<tfp_CTables...>{});
	}

	template <typename tf_CProjectionRow, typename tf_CTable, auto ...tfp_pMembers, umint ...tfp_iMembers>
	void fg_SqlApplyProjectionColumns(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, std::index_sequence<tfp_iMembers...>)
	{
		_Description.m_SelectColumns.f_Clear();
		_Description.m_QualifiedSelectColumns.f_Clear();
		_Description.m_SelectExpressions.f_Clear();
		_Description.m_RowMapping.m_Fields.f_Clear();
		_Description.m_RowMapping.m_fCreateRow = &fg_SqlCreateRowData<tf_CProjectionRow>;
		(
			fg_SqlAppendProjectionColumn<tf_CProjectionRow, tfp_iMembers>(_Description, _Table, tfp_pMembers)
			, ...
		);
	}

	template <typename tf_CProjectionRow, typename tf_CTable, typename ...tfp_CExpressions, umint ...tfp_iExpressions>
	auto fg_SqlApplyAggregateExpressions
		(
			CSqlPreparedSelectStatementDescription &_Description
			, tf_CTable const &_Table
			, TCSqlSelectedExpressions<tfp_CExpressions...> const &
			, std::index_sequence<tfp_iExpressions...>
		)
		-> void
	{
		_Description.m_SelectColumns.f_Clear();
		_Description.m_QualifiedSelectColumns.f_Clear();
		_Description.m_SelectExpressions.f_Clear();
		_Description.m_RowMapping.m_Fields.f_Clear();
		_Description.m_RowMapping.m_fCreateRow = &fg_SqlCreateRowData<tf_CProjectionRow>;
		(
			fg_SqlAppendSelectExpression<tf_CProjectionRow, tfp_iExpressions>(_Description, _Table, tfp_CExpressions{})
			, ...
		);
	}

	// Operation discriminators folded into every prepared-statement QueryID so that statements that would otherwise
	// hash identically (e.g. an INSERT and a DELETE that touch the same table and columns) never collide.
	enum class ESqlQueryTag : uint64
	{
		mc_Select = 1
		, mc_ProjectedSelect
		, mc_JoinSelect
		, mc_Join3Select
		, mc_JoinNSelect
		, mc_SetSelect
		, mc_Insert
		, mc_Update
		, mc_Delete
		, mc_Upsert
	};

	// Resolve a row member pointer to the content hash of the column it maps to, at consteval time. Mirrors
	// fg_SqlColumnNameForMember but returns the precomputed name hash instead of building a runtime string, so it
	// can participate in constexpr QueryID computation. Returns 0 when the member does not map to a column of
	// _Table (e.g. an aggregate over no column, or a projection-row alias member), which lets the generic node
	// hasher below probe members without hard errors.
	template <typename tf_CTable, typename tf_CMember>
	constexpr uint64 fg_SqlColumnHashForMember(tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember)
	{
		uint64 Hash = 0;
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == _pMember; })
							{
								if (p_Columns.m_pMember == _pMember)
									Hash = p_Columns.m_NameHash;
							}
						}
						()
						, ...
					);
				}
			)
		;

		return Hash;
	}

	// Generic, recursive content hasher for the value-bearing nodes of a prepared statement (predicates,
	// expressions, ORDER BY terms, HAVING, LIMIT/OFFSET, DISTINCT). It probes for the discriminating members each
	// node kind exposes - the operator/expression enum (mc_Type), parameter count (mc_nParameters), direction and
	// flag bits, the column member pointers (resolved to column-name hashes), nested sub-statement QueryIDs, and
	// nested predicate/expression values - and folds whatever is present. New node kinds are covered automatically
	// as long as they expose these conventional members, so the per-statement QueryID functions stay small.
	template <typename tf_CTable, typename tf_CNode>
	constexpr uint64 fg_SqlHashNode(uint64 _Hash, tf_CTable const &_Table, tf_CNode const &_Node)
	{
		if constexpr (requires { tf_CNode::mc_Type; })
			_Hash = fg_SqlHashMixValue(_Hash, uint64(tf_CNode::mc_Type));
		if constexpr (requires { tf_CNode::mc_nParameters; })
			_Hash = fg_SqlHashMixValue(_Hash, uint64(tf_CNode::mc_nParameters));
		if constexpr (requires { tf_CNode::mc_bDescending; })
			_Hash = fg_SqlHashMixValue(_Hash, tf_CNode::mc_bDescending ? 1u : 0u);
		if constexpr (requires { tf_CNode::mc_bDistinct; })
			_Hash = fg_SqlHashMixValue(_Hash, tf_CNode::mc_bDistinct ? 1u : 0u);
		if constexpr (requires { tf_CNode::mc_bHasLimit; })
			_Hash = fg_SqlHashMixValue(_Hash, (tf_CNode::mc_bHasLimit ? 1u : 0u) | (tf_CNode::mc_bHasOffset ? 2u : 0u));

		if constexpr (requires { tf_CNode::mc_pMember; })
		{
			if constexpr (requires { fg_SqlColumnHashForMember(_Table, tf_CNode::mc_pMember); })
				_Hash = fg_SqlHashMixValue(_Hash, fg_SqlColumnHashForMember(_Table, tf_CNode::mc_pMember));
		}
		if constexpr (requires { tf_CNode::mc_pLeftMember; })
		{
			if constexpr (requires { fg_SqlColumnHashForMember(_Table, tf_CNode::mc_pLeftMember); })
				_Hash = fg_SqlHashMixValue(_Hash, fg_SqlColumnHashForMember(_Table, tf_CNode::mc_pLeftMember));
		}
		if constexpr (requires { tf_CNode::mc_pRightMember; })
		{
			if constexpr (requires { fg_SqlColumnHashForMember(_Table, tf_CNode::mc_pRightMember); })
				_Hash = fg_SqlHashMixValue(_Hash, fg_SqlColumnHashForMember(_Table, tf_CNode::mc_pRightMember));
		}

		if constexpr (requires { tf_CNode::mc_Subquery.m_QueryID; })
			_Hash = fg_SqlHashMixValue(_Hash, tf_CNode::mc_Subquery.m_QueryID.m_Value);

		// A backend-function expression emits its function name into the SQL but carries no operator enum, so without
		// this two different functions over the same column would hash alike and the cache could run one's SQL for
		// the other. mc_FunctionName is a gc_Str, which converts to the TCStrConst the content hasher expects.
		if constexpr (requires { tf_CNode::mc_FunctionName; })
			_Hash = fg_SqlHashMixString(_Hash, tf_CNode::mc_FunctionName);

		if constexpr (requires { _Node.m_Left; _Node.m_Right; })
		{
			_Hash = fg_SqlHashNode(_Hash, _Table, _Node.m_Left);
			_Hash = fg_SqlHashNode(_Hash, _Table, _Node.m_Right);
		}
		if constexpr (requires { _Node.m_Predicate; })
			_Hash = fg_SqlHashNode(_Hash, _Table, _Node.m_Predicate);
		if constexpr (requires { _Node.m_Expression; })
			_Hash = fg_SqlHashNode(_Hash, _Table, _Node.m_Expression);

		return _Hash;
	}

	// The type-pack clauses (GROUP BY, the selected-column lists, ORDER BY, selected expressions) keep their content
	// in the template parameter list rather than in member values, so they need explicit pack expansion. These
	// overloads are more specialized than the generic node hasher above and so are preferred for these types.
	template <typename tf_CTable, auto ...tfp_pMembers>
	constexpr uint64 fg_SqlHashNode(uint64 _Hash, tf_CTable const &_Table, TCSqlGroupBy<tfp_pMembers...> const &)
	{
		((_Hash = fg_SqlHashMixValue(_Hash, fg_SqlColumnHashForMember(_Table, tfp_pMembers))), ...);

		return _Hash;
	}

	template <typename tf_CTable, auto ...tfp_pMembers>
	constexpr uint64 fg_SqlHashNode(uint64 _Hash, tf_CTable const &_Table, TCSqlSelectedColumns<tfp_pMembers...> const &)
	{
		((_Hash = fg_SqlHashMixValue(_Hash, fg_SqlColumnHashForMember(_Table, tfp_pMembers))), ...);

		return _Hash;
	}

	template <typename tf_CTable, typename ...tfp_CTerms>
	constexpr uint64 fg_SqlHashNode(uint64 _Hash, tf_CTable const &_Table, TCSqlOrderBy<tfp_CTerms...> const &)
	{
		((_Hash = fg_SqlHashNode(_Hash, _Table, tfp_CTerms{})), ...);

		return _Hash;
	}

	template <typename tf_CTable, typename ...tfp_CExpressions>
	constexpr uint64 fg_SqlHashNode(uint64 _Hash, tf_CTable const &_Table, TCSqlSelectedExpressions<tfp_CExpressions...> const &)
	{
		((_Hash = fg_SqlHashNode(_Hash, _Table, tfp_CExpressions{})), ...);

		return _Hash;
	}

	template <auto &tf_PreparedSelect>
	constexpr auto gc_SqlPreparedSelectWithLimit = tf_PreparedSelect.f_WithLimit();

	template <auto &tf_PreparedSelect, typename tf_CProjectionRow, auto ...tfp_pMembers>
	constexpr CSqlQueryID fg_SqlPreparedProjectedSelectQueryID()
	{
		// A projection is identified by the underlying select's (content-based) QueryID, the projected source columns
		// (tfp_pMembers are members of the underlying table's row, so they resolve against m_Table), and the result
		// row's layout. The prepared cache resolves each statement's row mapping by the compile-time-unique description
		// pointer, not by the shared QueryID, precisely because a row mapping cannot always be folded into the
		// SQL-derived QueryID, so this layout fold is not what prevents a mismatched-mapping reuse. It is kept so the
		// QueryID still reflects the projected result shape for diagnostics and the QueryID-keyed server-side prepare:
		// the source columns map positionally onto the result members, so fold each result member's SQL value type and
		// nullability in order.
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_ProjectedSelect));
		Hash = fg_SqlHashMixValue(Hash, tf_PreparedSelect.m_QueryID.m_Value);
		((Hash = fg_SqlHashMixValue(Hash, fg_SqlColumnHashForMember(tf_PreparedSelect.m_Table, tfp_pMembers))), ...);
		[&]<umint ...tfp_iMember>(std::index_sequence<tfp_iMember...>)
		{
			(
				(
					Hash = fg_SqlHashMixValue(Hash, uint64(TCSqlValueType<typename TCSqlProjectionMemberType<tf_CProjectionRow, tfp_iMember>::CType>::mc_Type))
					, Hash = fg_SqlHashMixValue(Hash, NStorage::cIsOptional<typename TCSqlProjectionMemberType<tf_CProjectionRow, tfp_iMember>::CType> ? 1u : 0u)
				)
				, ...
			);
		}
		(std::make_index_sequence<sizeof...(tfp_pMembers)>());

		return {Hash};
	}

	template <auto &tf_PreparedSelect, typename tf_CProjectionRow, auto ...tfp_pMembers>
	struct TCSqlSelectProjection : public ICSqlPreparedSelectStatement
	{
		constexpr TCSqlSelectProjection()
			: ICSqlPreparedSelectStatement(fg_SqlPreparedProjectedSelectQueryID<tf_PreparedSelect, tf_CProjectionRow, tfp_pMembers...>())
		{
		}

		CSqlPreparedSelectStatementDescription f_Describe() const override
		{
			using CPreparedSelect = NTraits::TCDecay<decltype(tf_PreparedSelect)>;
			using CTable = typename CPreparedSelect::CTable;

			CSqlPreparedSelectStatementDescription Description = tf_PreparedSelect.f_Describe();
			Description.m_QueryID = m_QueryID;
			fg_SqlApplyProjectionColumns<tf_CProjectionRow, CTable, tfp_pMembers...>
				(
					Description
					, tf_PreparedSelect.m_Table
					, std::make_index_sequence<sizeof...(tfp_pMembers)>()
				)
			;

			return Description;
		}
	};

	template <typename tf_CResult, auto ...tfp_pMembers, typename tf_CRow>
	tf_CResult fg_SqlProjectionResultFromRow(tf_CRow &&_Row)
	{
		using CResult = NTraits::TCRemoveReferenceAndQualifiers<tf_CResult>;
		using CRow = NTraits::TCRemoveReferenceAndQualifiers<tf_CRow>;

		if constexpr (NTraits::cIsSame<CResult, CRow>)
			return fg_Move(_Row);
		else
			return [&]<umint ...tfp_iMembers>(std::index_sequence<tfp_iMembers...>)
			{
				return tf_CResult{fg_Move(fg_Get<tfp_iMembers>(_Row))...};
			}(std::make_index_sequence<sizeof...(tfp_pMembers)>());
	}

	template <typename tf_CTable>
	void fg_SqlApplySelectColumns(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, CSqlAllColumns const &)
	{
		_Description.m_SelectColumns.f_Clear();
		_Description.m_QualifiedSelectColumns.f_Clear();
		_Description.m_SelectExpressions.f_Clear();
		_Description.m_RowMapping = fg_SqlRowMapping(_Table);
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							_Description.m_SelectColumns.f_InsertLast(p_Columns.f_Name());
						}
						()
						, ...
					);
				}
			)
		;
	}

	template <typename tf_CTable, typename ...tfp_CExpressions>
	void fg_SqlApplySelectColumns(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, TCSqlSelectedExpressions<tfp_CExpressions...> const &_Selection)
	{
		using CRow = typename TCSqlSelectedExpressions<tfp_CExpressions...>::CRow;

		fg_SqlApplyAggregateExpressions<CRow, tf_CTable, tfp_CExpressions...>
			(
				_Description
				, _Table
				, _Selection
				, std::make_index_sequence<sizeof...(tfp_CExpressions)>()
			)
		;
	}

	template <typename tf_CTable, auto ...tfp_pMembers>
	void fg_SqlApplySelectColumns(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, TCSqlSelectedColumns<tfp_pMembers...> const &)
	{
		using CRow = typename TCSqlSelectedColumns<tfp_pMembers...>::CRow;

		fg_SqlApplyProjectionColumns<CRow, tf_CTable, tfp_pMembers...>
			(
				_Description
				, _Table
				, std::make_index_sequence<sizeof...(tfp_pMembers)>()
			)
		;
	}

	template <typename tf_CTable, typename tf_CMember>
	NStr::CStr fg_SqlColumnNameForMember(tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember);

	template <typename tf_CTable, typename tf_CMember>
	void fg_SqlAppendUpdateColumn(CSqlPreparedUpdateStatementDescription &_Description, tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember)
	{
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == _pMember; })
							{
								if (p_Columns.m_pMember == _pMember)
								{
									using CColumn = NTraits::TCDecay<decltype(p_Columns)>;
									using CStoredMember = typename CColumn::CStoredMember;

									_Description.m_UpdateColumns.f_InsertLast(p_Columns.f_Name());
									_Description.m_UpdateColumnTypes.f_InsertLast(TCSqlValueType<CStoredMember>::mc_Type);
								}
							}
						}
						()
						, ...
					);
				}
			)
		;
	}

	template <typename tf_CTable, auto ...tfp_pMembers>
	void fg_SqlApplyUpdateColumns(CSqlPreparedUpdateStatementDescription &_Description, tf_CTable const &_Table, TCSqlSelectedColumns<tfp_pMembers...> const &)
	{
		(
			fg_SqlAppendUpdateColumn(_Description, _Table, tfp_pMembers)
			, ...
		);
	}

	template <typename tf_CDescription, typename tf_CTable>
	void fg_SqlApplyImplicitInsertColumns(tf_CDescription &_Description, tf_CTable const &_Table)
	{
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if (!fg_SqlPreparedInsertColumnIsImplicitlySelected(p_Columns))
								return;

							using CColumn = NTraits::TCDecay<decltype(p_Columns)>;
							using CStoredMember = typename CColumn::CStoredMember;

							_Description.m_InsertColumns.f_InsertLast(p_Columns.f_Name());
							_Description.m_InsertColumnTypes.f_InsertLast(TCSqlValueType<CStoredMember>::mc_Type);
						}
						()
						, ...
					);
				}
			)
		;
	}

	template <typename tf_CTable, typename tf_CMember>
	void fg_SqlAppendColumnNameForMember(NContainer::TCVector<NStr::CStr> &_ColumnNames, tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember)
	{
		_ColumnNames.f_InsertLast(fg_SqlColumnNameForMember(_Table, _pMember));
	}

	template <typename tf_CTable, auto ...tfp_pMembers>
	void fg_SqlApplyUpsertConflictColumns(CSqlPreparedUpsertStatementDescription &_Description, tf_CTable const &_Table, TCSqlSelectedColumns<tfp_pMembers...> const &)
	{
		(
			fg_SqlAppendColumnNameForMember(_Description.m_ConflictColumns, _Table, tfp_pMembers)
			, ...
		);
	}

	template <typename tf_CTable, auto ...tfp_pMembers>
	void fg_SqlApplyUpsertUpdateColumns(CSqlPreparedUpsertStatementDescription &_Description, tf_CTable const &_Table, TCSqlSelectedColumns<tfp_pMembers...> const &)
	{
		(
			fg_SqlAppendColumnNameForMember(_Description.m_UpdateColumns, _Table, tfp_pMembers)
			, ...
		);
	}

	template <typename tf_CTable, typename tf_CMember>
	NStr::CStr fg_SqlColumnNameForMember(tf_CTable const &_Table, tf_CMember tf_CTable::CRow::*_pMember)
	{
		NStr::CStr ColumnName;
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == _pMember; })
							{
								if (p_Columns.m_pMember == _pMember)
									ColumnName = p_Columns.f_Name();
							}
						}
						()
						, ...
					);
				}
			)
		;

		return ColumnName;
	}

	template
	<
		typename tf_CTable
		, typename tf_CPredicate
		, typename tf_COrderBy
		, typename tf_CLimitOffset
		, typename tf_CDistinct
		, typename tf_CSelection
		, typename tf_CGroupBy
		, typename tf_CHaving
	>
	constexpr CSqlQueryID fg_SqlPreparedSelectQueryID
		(
			tf_CTable const &_Table
			, tf_CPredicate const &_Predicate
			, tf_COrderBy const &_OrderBy
			, tf_CLimitOffset const &_LimitOffset
			, tf_CDistinct const &_Distinct
			, tf_CSelection const &_Selection
			, tf_CGroupBy const &_GroupBy
			, tf_CHaving const &_Having
		)
	{
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_Select));
		Hash = fg_SqlHashMixValue(Hash, _Table.m_NameHash);
		Hash = fg_SqlHashNode(Hash, _Table, _Selection);
		Hash = fg_SqlHashNode(Hash, _Table, _Predicate);
		Hash = fg_SqlHashNode(Hash, _Table, _OrderBy);
		Hash = fg_SqlHashNode(Hash, _Table, _LimitOffset);
		Hash = fg_SqlHashNode(Hash, _Table, _Distinct);
		Hash = fg_SqlHashNode(Hash, _Table, _GroupBy);
		Hash = fg_SqlHashNode(Hash, _Table, _Having);

		return {Hash};
	}

	// Resolve a join member pointer to its column-name hash, trying each participating table. A member pointer is
	// typed to its row, so only the table whose row matches resolves it; the rest fail the requires-guard.
	template <typename tf_CMember, typename ...tfp_CTables>
	constexpr uint64 fg_SqlColumnHashAcrossTables(tf_CMember _pMember, tfp_CTables const &...p_Tables)
	{
		uint64 Hash = 0;
		(
			[&]
			{
				if constexpr (requires { fg_SqlColumnHashForMember(p_Tables, _pMember); })
				{
					uint64 ColumnHash = fg_SqlColumnHashForMember(p_Tables, _pMember);
					if (ColumnHash != 0)
						Hash = ColumnHash;
				}
			}
			()
			, ...
		);

		return Hash;
	}

	template <auto tf_pLeftMember, auto tf_pRightMember, ESqlPredicateType tf_Type, typename ...tfp_CTables>
	constexpr uint64 fg_SqlHashJoinOn(uint64 _Hash, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, tf_Type> const &, tfp_CTables const &...p_Tables)
	{
		_Hash = fg_SqlHashMixValue(_Hash, uint64(tf_Type));
		_Hash = fg_SqlHashMixValue(_Hash, fg_SqlColumnHashAcrossTables(tf_pLeftMember, p_Tables...));
		_Hash = fg_SqlHashMixValue(_Hash, fg_SqlColumnHashAcrossTables(tf_pRightMember, p_Tables...));

		return _Hash;
	}

	template <auto tf_pLeftMember, auto tf_pRightMember, typename ...tfp_CTables>
	constexpr uint64 fg_SqlHashJoinOn(uint64 _Hash, TCSqlJoinOnEqual<tf_pLeftMember, tf_pRightMember> const &, tfp_CTables const &...p_Tables)
	{
		return fg_SqlHashJoinOn(_Hash, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_EqualParameter>{}, p_Tables...);
	}

	template <typename ...tfp_CPredicates, typename ...tfp_CTables>
	constexpr uint64 fg_SqlHashJoinOn(uint64 _Hash, TCSqlJoinOnAll<tfp_CPredicates...> const &, tfp_CTables const &...p_Tables)
	{
		((_Hash = fg_SqlHashJoinOn(_Hash, tfp_CPredicates{}, p_Tables...)), ...);

		return _Hash;
	}

	template <ESqlJoinType tf_JoinType, typename tf_CLeftTable, typename tf_CRightTable, typename tf_CJoinOn>
	constexpr CSqlQueryID fg_SqlPreparedJoinSelectQueryID(tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable, tf_CJoinOn const &_JoinOn)
	{
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_JoinSelect));
		Hash = fg_SqlHashMixValue(Hash, uint64(tf_JoinType));
		Hash = fg_SqlHashMixValue(Hash, _LeftTable.m_NameHash);
		Hash = fg_SqlHashMixValue(Hash, _RightTable.m_NameHash);
		Hash = fg_SqlHashJoinOn(Hash, _JoinOn, _LeftTable, _RightTable);

		return {Hash};
	}

	template
	<
		ESqlJoinType tf_FirstJoinType
		, ESqlJoinType tf_SecondJoinType
		, typename tf_CLeftTable
		, typename tf_CMiddleTable
		, typename tf_CRightTable
		, typename tf_CFirstJoinOn
		, typename tf_CSecondJoinOn
	>
	constexpr auto fg_SqlPreparedJoin3SelectQueryID
		(
			tf_CLeftTable const &_LeftTable
			, tf_CMiddleTable const &_MiddleTable
			, tf_CRightTable const &_RightTable
			, tf_CFirstJoinOn const &_FirstJoinOn
			, tf_CSecondJoinOn const &_SecondJoinOn
		)
		-> CSqlQueryID
	{
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_Join3Select));
		Hash = fg_SqlHashMixValue(Hash, uint64(tf_FirstJoinType));
		Hash = fg_SqlHashMixValue(Hash, uint64(tf_SecondJoinType));
		Hash = fg_SqlHashMixValue(Hash, _LeftTable.m_NameHash);
		Hash = fg_SqlHashMixValue(Hash, _MiddleTable.m_NameHash);
		Hash = fg_SqlHashMixValue(Hash, _RightTable.m_NameHash);
		Hash = fg_SqlHashJoinOn(Hash, _FirstJoinOn, _LeftTable, _MiddleTable, _RightTable);
		Hash = fg_SqlHashJoinOn(Hash, _SecondJoinOn, _LeftTable, _MiddleTable, _RightTable);

		return {Hash};
	}

	template <typename ...tfp_CTerms, typename ...tfp_CTables>
	constexpr CSqlQueryID fg_SqlPreparedJoinNSelectQueryID(TCSqlJoinTerms<tfp_CTerms...> const &, tfp_CTables const &...p_Tables)
	{
		// The join-on types are stateless (every operand is a member-pointer / enum NTTP), so each term supplies both
		// its join type and a default-constructed join-on to hash - no need to thread the runtime join-on values.
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_JoinNSelect));
		((Hash = fg_SqlHashMixValue(Hash, p_Tables.m_NameHash)), ...);
		(
			(
				Hash = fg_SqlHashMixValue(Hash, uint64(tfp_CTerms::mc_JoinType))
				, Hash = fg_SqlHashJoinOn(Hash, typename tfp_CTerms::CJoinOn{}, p_Tables...)
			)
			, ...
		);

		return {Hash};
	}

	template <auto &tf_LeftSelect, auto &tf_RightSelect, ESqlSetOperationType tf_Type>
	constexpr CSqlQueryID fg_SqlPreparedSetSelectQueryID()
	{
		// A set operation is identified by its operator plus the (content-based) QueryIDs of its two operand selects.
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_SetSelect));
		Hash = fg_SqlHashMixValue(Hash, uint64(tf_Type));
		Hash = fg_SqlHashMixValue(Hash, tf_LeftSelect.m_QueryID.m_Value);
		Hash = fg_SqlHashMixValue(Hash, tf_RightSelect.m_QueryID.m_Value);

		return {Hash};
	}

	template <typename tf_CTable, typename tf_CPredicate, typename tf_CSet>
	constexpr CSqlQueryID fg_SqlPreparedUpdateQueryID(tf_CTable const &_Table, tf_CPredicate const &_Predicate, tf_CSet const &_Set)
	{
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_Update));
		Hash = fg_SqlHashMixValue(Hash, _Table.m_NameHash);
		Hash = fg_SqlHashNode(Hash, _Table, _Set);
		Hash = fg_SqlHashNode(Hash, _Table, _Predicate);

		return {Hash};
	}

	template <typename tf_CTable, typename tf_CPredicate>
	constexpr CSqlQueryID fg_SqlPreparedDeleteQueryID(tf_CTable const &_Table, tf_CPredicate const &_Predicate)
	{
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_Delete));
		Hash = fg_SqlHashMixValue(Hash, _Table.m_NameHash);
		Hash = fg_SqlHashNode(Hash, _Table, _Predicate);

		return {Hash};
	}

	template <typename tf_CTable, typename tf_CConflict, typename tf_CUpdate>
	constexpr CSqlQueryID fg_SqlPreparedUpsertQueryID(tf_CTable const &_Table, tf_CConflict const &_Conflict, tf_CUpdate const &_Update)
	{
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_Upsert));
		Hash = fg_SqlHashMixValue(Hash, _Table.m_NameHash);
		Hash = fg_SqlHashNode(Hash, _Table, _Conflict);
		Hash = fg_SqlHashNode(Hash, _Table, _Update);

		return {Hash};
	}

	template <typename tf_CTable, typename tf_CPredicate>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, tf_CPredicate const &_Predicate, umint &o_iParameter);

	template <typename tf_CLeftTable, typename tf_CRightTable, auto tf_pLeftMember, auto tf_pRightMember, ESqlPredicateType tf_Type>
	auto fg_SqlAppendJoinOn
		(
			CSqlJoinDescription &_Join
			, tf_CLeftTable const &_LeftTable
			, tf_CRightTable const &_RightTable
			, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, tf_Type> const &
		)
		-> void
	{
		_Join.m_On.f_InsertLast
			(
				{
					.m_Type = tf_Type
					, .m_iLeftTable = 0
					, .m_iRightTable = 1
					, .m_LeftColumnName = fg_SqlColumnNameForMember(_LeftTable, tf_pLeftMember)
					, .m_RightColumnName = fg_SqlColumnNameForMember(_RightTable, tf_pRightMember)
				}
			)
		;
	}

	template <typename tf_CLeftTable, typename tf_CRightTable, auto tf_pLeftMember, auto tf_pRightMember>
	void fg_SqlAppendJoinOn(CSqlJoinDescription &_Join, tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable, TCSqlJoinOnEqual<tf_pLeftMember, tf_pRightMember> const &)
	{
		fg_SqlAppendJoinOn(_Join, _LeftTable, _RightTable, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_EqualParameter>{});
	}

	template <typename tf_CLeftTable, typename tf_CRightTable, typename ...tfp_CPredicates>
	void fg_SqlAppendJoinOn(CSqlJoinDescription &_Join, tf_CLeftTable const &_LeftTable, tf_CRightTable const &_RightTable, TCSqlJoinOnAll<tfp_CPredicates...> const &)
	{
		(
			fg_SqlAppendJoinOn(_Join, _LeftTable, _RightTable, tfp_CPredicates{})
			, ...
		);
	}

	template <typename tf_CLeftTable, typename tf_CRightTable, typename tf_CJoinOn>
	auto fg_SqlApplyJoinOn
		(
			CSqlPreparedSelectStatementDescription &_Description
			, tf_CLeftTable const &_LeftTable
			, tf_CRightTable const &_RightTable
			, tf_CJoinOn const &_JoinOn
			, ESqlJoinType _JoinType
		)
		-> void
	{
		CSqlJoinDescription Join;
		Join.m_Type = _JoinType;
		Join.m_TableName = _RightTable.f_Name();
		fg_SqlAppendJoinOn(Join, _LeftTable, _RightTable, _JoinOn);
		_Description.m_Joins.f_InsertLast(fg_Move(Join));
	}

	template <typename tf_CLeftTable, typename tf_CMiddleTable, typename tf_CRightTable, auto tf_pLeftMember, auto tf_pRightMember, ESqlPredicateType tf_Type>
	auto fg_SqlAppendJoinOn
		(
			CSqlJoinDescription &_Join
			, tf_CLeftTable const &_LeftTable
			, tf_CMiddleTable const &_MiddleTable
			, tf_CRightTable const &_RightTable
			, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, tf_Type> const &
		)
		-> void
	{
		using CLeftPredicateRow = typename TCSqlMemberPointerTraits<tf_pLeftMember>::CRow;
		constexpr umint c_iLeftTable = NTraits::cIsSame<CLeftPredicateRow, typename tf_CMiddleTable::CRow> ? umint(1) : umint(0);
		auto LeftColumnName = [&]
		{
			if constexpr (c_iLeftTable == 1)
				return fg_SqlColumnNameForMember(_MiddleTable, tf_pLeftMember);
			else
				return fg_SqlColumnNameForMember(_LeftTable, tf_pLeftMember);
		}();

		_Join.m_On.f_InsertLast
			(
				{
					.m_Type = tf_Type
					, .m_iLeftTable = c_iLeftTable
					, .m_iRightTable = 2
					, .m_LeftColumnName = fg_Move(LeftColumnName)
					, .m_RightColumnName = fg_SqlColumnNameForMember(_RightTable, tf_pRightMember)
				}
			)
		;
	}

	template <typename tf_CLeftTable, typename tf_CMiddleTable, typename tf_CRightTable, auto tf_pLeftMember, auto tf_pRightMember>
	auto fg_SqlAppendJoinOn
		(
			CSqlJoinDescription &_Join
			, tf_CLeftTable const &_LeftTable
			, tf_CMiddleTable const &_MiddleTable
			, tf_CRightTable const &_RightTable
			, TCSqlJoinOnEqual<tf_pLeftMember, tf_pRightMember> const &
		)
		-> void
	{
		fg_SqlAppendJoinOn(_Join, _LeftTable, _MiddleTable, _RightTable, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_EqualParameter>{});
	}

	template <typename tf_CLeftTable, typename tf_CMiddleTable, typename tf_CRightTable, typename ...tfp_CPredicates>
	auto fg_SqlAppendJoinOn
		(
			CSqlJoinDescription &_Join
			, tf_CLeftTable const &_LeftTable
			, tf_CMiddleTable const &_MiddleTable
			, tf_CRightTable const &_RightTable
			, TCSqlJoinOnAll<tfp_CPredicates...> const &
		)
		-> void
	{
		(
			fg_SqlAppendJoinOn(_Join, _LeftTable, _MiddleTable, _RightTable, tfp_CPredicates{})
			, ...
		);
	}

	template <typename tf_CLeftTable, typename tf_CMiddleTable, typename tf_CRightTable, typename tf_CJoinOn>
	auto fg_SqlApplyJoinOn
		(
			CSqlPreparedSelectStatementDescription &_Description
			, tf_CLeftTable const &_LeftTable
			, tf_CMiddleTable const &_MiddleTable
			, tf_CRightTable const &_RightTable
			, tf_CJoinOn const &_JoinOn
			, ESqlJoinType _JoinType
		)
		-> void
	{
		CSqlJoinDescription Join;
		Join.m_Type = _JoinType;
		Join.m_TableName = _RightTable.f_Name();
		fg_SqlAppendJoinOn(Join, _LeftTable, _MiddleTable, _RightTable, _JoinOn);
		_Description.m_Joins.f_InsertLast(fg_Move(Join));
	}

	template <typename tf_CRow, umint tf_iRightTable, umint tf_iCurrentTable, typename ...tfp_CTables>
	struct TCSqlJoinLeftTableIndex;

	template <typename tf_CRow, umint tf_iRightTable, umint tf_iCurrentTable>
	struct TCSqlJoinLeftTableIndex<tf_CRow, tf_iRightTable, tf_iCurrentTable>
	{
		static constexpr bool mc_bFound = false;
		static constexpr umint mc_iTable = 0;
	};

	template <typename tf_CRow, umint tf_iRightTable, umint tf_iCurrentTable, typename tf_CTable, typename ...tfp_CTables>
	struct TCSqlJoinLeftTableIndex<tf_CRow, tf_iRightTable, tf_iCurrentTable, tf_CTable, tfp_CTables...>
	{
		using CNext = TCSqlJoinLeftTableIndex<tf_CRow, tf_iRightTable, tf_iCurrentTable + 1, tfp_CTables...>;
		static constexpr bool mc_bCurrentMatches = tf_iCurrentTable < tf_iRightTable && NTraits::cIsSame<tf_CRow, typename tf_CTable::CRow>;
		static constexpr bool mc_bFound = CNext::mc_bFound || mc_bCurrentMatches;
		static constexpr umint mc_iTable = CNext::mc_bFound ? CNext::mc_iTable : (mc_bCurrentMatches ? tf_iCurrentTable : 0);
	};

	template <umint tf_iRightTable, typename tf_CTables, auto tf_pLeftMember, auto tf_pRightMember, ESqlPredicateType tf_Type>
	void fg_SqlAppendJoinOn(CSqlJoinDescription &_Join, tf_CTables const &_Tables, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, tf_Type> const &);

	template <umint tf_iRightTable, typename ...tfp_CTables, auto tf_pLeftMember, auto tf_pRightMember, ESqlPredicateType tf_Type>
	void fg_SqlAppendJoinOn(CSqlJoinDescription &_Join, NStorage::TCTuple<tfp_CTables const &...> const &_Tables, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, tf_Type> const &)
	{
		using CLeftPredicateRow = typename TCSqlMemberPointerTraits<tf_pLeftMember>::CRow;
		using CLeftIndex = TCSqlJoinLeftTableIndex<CLeftPredicateRow, tf_iRightTable, 0, tfp_CTables...>;
		static_assert(CLeftIndex::mc_bFound, "SQL chained join predicate left member must refer to a table already present in the join chain");
		constexpr umint c_iLeftTable = CLeftIndex::mc_iTable;

		_Join.m_On.f_InsertLast
			(
				{
					.m_Type = tf_Type
					, .m_iLeftTable = c_iLeftTable
					, .m_iRightTable = tf_iRightTable
					, .m_LeftColumnName = fg_SqlColumnNameForMember(fg_Get<c_iLeftTable>(_Tables), tf_pLeftMember)
					, .m_RightColumnName = fg_SqlColumnNameForMember(fg_Get<tf_iRightTable>(_Tables), tf_pRightMember)
				}
			)
		;
	}

	template <umint tf_iRightTable, typename tf_CTables, auto tf_pLeftMember, auto tf_pRightMember>
	void fg_SqlAppendJoinOn(CSqlJoinDescription &_Join, tf_CTables const &_Tables, TCSqlJoinOnEqual<tf_pLeftMember, tf_pRightMember> const &)
	{
		fg_SqlAppendJoinOn<tf_iRightTable>(_Join, _Tables, TCSqlJoinOnCompare<tf_pLeftMember, tf_pRightMember, ESqlPredicateType::mc_EqualParameter>{});
	}

	template <umint tf_iRightTable, typename tf_CTables, typename ...tfp_CPredicates>
	void fg_SqlAppendJoinOn(CSqlJoinDescription &_Join, tf_CTables const &_Tables, TCSqlJoinOnAll<tfp_CPredicates...> const &)
	{
		(
			fg_SqlAppendJoinOn<tf_iRightTable>(_Join, _Tables, tfp_CPredicates{})
			, ...
		);
	}

	template <umint tf_iTerm, typename tf_CTerm, typename tf_CTables, typename tf_CJoinOn>
	void fg_SqlApplyJoinTerm(CSqlPreparedSelectStatementDescription &_Description, tf_CTables const &_Tables, tf_CJoinOn const &_JoinOn)
	{
		constexpr umint c_iRightTable = tf_iTerm + 1;
		CSqlJoinDescription Join;
		Join.m_Type = tf_CTerm::mc_JoinType;
		Join.m_TableName = fg_Get<c_iRightTable>(_Tables).f_Name();
		fg_SqlAppendJoinOn<c_iRightTable>(Join, _Tables, _JoinOn);
		_Description.m_Joins.f_InsertLast(fg_Move(Join));
	}

	template <typename tf_CTerms, typename tf_CTables, typename tf_CJoinOns, umint ...tfp_iTerms>
	void fg_SqlApplyJoinTerms(CSqlPreparedSelectStatementDescription &_Description, tf_CTables const &_Tables, tf_CJoinOns const &_JoinOns, std::index_sequence<tfp_iTerms...>);

	template <typename ...tfp_CTerms, typename tf_CTables, typename tf_CJoinOns, umint ...tfp_iTerms>
	auto fg_SqlApplyJoinTerms
		(
			CSqlPreparedSelectStatementDescription &_Description
			, tf_CTables const &_Tables
			, tf_CJoinOns const &_JoinOns
			, TCSqlJoinTerms<tfp_CTerms...> const &
			, std::index_sequence<tfp_iTerms...>
		)
		-> void
	{
		(
			fg_SqlApplyJoinTerm<tfp_iTerms, tfp_CTerms>(_Description, _Tables, fg_Get<tfp_iTerms>(_JoinOns))
			, ...
		);
	}

	template <typename tf_CTerms, typename tf_CTables, typename tf_CJoinOns>
	void fg_SqlApplyJoinTerms(CSqlPreparedSelectStatementDescription &_Description, tf_CTables const &_Tables, tf_CJoinOns const &_JoinOns)
	{
		fg_SqlApplyJoinTerms(_Description, _Tables, _JoinOns, tf_CTerms{}, std::make_index_sequence<NStorage::gc_Tuple_Len<tf_CJoinOns>>{});
	}

	template <ESqlJoinType tf_JoinType, typename ...tfp_CTables, typename ...tfp_CTerms, typename tf_CNextTable, typename tf_CJoinOn, umint ...tfp_iTables, umint ...tfp_iTerms>
	consteval auto fg_SqlAppendPreparedJoin
		(
			TCSqlPreparedJoinNSelect<TCSqlJoinedTables<tfp_CTables...>, TCSqlJoinTerms<tfp_CTerms...>> const &_Select
			, tf_CNextTable const &_NextTable
			, tf_CJoinOn _JoinOn
			, std::index_sequence<tfp_iTables...>
			, std::index_sequence<tfp_iTerms...>
		)
	{
		using CNextTable = NTraits::TCRemoveReferenceAndQualifiers<tf_CNextTable>;
		using CResult = TCSqlPreparedJoinNSelect<TCSqlJoinedTables<tfp_CTables..., CNextTable>, TCSqlJoinTerms<tfp_CTerms..., TCSqlJoinTerm<CNextTable, tf_CJoinOn, tf_JoinType>>>;

		return CResult(std::get<tfp_iTables>(_Select.m_Tables)..., _NextTable, std::get<tfp_iTerms>(_Select.m_JoinOns)..., _JoinOn);
	}

	template <ESqlJoinType tf_JoinType, typename tf_CJoinSelect, typename tf_CNextTable, typename tf_CJoinOn>
	consteval auto fg_SqlAppendPreparedJoin(tf_CJoinSelect const &_Select, tf_CNextTable const &_NextTable, tf_CJoinOn _JoinOn)
	{
		return fg_SqlAppendPreparedJoin<tf_JoinType>
			(
				_Select
				, _NextTable
				, _JoinOn
				, std::make_index_sequence<NStorage::gc_Tuple_Len<decltype(_Select.m_Tables)>>{}
				, std::make_index_sequence<NStorage::gc_Tuple_Len<decltype(_Select.m_JoinOns)>>{}
			)
		;
	}

	template <typename tf_CTable, typename tf_CLeft, typename tf_CRight, ESqlPredicateType tf_Type>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlBinaryPredicate<tf_CLeft, tf_CRight, tf_Type> const &_Predicate, umint &o_iParameter);

	template <typename tf_CTable, typename tf_CPredicate>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlNotPredicate<tf_CPredicate> const &_Predicate, umint &o_iParameter);

	template <typename tf_CTable, auto tf_pMember, ESqlPredicateType tf_Type>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlParameterPredicate<tf_pMember, tf_Type> const &, umint &o_iParameter)
	{
		umint iParameter = o_iParameter++;

		return
			{
				.m_Type = tf_Type
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
				, .m_iParameter = iParameter
			}
		;
	}

	template <typename tf_CTable, auto tf_pMember, ESqlPredicateType tf_Type>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlNullPredicate<tf_pMember, tf_Type> const &, umint &)
	{
		// IS NULL / IS NOT NULL bind no value, so report zero parameters; otherwise the default count of 1 would push
		// the PostgreSQL placeholder numbering of a following parameterized clause (HAVING, a limited subquery) one too
		// far, emitting $2 for the next value while only one value is bound.
		return
			{
				.m_Type = tf_Type
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
				, .m_iParameter = 0
				, .m_nParameters = 0
			}
		;
	}

	template <typename tf_CTable>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &, CSqlAllRowsPredicate const &, umint &)
	{
		return
			{
				.m_Type = ESqlPredicateType::mc_AllRows
				, .m_iParameter = 0
				, .m_nParameters = 0
			}
		;
	}

	template <typename tf_CTable, auto tf_pMember, umint tf_nParameters>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlInPredicate<tf_pMember, tf_nParameters> const &, umint &o_iParameter)
	{
		umint iParameter = o_iParameter;
		o_iParameter += tf_nParameters;

		return
			{
				.m_Type = ESqlPredicateType::mc_InParameters
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
				, .m_iParameter = iParameter
				, .m_nParameters = tf_nParameters
			}
		;
	}

	template <typename tf_CTable, auto tf_pMember, auto &tf_Subquery>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlInSubqueryPredicate<tf_pMember, tf_Subquery> const &, umint &o_iParameter)
	{
		using CSubquery = NTraits::TCDecay<decltype(tf_Subquery)>;
		umint iParameter = o_iParameter;
		constexpr umint c_nParameters = TCSqlPreparedSelectNestedParameterTypesList<CSubquery>::CType::mc_nTypes;
		o_iParameter += c_nParameters;

		return
			{
				.m_Type = ESqlPredicateType::mc_InSubquery
				, .m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
				, .m_iParameter = iParameter
				, .m_nParameters = c_nParameters
				, .m_pSubqueryStatement = &tf_Subquery
			}
		;
	}

	template <typename tf_CTable, auto &tf_Subquery, ESqlPredicateType tf_Type>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &, TCSqlExistsPredicate<tf_Subquery, tf_Type> const &, umint &o_iParameter)
	{
		using CSubquery = NTraits::TCDecay<decltype(tf_Subquery)>;
		umint iParameter = o_iParameter;
		constexpr umint c_nParameters = TCSqlPreparedSelectNestedParameterTypesList<CSubquery>::CType::mc_nTypes;
		o_iParameter += c_nParameters;

		return
			{
				.m_Type = tf_Type
				, .m_iParameter = iParameter
				, .m_nParameters = c_nParameters
				, .m_pSubqueryStatement = &tf_Subquery
			}
		;
	}

	template <typename tf_CTable, typename tf_CLeft, typename tf_CRight, ESqlPredicateType tf_Type>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlBinaryPredicate<tf_CLeft, tf_CRight, tf_Type> const &_Predicate, umint &o_iParameter)
	{
		CSqlPredicateDescription Description;
		Description.m_Type = tf_Type;
		Description.m_Children.f_InsertLast(fg_SqlPredicateDescription(_Table, _Predicate.m_Left, o_iParameter));
		Description.m_Children.f_InsertLast(fg_SqlPredicateDescription(_Table, _Predicate.m_Right, o_iParameter));

		// Sum the described children rather than the static type constants: an EXISTS/IN subquery child consumes
		// parameters at runtime but reports mc_nParameters == 0 as a type constant, so the static sum would
		// undercount and later PostgreSQL $n placeholders (HAVING/LIMIT/OFFSET/set operands) would collide.
		Description.m_nParameters = Description.m_Children[0].m_nParameters + Description.m_Children[1].m_nParameters;

		return Description;
	}

	template <typename tf_CTable, typename tf_CPredicate>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, TCSqlNotPredicate<tf_CPredicate> const &_Predicate, umint &o_iParameter)
	{
		CSqlPredicateDescription Description;
		Description.m_Type = ESqlPredicateType::mc_Not;
		Description.m_Children.f_InsertLast(fg_SqlPredicateDescription(_Table, _Predicate.m_Predicate, o_iParameter));

		// Use the described child's parameter count, which includes any nested subquery parameters that the static
		// type constant reports as zero.
		Description.m_nParameters = Description.m_Children[0].m_nParameters;

		return Description;
	}

	template <typename tf_CTable, typename tf_CPredicate>
	CSqlPredicateDescription fg_SqlPredicateDescription(tf_CTable const &_Table, tf_CPredicate const &_Predicate)
	{
		umint iParameter = 0;

		return fg_SqlPredicateDescription(_Table, _Predicate, iParameter);
	}

	template <typename tf_CTable>
	void fg_SqlAppendOrderBy(CSqlPreparedSelectStatementDescription &, tf_CTable const &, CSqlNoOrderBy const &)
	{
	}

	template <typename tf_CTable, auto tf_pMember, bool tf_bDescending>
	void fg_SqlAppendOrderByTerm(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, TCSqlOrderByTerm<tf_pMember, tf_bDescending> const &)
	{
		_Description.m_OrderBy.f_InsertLast
			(
				{
					.m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember)
					, .m_bDescending = tf_bDescending
				}
			)
		;
	}

	template <typename tf_CTable, typename ...tfp_CTerms>
	void fg_SqlAppendOrderBy(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, TCSqlOrderBy<tfp_CTerms...> const &)
	{
		(
			fg_SqlAppendOrderByTerm(_Description, _Table, tfp_CTerms{})
			, ...
		);
	}

	template <typename tf_CTable>
	void fg_SqlAppendGroupBy(CSqlPreparedSelectStatementDescription &, tf_CTable const &, CSqlNoGroupBy const &)
	{
	}

	template <typename tf_CTable, auto ...tfp_pMembers>
	void fg_SqlAppendGroupBy(CSqlPreparedSelectStatementDescription &_Description, tf_CTable const &_Table, TCSqlGroupBy<tfp_pMembers...> const &)
	{
		(
			_Description.m_GroupBy.f_InsertLast({.m_ColumnName = fg_SqlColumnNameForMember(_Table, tfp_pMembers)})
			, ...
		);
	}

	template <typename tf_CTable>
	void fg_SqlApplyHaving(CSqlPreparedSelectStatementDescription &, tf_CTable const &, CSqlNoHaving const &, umint)
	{
	}

	template <typename tf_CTable, typename tf_CExpression, ESqlPredicateType tf_Type>
	auto fg_SqlApplyHaving
		(
			CSqlPreparedSelectStatementDescription &_Description
			, tf_CTable const &_Table
			, TCSqlHavingAggregatePredicate<tf_CExpression, tf_Type> const &_Having
			, umint _iParameter
		)
		-> void
	{
		_Description.m_bHasHaving = true;
		_Description.m_Having =
			{
				.m_Type = tf_Type
				, .m_Expression = fg_SqlSelectExpressionDescription(_Table, _Having.m_Expression)
				, .m_bExpression = true
				, .m_iParameter = _iParameter
			}
		;
	}

	template <typename tf_CLimitOffset>
	void fg_SqlApplyLimitOffset(CSqlPreparedSelectStatementDescription &_Description)
	{
		_Description.m_LimitOffset =
			{
				.m_bHasLimit = tf_CLimitOffset::mc_bHasLimit
				, .m_bHasOffset = tf_CLimitOffset::mc_bHasOffset
			}
		;
	}

	template <typename tf_CMember>
	void fg_SqlAppendSelectParameters(CSqlSelectOperation &)
	{
	}

	template <typename tf_CMember, typename tf_CParam, typename ...tfp_CParams>
	void fg_SqlAppendSelectParameters(CSqlSelectOperation &_Operation, tf_CParam _Param, tfp_CParams ...p_Params)
	{
		_Operation.m_Parameters.f_InsertLast(fg_SqlCoercedValue<tf_CMember>(fg_Move(_Param)));
		fg_SqlAppendSelectParameters<tf_CMember>(_Operation, fg_Move(p_Params)...);
	}

	template <typename tf_CMemberList>
	struct TCSqlSelectParameterAppender;

	template <>
	struct TCSqlSelectParameterAppender<TCSqlMemberList<>>
	{
		static void fs_Append(CSqlSelectOperation &)
		{
		}
	};

	template <typename tf_CMemberList>
	struct TCSqlUpdateValueAppender;

	template <>
	struct TCSqlUpdateValueAppender<TCSqlMemberList<>>
	{
		template <typename tf_COperation>
		static void fs_Append(tf_COperation &)
		{
		}
	};

	template <auto tf_pMember, auto ...tfp_pMembers>
	struct TCSqlUpdateValueAppender<TCSqlMemberList<tf_pMember, tfp_pMembers...>>
	{
		template <typename tf_COperation, typename tf_CValue, typename ...tfp_CValues>
		static void fs_Append(tf_COperation &_Operation, tf_CValue &&_Value, tfp_CValues &&...p_Values)
		{
			using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;

			_Operation.m_Values.f_InsertLast(fg_SqlCoercedValue<CMember>(fg_Forward<tf_CValue>(_Value)));
			TCSqlUpdateValueAppender<TCSqlMemberList<tfp_pMembers...>>::fs_Append(_Operation, fg_Forward<tfp_CValues>(p_Values)...);
		}
	};

	template <auto tf_pMember, auto ...tfp_pMembers>
	struct TCSqlSelectParameterAppender<TCSqlMemberList<tf_pMember, tfp_pMembers...>>
	{
		template <typename tf_CParam, typename ...tfp_CParams>
		static void fs_Append(CSqlSelectOperation &_Operation, tf_CParam &&_Param, tfp_CParams &&...p_Params)
		{
			using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;
			_Operation.m_Parameters.f_InsertLast(fg_SqlCoercedValue<CMember>(fg_Forward<tf_CParam>(_Param)));
			TCSqlSelectParameterAppender<TCSqlMemberList<tfp_pMembers...>>::fs_Append(_Operation, fg_Forward<tfp_CParams>(p_Params)...);
		}
	};

	template <typename tf_CTypeList>
	struct TCSqlSelectTypeParameterAppender;

	template <>
	struct TCSqlSelectTypeParameterAppender<TCSqlTypeList<>>
	{
		static void fs_Append(CSqlSelectOperation &)
		{
		}
	};

	template <typename tf_CType, typename ...tfp_CTypes>
	struct TCSqlSelectTypeParameterAppender<TCSqlTypeList<tf_CType, tfp_CTypes...>>
	{
		template <typename tf_CParam, typename ...tfp_CParams>
		static void fs_Append(CSqlSelectOperation &_Operation, tf_CParam &&_Param, tfp_CParams &&...p_Params)
		{
			_Operation.m_Parameters.f_InsertLast(fg_SqlCoercedValue<tf_CType>(fg_Forward<tf_CParam>(_Param)));
			TCSqlSelectTypeParameterAppender<TCSqlTypeList<tfp_CTypes...>>::fs_Append(_Operation, fg_Forward<tfp_CParams>(p_Params)...);
		}
	};

	// Append update/delete parameter values by their declared types. Unlike the member-based appender this can
	// represent a predicate subquery's non-column parameters (HAVING, LIMIT/OFFSET, set operands), which the
	// generated SQL emits placeholders for.
	template <typename tf_CTypeList>
	struct TCSqlMutationTypeValueAppender;

	template <>
	struct TCSqlMutationTypeValueAppender<TCSqlTypeList<>>
	{
		template <typename tf_COperation>
		static void fs_Append(tf_COperation &)
		{
		}
	};

	template <typename tf_CType, typename ...tfp_CTypes>
	struct TCSqlMutationTypeValueAppender<TCSqlTypeList<tf_CType, tfp_CTypes...>>
	{
		template <typename tf_COperation, typename tf_CValue, typename ...tfp_CValues>
		static void fs_Append(tf_COperation &_Operation, tf_CValue &&_Value, tfp_CValues &&...p_Values)
		{
			_Operation.m_Values.f_InsertLast(fg_SqlCoercedValue<tf_CType>(fg_Forward<tf_CValue>(_Value)));
			TCSqlMutationTypeValueAppender<TCSqlTypeList<tfp_CTypes...>>::fs_Append(_Operation, fg_Forward<tfp_CValues>(p_Values)...);
		}
	};

	template <typename tf_CMemberList>
	struct TCSqlParameterTypesForMembers;

	template <auto ...tfp_pMembers>
	struct TCSqlParameterTypesForMembers<TCSqlMemberList<tfp_pMembers...>>
	{
		using CType = TCSqlParameterTypes<TCSqlValueType<typename TCSqlMemberPointerTraits<tfp_pMembers>::CMember>::mc_Type...>;
	};

	template <typename tf_CTypeList>
	struct TCSqlParameterTypesForTypeList;

	template <typename ...tfp_CTypes>
	struct TCSqlParameterTypesForTypeList<TCSqlTypeList<tfp_CTypes...>>
	{
		using CType = TCSqlParameterTypes<TCSqlValueType<tfp_CTypes>::mc_Type...>;
	};

	template <typename tf_CPreparedSelect, bool tf_bHasHaving>
	struct TCSqlPreparedSelectParameterTypesList_WithHaving;

	template <typename tf_CPreparedSelect>
	struct TCSqlPreparedSelectParameterTypesList_WithHaving<tf_CPreparedSelect, true>
	{
		using CWhereTypes = typename TCSqlPredicateParameterTypes<typename tf_CPreparedSelect::CPredicate>::CType;
		using CHavingTypes = typename TCSqlHavingParameterTypes<typename tf_CPreparedSelect::CHaving>::CType;
		using CType = typename TCSqlTypeListConcat<CWhereTypes, CHavingTypes>::CType;
	};

	template <typename tf_CPreparedSelect>
	struct TCSqlPreparedSelectParameterTypesList_WithHaving<tf_CPreparedSelect, false>
	{
		using CType = typename TCSqlPredicateParameterTypes<typename tf_CPreparedSelect::CPredicate>::CType;
	};

	template <typename tf_CPreparedSelect>
	struct TCSqlPreparedSelectParameterTypesList : public TCSqlPreparedSelectParameterTypesList_WithHaving<tf_CPreparedSelect, requires { typename tf_CPreparedSelect::CHaving; }>
	{
	};

	template <auto &tf_LeftSelect, auto &tf_RightSelect, ESqlSetOperationType tf_Type>
	struct TCSqlPreparedSelectParameterTypesList<TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, tf_Type>>
	{
		using CLeftTypes = typename TCSqlPreparedSelectParameterTypesList<NTraits::TCDecay<decltype(tf_LeftSelect)>>::CType;
		using CRightTypes = typename TCSqlPreparedSelectParameterTypesList<NTraits::TCDecay<decltype(tf_RightSelect)>>::CType;
		using CType = typename TCSqlTypeListConcat<CLeftTypes, CRightTypes>::CType;
	};

	template <typename tf_CLimitOffset>
	struct TCSqlLimitOffsetParameterTypes
	{
		using CType = TCSqlTypeList<>;
	};

	template <>
	struct TCSqlLimitOffsetParameterTypes<TCSqlLimitOffset<true, false>>
	{
		using CType = TCSqlTypeList<int64>;
	};

	template <>
	struct TCSqlLimitOffsetParameterTypes<TCSqlLimitOffset<false, true>>
	{
		using CType = TCSqlTypeList<int64>;
	};

	template <>
	struct TCSqlLimitOffsetParameterTypes<TCSqlLimitOffset<true, true>>
	{
		using CType = TCSqlTypeList<int64, int64>;
	};

	template <typename tf_CPreparedSelect>
	struct TCSqlPreparedSelectNestedParameterTypesList
	{
		using CParameterTypes = typename TCSqlPreparedSelectParameterTypesList<tf_CPreparedSelect>::CType;
		using CLimitOffsetTypes = typename TCSqlLimitOffsetParameterTypes<typename tf_CPreparedSelect::CLimitOffset>::CType;
		using CType = typename TCSqlTypeListConcat<CParameterTypes, CLimitOffsetTypes>::CType;
	};

	template <auto &tf_LeftSelect, auto &tf_RightSelect, ESqlSetOperationType tf_Type>
	struct TCSqlPreparedSelectNestedParameterTypesList<TCSqlPreparedSetSelect<tf_LeftSelect, tf_RightSelect, tf_Type>>
	{
		using CLeftTypes = typename TCSqlPreparedSelectNestedParameterTypesList<NTraits::TCDecay<decltype(tf_LeftSelect)>>::CType;
		using CRightTypes = typename TCSqlPreparedSelectNestedParameterTypesList<NTraits::TCDecay<decltype(tf_RightSelect)>>::CType;
		using CType = typename TCSqlTypeListConcat<CLeftTypes, CRightTypes>::CType;
	};

	template <typename tf_CMember, typename ...tfp_CParams>
	constexpr CSqlParameterTypesDescription fg_SqlSelectParameterTypes(ESqlValueType const *_pTypes)
	{
		CSqlQueryID QueryID{14695981039346656037ull};
		for (umint i = 0; i < sizeof...(tfp_CParams); ++i)
			QueryID = fg_SqlMixQueryID(QueryID, TCSqlValueType<tf_CMember>::mc_Type);

		return
			{
				.m_QueryID = QueryID
				, .m_pTypes = _pTypes
				, .m_nTypes = sizeof...(tfp_CParams)
			}
		;
	}

	template <auto ...tfp_pMembers, typename tf_CTable>
	constexpr CSqlQueryID fg_SqlPreparedInsertQueryID(tf_CTable const &_Table)
	{
		// An empty member pack means "insert all columns"; the table-name hash alone then distinguishes it from the
		// all-columns insert of any other table. An explicit member list folds each target column's name hash.
		uint64 Hash = gc_SqlHashSeed;
		Hash = fg_SqlHashMixValue(Hash, uint64(ESqlQueryTag::mc_Insert));
		Hash = fg_SqlHashMixValue(Hash, _Table.m_NameHash);
		((Hash = fg_SqlHashMixValue(Hash, fg_SqlColumnHashForMember(_Table, tfp_pMembers))), ...);

		return {Hash};
	}

	template <auto tf_pMember, typename tf_CColumn>
	constexpr bool fg_SqlPreparedInsertColumnIsSelectedByMember(tf_CColumn const &_Column)
	{
		if constexpr (requires { _Column.m_pMember == tf_pMember; })
			return _Column.m_pMember == tf_pMember;
		else
			return false;
	}

	template <typename tf_CColumn, auto ...tfp_pMembers>
	constexpr bool fg_SqlPreparedInsertColumnIsSelected(tf_CColumn const &_Column)
	{
		if constexpr (sizeof...(tfp_pMembers) == 0)
			return fg_SqlPreparedInsertColumnIsImplicitlySelected(_Column);
		else
			return (fg_SqlPreparedInsertColumnIsSelectedByMember<tfp_pMembers>(_Column) || ...);
	}

	template <typename tf_CRow, typename tf_CMember>
	void fg_SqlAppendInsertColumnValue(CSqlInsertOperation &_Operation, TCSqlColumn<tf_CRow, tf_CMember> const &_Column, tf_CRow &&_Row)
	{
		_Operation.m_Values.f_InsertLast({.m_ColumnName = _Column.f_Name(), .m_Value = fg_SqlValue(fg_Move(_Column.f_Value(_Row)))});
	}

	template <typename tf_CRow, typename tf_CMember>
	void fg_SqlAppendInsertValue(CSqlInsertOperation &_Operation, TCSqlColumn<tf_CRow, tf_CMember> const &_Column, tf_CRow &&_Row)
	{
		// Skip the columns the database fills itself - an autoincrement primary key and a generated column - so a
		// direct row insert never names a column the backend refuses an explicit value for.
		if (!fg_SqlPreparedInsertColumnIsImplicitlySelected(_Column))
			return;

		fg_SqlAppendInsertColumnValue(_Operation, _Column, fg_Forward<tf_CRow>(_Row));
	}

	template <auto ...tfp_pMembers>
	struct TCSqlPreparedInsertValueAppender;

	template <>
	struct TCSqlPreparedInsertValueAppender<>
	{
		template <typename tf_CTable>
		static void fs_Append(CSqlInsertOperation &, tf_CTable const &)
		{
		}
	};

	template <auto tf_pMember, auto ...tfp_pMembers>
	struct TCSqlPreparedInsertValueAppender<tf_pMember, tfp_pMembers...>
	{
		template <typename tf_CTable, typename tf_CValue, typename ...tfp_CValues>
		static void fs_Append(CSqlInsertOperation &_Operation, tf_CTable const &_Table, tf_CValue &&_Value, tfp_CValues &&...p_Values)
		{
			using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;
			_Operation.m_Values.f_InsertLast({.m_ColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember), .m_Value = fg_SqlCoercedValue<CMember>(fg_Forward<tf_CValue>(_Value))});
			TCSqlPreparedInsertValueAppender<tfp_pMembers...>::fs_Append(_Operation, _Table, fg_Forward<tfp_CValues>(p_Values)...);
		}
	};

	template <umint tf_iValue, typename tf_COperation, typename tf_CColumn, typename tf_CValue, typename ...tfp_CValues>
	void fg_SqlAppendPreparedInsertValueAt(tf_COperation &_Operation, tf_CColumn const &_Column, tf_CValue &&_Value, tfp_CValues &&...p_Values)
	{
		if constexpr (tf_iValue == 0)
		{
			using CMember = typename NTraits::TCRemoveReferenceAndQualifiers<tf_CColumn>::CMember;
			_Operation.m_Values.f_InsertLast({.m_ColumnName = _Column.f_Name(), .m_Value = fg_SqlCoercedValue<CMember>(fg_Forward<tf_CValue>(_Value))});
		}
		else
			fg_SqlAppendPreparedInsertValueAt<tf_iValue - 1>(_Operation, _Column, fg_Forward<tfp_CValues>(p_Values)...);
	}

	template <umint tf_iValue, typename tf_COperation, typename tf_CColumn>
	bool fg_SqlAppendTableInsertValueAt(tf_COperation &, tf_CColumn const &, umint)
	{
		return false;
	}

	template <umint tf_iValue, typename tf_COperation, typename tf_CColumn, typename tf_CValue, typename ...tfp_CValues>
	bool fg_SqlAppendTableInsertValueAt(tf_COperation &_Operation, tf_CColumn const &_Column, umint _iValue, tf_CValue &&_Value, tfp_CValues &&...p_Values)
	{
		if (_iValue == tf_iValue)
		{
			using CMember = typename NTraits::TCRemoveReferenceAndQualifiers<tf_CColumn>::CMember;
			_Operation.m_Values.f_InsertLast({.m_ColumnName = _Column.f_Name(), .m_Value = fg_SqlCoercedValue<CMember>(fg_Forward<tf_CValue>(_Value))});
			return true;
		}

		return fg_SqlAppendTableInsertValueAt<tf_iValue + 1>(_Operation, _Column, _iValue, fg_Forward<tfp_CValues>(p_Values)...);
	}

	template <typename tf_CTable, umint tf_iColumn, umint tf_iValue, typename tf_COperation, typename ...tfp_CValues>
	void fg_SqlAppendTableInsertColumns(tf_COperation &_Operation, tf_CTable const &_Table, tfp_CValues &&...p_Values)
	{
		if constexpr (tf_iValue != sizeof...(tfp_CValues))
		{
			auto const &Column = std::get<tf_iColumn>(_Table.m_Columns.m_Columns);
			fg_SqlAppendPreparedInsertValueAt<tf_iValue>(_Operation, Column, fg_Forward<tfp_CValues>(p_Values)...);
			fg_SqlAppendTableInsertColumns<tf_CTable, tf_iColumn + 1, tf_iValue + 1>(_Operation, _Table, fg_Forward<tfp_CValues>(p_Values)...);
		}
	}

	template <auto &tf_Table, umint tf_iColumn, umint tf_iValue, typename tf_COperation, typename ...tfp_CValues>
	void fg_SqlAppendImplicitInsertColumns(tf_COperation &_Operation, tfp_CValues &&...p_Values)
	{
		if constexpr (tf_iColumn != NTraits::TCDecay<decltype(tf_Table)>::mc_nColumns)
		{
			constexpr auto const &Column = std::get<tf_iColumn>(tf_Table.m_Columns.m_Columns);
			// Bind values only to the implicit columns - skip the autoincrement primary key and any generated column,
			// wherever they appear - so a value is never mapped onto a column the database fills itself.
			if constexpr (fg_SqlPreparedInsertColumnIsImplicitlySelected(Column) && tf_iValue != sizeof...(tfp_CValues))
			{
				fg_SqlAppendPreparedInsertValueAt<tf_iValue>(_Operation, Column, fg_Forward<tfp_CValues>(p_Values)...);
				fg_SqlAppendImplicitInsertColumns<tf_Table, tf_iColumn + 1, tf_iValue + 1>(_Operation, fg_Forward<tfp_CValues>(p_Values)...);
			}
			else
				fg_SqlAppendImplicitInsertColumns<tf_Table, tf_iColumn + 1, tf_iValue>(_Operation, fg_Forward<tfp_CValues>(p_Values)...);
		}
	}

	template <auto &tf_PreparedInsert, umint tf_iColumn, umint tf_iValue, typename ...tfp_CValues>
	void fg_SqlAppendPreparedInsertColumns(CSqlInsertOperation &_Operation, tfp_CValues &&...p_Values)
	{
		using CPreparedInsert = NTraits::TCDecay<decltype(tf_PreparedInsert)>;
		if constexpr (tf_iColumn == CPreparedInsert::CTable::mc_nColumns)
			static_assert(tf_iValue == sizeof...(tfp_CValues), "Prepared insert value count must match selected columns");
		else
		{
			constexpr auto const &Column = std::get<tf_iColumn>(tf_PreparedInsert.m_Table.m_Columns.m_Columns);
			if constexpr (fg_SqlPreparedInsertColumnIsSelected<NTraits::TCRemoveReferenceAndQualifiers<decltype(Column)>>(Column))
			{
				fg_SqlAppendPreparedInsertValueAt<tf_iValue>(_Operation, Column, fg_Forward<tfp_CValues>(p_Values)...);
				fg_SqlAppendPreparedInsertColumns<tf_PreparedInsert, tf_iColumn + 1, tf_iValue + 1>(_Operation, fg_Forward<tfp_CValues>(p_Values)...);
			}
			else
				fg_SqlAppendPreparedInsertColumns<tf_PreparedInsert, tf_iColumn + 1, tf_iValue>(_Operation, fg_Forward<tfp_CValues>(p_Values)...);
		}
	}

	template <auto tf_pMember, typename tf_CTable, typename tf_CRow>
	void fg_SqlAppendInsertRowMemberValue(CSqlInsertOperation &_Operation, tf_CTable const &_Table, tf_CRow &&_Row)
	{
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == tf_pMember; })
							{
								if (p_Columns.m_pMember == tf_pMember)
									fg_SqlAppendInsertColumnValue(_Operation, p_Columns, fg_Forward<tf_CRow>(_Row));
							}
						}
						()
						, ...
					);
				}
			)
		;
	}

	template <auto &tf_PreparedInsert, typename tf_CRow>
	void fg_SqlAppendPreparedInsertRow(CSqlInsertOperation &_Operation, tf_CRow &&_Row)
	{
		using CPreparedInsert = NTraits::TCDecay<decltype(tf_PreparedInsert)>;
		// Bind exactly the columns f_Describe puts in the INSERT statement, in the same order as the generated
		// placeholders. An explicit .f_Columns<...>() subset is bound in the requested pack order - which f_Describe
		// also uses - so a reordered .f_Columns<&Row::m_B, &Row::m_A>() binds B then A, not table order. With no
		// members the bind falls back to the implicit insert set in table-declaration order (which f_Describe likewise
		// uses), excluding the columns the database fills.
		[&]<auto ...tfp_pMembers>(TCSqlPreparedInsert<typename CPreparedInsert::CTable, tfp_pMembers...> const *)
		{
			if constexpr (sizeof...(tfp_pMembers) > 0)
			{
				(
					fg_SqlAppendInsertRowMemberValue<tfp_pMembers>(_Operation, tf_PreparedInsert.m_Table, fg_Forward<tf_CRow>(_Row))
					, ...
				);
			}
			else
			{
				tf_PreparedInsert.m_Table.f_ForEachColumn
					(
						[&](auto const &...p_Columns)
						{
							(
								[&]
								{
									if (!fg_SqlPreparedInsertColumnIsImplicitlySelected(p_Columns))
										return;

									fg_SqlAppendInsertColumnValue(_Operation, p_Columns, fg_Forward<tf_CRow>(_Row));
								}
								()
								, ...
							);
						}
					)
				;
			}
		}
		(&tf_PreparedInsert);
	}

	template <auto &tf_PreparedInsert, typename tf_CValue>
	void fg_SqlAppendPreparedInsertImplicit(CSqlInsertOperation &_Operation, tf_CValue &&_Value)
	{
		if constexpr (fg_SqlPreparedInsertImplicitRowValueMatches<tf_PreparedInsert, tf_CValue>())
			fg_SqlAppendPreparedInsertRow<tf_PreparedInsert>(_Operation, fg_Forward<tf_CValue>(_Value));
		else
			fg_SqlAppendPreparedInsertColumns<tf_PreparedInsert, 0, 0>(_Operation, fg_Forward<tf_CValue>(_Value));
	}

	template <auto &tf_PreparedInsert, typename tf_CValue0, typename tf_CValue1, typename ...tfp_CValues>
	void fg_SqlAppendPreparedInsertImplicit(CSqlInsertOperation &_Operation, tf_CValue0 &&_Value0, tf_CValue1 &&_Value1, tfp_CValues &&...p_Values)
	{
		fg_SqlAppendPreparedInsertColumns<tf_PreparedInsert, 0, 0>(_Operation, fg_Forward<tf_CValue0>(_Value0), fg_Forward<tf_CValue1>(_Value1), fg_Forward<tfp_CValues>(p_Values)...);
	}

	template <typename tf_CTable>
	CSqlInsertOperation fg_SqlInsertOperation(tf_CTable const &_Table, typename tf_CTable::CRow &&_Row)
	{
		CSqlInsertOperation Operation;
		Operation.m_TableName = _Table.f_Name();
		_Table.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							fg_SqlAppendInsertValue(Operation, p_Columns, fg_Move(_Row));
						}
						()
						, ...
					);
				}
			)
		;

		return Operation;
	}

	template <typename tf_CTable, typename ...tfp_CValues>
	CSqlInsertOperation fg_SqlInsertOperation(tf_CTable const &_Table, tfp_CValues &&...p_Values)
		requires (fg_SqlTableInsertValuesMatch<tf_CTable, tfp_CValues...>())
	{
		CSqlInsertOperation Operation;
		Operation.m_TableName = _Table.f_Name();
		if constexpr (fg_SqlTableInsertValuesMatchFrom<tf_CTable, 0, tfp_CValues...>())
			fg_SqlAppendTableInsertColumns<tf_CTable, 0, 0>(Operation, _Table, fg_Forward<tfp_CValues>(p_Values)...);
		else
			fg_SqlAppendTableInsertColumns<tf_CTable, 1, 0>(Operation, _Table, fg_Forward<tfp_CValues>(p_Values)...);

		return Operation;
	}

	template <auto &tf_PreparedInsert>
	CSqlInsertOperation fg_SqlInsertOperationFromRow(typename NTraits::TCDecay<decltype(tf_PreparedInsert)>::CRow _Row)
	{
		CSqlInsertOperation Operation;
		Operation.m_TableName = tf_PreparedInsert.m_Table.f_Name();
		Operation.m_pDescription = &gc_SqlInsertOperationDescription<tf_PreparedInsert>;
		fg_SqlAppendPreparedInsertRow<tf_PreparedInsert>(Operation, fg_Move(_Row));

		return Operation;
	}

	template <auto &tf_PreparedInsert, typename ...tfp_CValues>
	CSqlInsertOperation fg_SqlInsertOperation(tfp_CValues &&...p_Values)
	requires (fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>())
	{
		using CPreparedInsert = NTraits::TCDecay<decltype(tf_PreparedInsert)>;

		CSqlInsertOperation Operation;
		Operation.m_TableName = tf_PreparedInsert.m_Table.f_Name();
		Operation.m_pDescription = &gc_SqlInsertOperationDescription<tf_PreparedInsert>;
		[]<auto ...tfp_pMembers, typename ...tfp_CCallValues>
		(
			CSqlInsertOperation &o_Operation
			, TCSqlPreparedInsert<typename CPreparedInsert::CTable, tfp_pMembers...> const *
			, tfp_CCallValues &&...p_CallValues
		)
		{
			if constexpr (sizeof...(tfp_pMembers) != 0)
				TCSqlPreparedInsertValueAppender<tfp_pMembers...>::fs_Append(o_Operation, tf_PreparedInsert.m_Table, fg_Forward<tfp_CCallValues>(p_CallValues)...);
			else
				fg_SqlAppendPreparedInsertImplicit<tf_PreparedInsert>(o_Operation, fg_Forward<tfp_CCallValues>(p_CallValues)...);
		}
		(Operation, &tf_PreparedInsert, fg_Forward<tfp_CValues>(p_Values)...);

		return Operation;
	}

	template <auto tf_pMember, auto &tf_PreparedInsert, typename ...tfp_CValues>
	CSqlInsertOperation fg_SqlInsertReturningOperation(tfp_CValues &&...p_Values)
	requires
	(
		fg_SqlPreparedInsertValuesMatch<tf_PreparedInsert, tfp_CValues...>()
		&& fg_SqlPreparedInsertGeneratedPrimaryKeyColumnCount<tf_PreparedInsert>() == 1
		&& fg_SqlTableMemberIsGeneratedPrimaryKey<tf_PreparedInsert.m_Table, tf_pMember>()
	)
	{
		using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;
		using CStoredMember = NStorage::TCOptionalType<CMember>;
		using CTraits = TCSqlTypeTraits<CStoredMember>;

		auto Operation = fg_SqlInsertOperation<tf_PreparedInsert>(fg_Forward<tfp_CValues>(p_Values)...);
		Operation.m_ReturningColumnName = fg_SqlColumnNameForMember(tf_PreparedInsert.m_Table, tf_pMember);
		Operation.m_ReturningValueType = CTraits::mc_ValueType;
		Operation.m_bReturning = true;

		return Operation;
	}

	template <auto tf_pMember, typename tf_CTable>
	auto fg_SqlReturningMemberValue(CSqlValue &&_Value, tf_CTable const &_Table)
		-> NConcurrency::TCWrapped<typename TCSqlMemberPointerTraits<tf_pMember>::CMember>
	;

	template <auto tf_pMember, auto &tf_PreparedInsert>
	auto fg_SqlInsertReturningMemberValue(CSqlValue &&_Value)
		-> NConcurrency::TCWrapped<typename TCSqlMemberPointerTraits<tf_pMember>::CMember>
	{
		// Share the optional/null-aware conversion with the other RETURNING paths so an inserted NULL into a TCOptional
		// member comes back as an empty optional rather than a conversion error.
		return fg_SqlReturningMemberValue<tf_pMember>(fg_Move(_Value), tf_PreparedInsert.m_Table);
	}

	template <auto tf_pMember, auto &tf_PreparedInsert, typename tf_CActor>
	auto fg_SqlInsertReturning(NConcurrency::TCActor<tf_CActor> _Actor, CSqlInsertOperation _Operation)
		-> NConcurrency::TCFuture<typename TCSqlMemberPointerTraits<tf_pMember>::CMember>
	{
		CSqlValue Value = co_await _Actor(&tf_CActor::f_InsertReturning, fg_Move(_Operation));

		co_return fg_SqlInsertReturningMemberValue<tf_pMember, tf_PreparedInsert>(fg_Move(Value));
	}

	template <auto &tf_Table, auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSetMembers, typename tf_CActor>
	auto fg_SqlSave(NConcurrency::TCActor<tf_CActor> _Actor, typename NTraits::TCDecay<decltype(tf_Table)>::CRow _Row)
		-> NConcurrency::TCFuture<TCSqlSaveResult<typename NTraits::TCDecay<decltype(tf_Table)>::CRow>>
	{
		using CTable = NTraits::TCDecay<decltype(tf_Table)>;
		using CRow = typename CTable::CRow;
		using CID = typename TCSqlMemberPointerTraits<tf_pIDMember>::CMember;
		using CVersion = typename TCSqlMemberPointerTraits<tf_pVersionMember>::CMember;

		if (_Row.*tf_pIDMember == CID{})
		{
			_Row.*tf_pVersionMember = CVersion(1);
			_Row.*tf_pIDMember = co_await fg_SqlInsertReturning<tf_pIDMember, gc_SqlSaveInsert<tf_Table>>
				(
					_Actor
					, fg_SqlInsertReturningOperation<tf_pIDMember, gc_SqlSaveInsert<tf_Table>>(CRow(_Row))
				)
			;

			co_return TCSqlSaveResult<CRow>{.m_Result = ESqlSaveResult::mc_Inserted, .m_Row = fg_Move(_Row)};
		}

		CVersion OldVersion = _Row.*tf_pVersionMember;
		CVersion NewVersion = OldVersion + CVersion(1);
		umint nUpdated = co_await _Actor
			(
				&tf_CActor::f_Update
				, fg_SqlUpdateOperation<gc_SqlSaveUpdate<tf_Table, tf_pIDMember, tf_pVersionMember, tfp_pSetMembers...>>
					(
						_Row.*tfp_pSetMembers...
						, NewVersion
						, _Row.*tf_pIDMember
						, OldVersion
					)
			)
		;

		if (nUpdated == 0)
			co_return TCSqlSaveResult<CRow>{.m_Result = ESqlSaveResult::mc_StaleOrMissing, .m_Row = fg_Move(_Row)};

		if (nUpdated != 1)
			co_return DMibErrorDatabaseInstance("SQL save by ID updated more than one row");

		_Row.*tf_pVersionMember = NewVersion;

		co_return TCSqlSaveResult<CRow>{.m_Result = ESqlSaveResult::mc_Updated, .m_Row = fg_Move(_Row)};
	}

	template <auto &tf_PreparedUpdate, typename ...tfp_CValues>
	CSqlUpdateOperation fg_SqlUpdateOperation(tfp_CValues &&...p_Values)
	{
		using CPreparedUpdate = NTraits::TCDecay<decltype(tf_PreparedUpdate)>;
		using CSetTypes = typename TCSqlTypeListForMemberList<typename TCSqlSetParameterMembers<typename CPreparedUpdate::CSet>::CType>::CType;
		using CPredicateTypes = typename TCSqlPredicateParameterTypes<typename CPreparedUpdate::CPredicate>::CType;
		using CParameterTypes = typename TCSqlTypeListConcat<CSetTypes, CPredicateTypes>::CType;

		CSqlUpdateOperation Operation;
		Operation.m_pDescription = &gc_SqlUpdateOperationDescription<tf_PreparedUpdate>;
		TCSqlMutationTypeValueAppender<CParameterTypes>::fs_Append(Operation, fg_Forward<tfp_CValues>(p_Values)...);

		return Operation;
	}

	template <auto &tf_PreparedDelete, typename ...tfp_CValues>
	CSqlDeleteOperation fg_SqlDeleteOperation(tfp_CValues &&...p_Values)
	{
		using CPreparedDelete = NTraits::TCDecay<decltype(tf_PreparedDelete)>;
		using CParameterTypes = typename TCSqlPredicateParameterTypes<typename CPreparedDelete::CPredicate>::CType;

		CSqlDeleteOperation Operation;
		Operation.m_pDescription = &gc_SqlDeleteOperationDescription<tf_PreparedDelete>;
		TCSqlMutationTypeValueAppender<CParameterTypes>::fs_Append(Operation, fg_Forward<tfp_CValues>(p_Values)...);

		return Operation;
	}

	template <auto &tf_PreparedUpsert, typename ...tfp_CValues>
	CSqlUpsertOperation fg_SqlUpsertOperation(tfp_CValues &&...p_Values)
	{
		CSqlUpsertOperation Operation;
		Operation.m_pDescription = &gc_SqlUpsertOperationDescription<tf_PreparedUpsert>;
		// The upsert binds values to the implicit insert columns (the same set f_Describe builds the INSERT list from),
		// so bind through the implicit appender, which skips the autoincrement primary key and any generated column.
		fg_SqlAppendImplicitInsertColumns<tf_PreparedUpsert.m_Table, 0, 0>(Operation, fg_Forward<tfp_CValues>(p_Values)...);

		return Operation;
	}

	template <auto tf_pMember, typename tf_COperation, typename tf_CTable>
	void fg_SqlApplyReturning(tf_COperation &o_Operation, tf_CTable const &_Table)
	{
		using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;
		using CStoredMember = NStorage::TCOptionalType<CMember>;
		using CTraits = TCSqlTypeTraits<CStoredMember>;

		o_Operation.m_ReturningColumnName = fg_SqlColumnNameForMember(_Table, tf_pMember);
		o_Operation.m_ReturningValueType = CTraits::mc_ValueType;
		o_Operation.m_bReturning = true;
	}

	template <auto tf_pMember, auto &tf_PreparedUpdate, typename ...tfp_CValues>
	CSqlUpdateOperation fg_SqlUpdateReturningOperation(tfp_CValues &&...p_Values)
		requires (fg_SqlPreparedUpdateValuesMatch<tf_PreparedUpdate, tfp_CValues...>())
	{
		auto Operation = fg_SqlUpdateOperation<tf_PreparedUpdate>(fg_Forward<tfp_CValues>(p_Values)...);
		fg_SqlApplyReturning<tf_pMember>(Operation, tf_PreparedUpdate.m_Table);

		return Operation;
	}

	template <auto tf_pMember, auto &tf_PreparedDelete, typename ...tfp_CValues>
	CSqlDeleteOperation fg_SqlDeleteReturningOperation(tfp_CValues &&...p_Values)
		requires (fg_SqlPreparedDeleteValuesMatch<tf_PreparedDelete, tfp_CValues...>())
	{
		auto Operation = fg_SqlDeleteOperation<tf_PreparedDelete>(fg_Forward<tfp_CValues>(p_Values)...);
		fg_SqlApplyReturning<tf_pMember>(Operation, tf_PreparedDelete.m_Table);

		return Operation;
	}

	template <auto tf_pMember, auto &tf_PreparedUpsert, typename ...tfp_CValues>
	CSqlUpsertOperation fg_SqlUpsertReturningOperation(tfp_CValues &&...p_Values)
		requires (fg_SqlPreparedUpsertValuesMatch<tf_PreparedUpsert, tfp_CValues...>())
	{
		auto Operation = fg_SqlUpsertOperation<tf_PreparedUpsert>(fg_Forward<tfp_CValues>(p_Values)...);
		fg_SqlApplyReturning<tf_pMember>(Operation, tf_PreparedUpsert.m_Table);

		return Operation;
	}

	template <auto tf_pMember, typename tf_CTable>
	auto fg_SqlReturningMemberValue(CSqlValue &&_Value, tf_CTable const &_Table)
		-> NConcurrency::TCWrapped<typename TCSqlMemberPointerTraits<tf_pMember>::CMember>
	{
		using CMember = typename TCSqlMemberPointerTraits<tf_pMember>::CMember;

		// Mirror the optional/null handling used by row mapping: a TCOptional member must accept SQL NULL as an empty
		// optional rather than forwarding NULL to the underlying storage converter, which would reject it as a type
		// mismatch and surface a spurious conversion error. A non-optional member returning NULL is a genuine error.
		if constexpr (NStorage::cIsOptional<CMember>)
		{
			if (_Value.f_GetTypeID() == ESqlValueType::mc_Null)
				return CMember();

			auto WrappedValue = fg_SqlMappedValueFromValue<NStorage::TCOptionalType<CMember>>(fg_Move(_Value), fg_SqlColumnNameForMember(_Table, tf_pMember));
			if (!WrappedValue)
				return fg_Move(WrappedValue).f_GetException();

			return CMember(fg_Move(*WrappedValue));
		}
		else
		{
			if (_Value.f_GetTypeID() == ESqlValueType::mc_Null)
				return fg_SqlConversionError(fg_SqlColumnNameForMember(_Table, tf_pMember), "NULL cannot be assigned to non-nullable field");

			return fg_SqlMappedValueFromValue<CMember>(fg_Move(_Value), fg_SqlColumnNameForMember(_Table, tf_pMember));
		}
	}

	template <auto tf_pMember, auto &tf_PreparedMutation, typename tf_CActor, typename tf_COperation, typename tf_CFunction>
	auto fg_SqlMutationReturning(NConcurrency::TCActor<tf_CActor> _Actor, tf_CFunction _fFunction, tf_COperation _Operation)
		-> NConcurrency::TCFuture<typename TCSqlMemberPointerTraits<tf_pMember>::CMember>
	{
		CSqlValue Value = co_await _Actor(_fFunction, fg_Move(_Operation));

		co_return fg_SqlReturningMemberValue<tf_pMember>(fg_Move(Value), tf_PreparedMutation.m_Table);
	}

	template <auto &tf_PreparedSelect, typename tf_CParam>
	CSqlSelectOperation fg_SqlSelectOperation(tf_CParam _Param, CSqlSelectSettings _Settings)
		requires (fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	{
		using CPreparedSelect = NTraits::TCDecay<decltype(tf_PreparedSelect)>;
		using CParameterTypeList = typename TCSqlPreparedSelectParameterTypesList<CPreparedSelect>::CType;
		using CParameterTypes = typename TCSqlParameterTypesForTypeList<CParameterTypeList>::CType;
		constexpr CSqlParameterTypesDescription c_ParameterTypes = CParameterTypes::fs_Describe();
		static constexpr CSqlSelectOperationDescription c_Description =
			{
				.m_pStatement = &tf_PreparedSelect
				, .m_QueryID = fg_SqlMixQueryIDs(tf_PreparedSelect.m_QueryID, c_ParameterTypes.m_QueryID)
				, .m_ParameterTypes = c_ParameterTypes
			}
		;

		CSqlSelectOperation Operation;
		Operation.m_pDescription = &c_Description;
		TCSqlSelectTypeParameterAppender<CParameterTypeList>::fs_Append(Operation, fg_Move(_Param));
		Operation.m_nRowsPerBatch = _Settings.m_nRowsPerBatch;
		Operation.m_nResultRowLimit = _Settings.m_nResultRowLimit;
		Operation.m_nResultRowOffset = _Settings.m_nResultRowOffset;

		return Operation;
	}

	template <auto &tf_PreparedSelect, typename tf_CParam>
	CSqlSelectOperation fg_SqlSelectOperation(CSqlSelectSettings _Settings, tf_CParam _Param)
		requires (fg_SqlPreparedSelectParameterMatches<tf_PreparedSelect, tf_CParam>())
	{
		return fg_SqlSelectOperation<tf_PreparedSelect>(fg_Move(_Param), _Settings);
	}

	template <auto &tf_PreparedSelect>
	CSqlSelectOperation fg_SqlSelectOperation(CSqlSelectSettings _Settings)
		requires (fg_SqlPreparedSelectHasNoParameters<tf_PreparedSelect>())
	{
		constexpr CSqlParameterTypesDescription c_ParameterTypes =
			{
				.m_QueryID = {14695981039346656037ull}
				, .m_pTypes = nullptr
				, .m_nTypes = 0
			}
		;
		static constexpr CSqlSelectOperationDescription c_Description =
			{
				.m_pStatement = &tf_PreparedSelect
				, .m_QueryID = fg_SqlMixQueryIDs(tf_PreparedSelect.m_QueryID, c_ParameterTypes.m_QueryID)
				, .m_ParameterTypes = c_ParameterTypes
			}
		;

		CSqlSelectOperation Operation;
		Operation.m_pDescription = &c_Description;
		Operation.m_nRowsPerBatch = _Settings.m_nRowsPerBatch;
		Operation.m_nResultRowLimit = _Settings.m_nResultRowLimit;
		Operation.m_nResultRowOffset = _Settings.m_nResultRowOffset;

		return Operation;
	}

	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	CSqlSelectOperation fg_SqlSelectOperation(CSqlSelectSettings _Settings, tfp_CParams ...p_Params)
		requires (fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>() && sizeof...(tfp_CParams) > 1)
	{
		using CPreparedSelect = NTraits::TCDecay<decltype(tf_PreparedSelect)>;
		using CParameterTypeList = typename TCSqlPreparedSelectParameterTypesList<CPreparedSelect>::CType;
		using CParameterTypes = typename TCSqlParameterTypesForTypeList<CParameterTypeList>::CType;
		constexpr CSqlParameterTypesDescription c_ParameterTypes = CParameterTypes::fs_Describe();
		static constexpr CSqlSelectOperationDescription c_Description =
			{
				.m_pStatement = &tf_PreparedSelect
				, .m_QueryID = fg_SqlMixQueryIDs(tf_PreparedSelect.m_QueryID, c_ParameterTypes.m_QueryID)
				, .m_ParameterTypes = c_ParameterTypes
			}
		;

		CSqlSelectOperation Operation;
		Operation.m_pDescription = &c_Description;
		TCSqlSelectTypeParameterAppender<CParameterTypeList>::fs_Append(Operation, fg_Move(p_Params)...);
		Operation.m_nRowsPerBatch = _Settings.m_nRowsPerBatch;
		Operation.m_nResultRowLimit = _Settings.m_nResultRowLimit;
		Operation.m_nResultRowOffset = _Settings.m_nResultRowOffset;

		return Operation;
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	CSqlSelectOperation fg_SqlProjectionSelectOperation(CSqlSelectSettings _Settings, tfp_CParams ...p_Params)
		requires (fg_SqlPreparedSelectParametersMatch<tf_PreparedSelect, tfp_CParams...>())
	{
		using CPreparedSelect = NTraits::TCDecay<decltype(tf_PreparedSelect)>;
		using CParameterTypeList = typename TCSqlPreparedSelectParameterTypesList<CPreparedSelect>::CType;
		using CParameterTypes = typename TCSqlParameterTypesForTypeList<CParameterTypeList>::CType;
		using CProjectionRow = TCSqlProjectionBackendRow<tf_CResult, tfp_pMembers...>;
		static constexpr TCSqlSelectProjection<tf_PreparedSelect, CProjectionRow, tfp_pMembers...> c_Projection;
		constexpr CSqlParameterTypesDescription c_ParameterTypes = CParameterTypes::fs_Describe();
		static constexpr CSqlSelectOperationDescription c_Description =
			{
				.m_pStatement = &c_Projection
				, .m_QueryID = fg_SqlMixQueryIDs(c_Projection.m_QueryID, c_ParameterTypes.m_QueryID)
				, .m_ParameterTypes = c_ParameterTypes
			}
		;

		CSqlSelectOperation Operation;
		Operation.m_pDescription = &c_Description;
		TCSqlSelectTypeParameterAppender<CParameterTypeList>::fs_Append(Operation, fg_Move(p_Params)...);
		Operation.m_nRowsPerBatch = _Settings.m_nRowsPerBatch;
		Operation.m_nResultRowLimit = _Settings.m_nResultRowLimit;
		Operation.m_nResultRowOffset = _Settings.m_nResultRowOffset;

		return Operation;
	}

	// f_QueryOptional/f_QueryOne only need to know whether more than one row matches, and their streaming consumer
	// stops after the second row. Cap the SQL to two rows when the select declares a trailing LIMIT so the database
	// stops early too; compound selects (unions etc.) have no f_WithLimit(), so run them uncapped - the consumer
	// still stops after the second row.
	template <auto &tf_PreparedSelect, typename ...tfp_CParams>
	CSqlSelectOperation fg_SqlTwoRowSelectOperation(tfp_CParams ...p_Params)
	{
		if constexpr (requires { tf_PreparedSelect.f_WithLimit(); })
		{
			auto const &c_WithLimit = gc_SqlPreparedSelectWithLimit<tf_PreparedSelect>;
			return fg_SqlSelectOperation<c_WithLimit>(CSqlSelectSettings{.m_nResultRowLimit = 2}, fg_Move(p_Params)...);
		}
		else
			return fg_SqlSelectOperation<tf_PreparedSelect>(CSqlSelectSettings{}, fg_Move(p_Params)...);
	}

	template <auto &tf_PreparedSelect, typename tf_CResult, auto ...tfp_pMembers, typename ...tfp_CParams>
	CSqlSelectOperation fg_SqlTwoRowProjectionSelectOperation(tfp_CParams ...p_Params)
	{
		if constexpr (requires { tf_PreparedSelect.f_WithLimit(); })
		{
			auto const &c_WithLimit = gc_SqlPreparedSelectWithLimit<tf_PreparedSelect>;
			return fg_SqlProjectionSelectOperation<c_WithLimit, tf_CResult, tfp_pMembers...>(CSqlSelectSettings{.m_nResultRowLimit = 2}, fg_Move(p_Params)...);
		}
		else
			return fg_SqlProjectionSelectOperation<tf_PreparedSelect, tf_CResult, tfp_pMembers...>(CSqlSelectSettings{}, fg_Move(p_Params)...);
	}
}
