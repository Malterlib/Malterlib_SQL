// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

/*
SPDX-License-Identifier: PostgreSQL
This code is derived from src/common/saslprep.c, src/common/unicode_norm.c and the UTF-8 helpers of src/common/wchar.c and
src/include/mb/pg_wchar.h of PostgreSQL, with the following copyright:

Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
Portions Copyright (c) 1994, Regents of the University of California
*/

#include "Malterlib_SQL_PostgresClient_Internal.h"

#include <Mib/Memory/Allocators/Secure>

namespace NMib::NSQL::NPrivate::NPostgresSASLprep
{
	// NOLINTBEGIN: the tables are PostgreSQL's, included as they are generated and extracted
	using pg_wchar = uint32;

#include "../../SourceGenerated/PostgreSQL/saslprep_tables.h"
#include "../../SourceGenerated/PostgreSQL/unicode_norm_table.h"
	// NOLINTEND

	using CCodePoints = NContainer::TCVector<uint32, NMemory::CAllocator_HeapSecure>;

	constexpr uint32 gc_SBase = 0xAC00;
	constexpr uint32 gc_LBase = 0x1100;
	constexpr uint32 gc_VBase = 0x1161;
	constexpr uint32 gc_TBase = 0x11A7;
	constexpr uint32 gc_LCount = 19;
	constexpr uint32 gc_VCount = 21;
	constexpr uint32 gc_TCount = 28;
	constexpr uint32 gc_NCount = gc_VCount * gc_TCount;
	constexpr uint32 gc_SCount = gc_LCount * gc_NCount;

	template <umint tf_nValues>
	bool fg_IsCodeInTable(uint32 _Code, uint32 const (&_Map)[tf_nValues])
	{
		static_assert(tf_nValues % 2 == 0);

		if (_Code < _Map[0] || _Code > _Map[tf_nValues - 1])
			return false;

		umint iLow = 0;
		umint iHigh = tf_nValues / 2;
		while (iLow < iHigh)
		{
			umint iRange = (iLow + iHigh) / 2;
			if (_Code < _Map[iRange * 2])
				iHigh = iRange;
			else if (_Code > _Map[iRange * 2 + 1])
				iLow = iRange + 1;
			else
				return true;
		}

		return false;
	}

	umint fg_Utf8Length(uint8 const *_pChar)
	{
		if ((*_pChar & 0x80) == 0)
			return 1;
		else if ((*_pChar & 0xE0) == 0xC0)
			return 2;
		else if ((*_pChar & 0xF0) == 0xE0)
			return 3;
		else if ((*_pChar & 0xF8) == 0xF0)
			return 4;

		return 1;
	}

	bool fg_Utf8IsLegal(uint8 const *_pSource, umint _Length)
	{
		if (_Length < 1 || _Length > 4)
			return false;

		if (_Length >= 4 && (_pSource[3] < 0x80 || _pSource[3] > 0xBF))
			return false;

		if (_Length >= 3 && (_pSource[2] < 0x80 || _pSource[2] > 0xBF))
			return false;

		if (_Length >= 2)
		{
			uint8 Second = _pSource[1];
			switch (_pSource[0])
			{
			case 0xE0:
				if (Second < 0xA0 || Second > 0xBF)
					return false;

				break;
			case 0xED:
				if (Second < 0x80 || Second > 0x9F)
					return false;

				break;
			case 0xF0:
				if (Second < 0x90 || Second > 0xBF)
					return false;

				break;
			case 0xF4:
				if (Second < 0x80 || Second > 0x8F)
					return false;

				break;
			default:
				if (Second < 0x80 || Second > 0xBF)
					return false;

				break;
			}
		}

		if (_pSource[0] >= 0x80 && _pSource[0] < 0xC2)
			return false;

		if (_pSource[0] > 0xF4)
			return false;

		return true;
	}

	uint32 fg_Utf8ToUnicode(uint8 const *_pChar)
	{
		if ((*_pChar & 0x80) == 0)
			return _pChar[0];
		else if ((*_pChar & 0xE0) == 0xC0)
			return (uint32(_pChar[0] & 0x1F) << 6) | (_pChar[1] & 0x3F);
		else if ((*_pChar & 0xF0) == 0xE0)
			return (uint32(_pChar[0] & 0x0F) << 12) | (uint32(_pChar[1] & 0x3F) << 6) | (_pChar[2] & 0x3F);
		else if ((*_pChar & 0xF8) == 0xF0)
			return (uint32(_pChar[0] & 0x07) << 18) | (uint32(_pChar[1] & 0x3F) << 12) | (uint32(_pChar[2] & 0x3F) << 6) | (_pChar[3] & 0x3F);

		return 0xFFFFFFFF;
	}

	void fg_AppendUnicodeAsUtf8(uint32 _Code, NContainer::TCVector<uint8, NMemory::CAllocator_HeapSecure> &o_Utf8)
	{
		if (_Code <= 0x7F)
			o_Utf8.f_InsertLast(uint8(_Code));
		else if (_Code <= 0x7FF)
		{
			o_Utf8.f_InsertLast(uint8(0xC0 | ((_Code >> 6) & 0x1F)));
			o_Utf8.f_InsertLast(uint8(0x80 | (_Code & 0x3F)));
		}
		else if (_Code <= 0xFFFF)
		{
			o_Utf8.f_InsertLast(uint8(0xE0 | ((_Code >> 12) & 0x0F)));
			o_Utf8.f_InsertLast(uint8(0x80 | ((_Code >> 6) & 0x3F)));
			o_Utf8.f_InsertLast(uint8(0x80 | (_Code & 0x3F)));
		}
		else
		{
			o_Utf8.f_InsertLast(uint8(0xF0 | ((_Code >> 18) & 0x07)));
			o_Utf8.f_InsertLast(uint8(0x80 | ((_Code >> 12) & 0x3F)));
			o_Utf8.f_InsertLast(uint8(0x80 | ((_Code >> 6) & 0x3F)));
			o_Utf8.f_InsertLast(uint8(0x80 | (_Code & 0x3F)));
		}
	}

	pg_unicode_decomposition const *fg_GetCodeEntry(uint32 _Code)
	{
		umint iLow = 0;
		umint iHigh = sizeof(UnicodeDecompMain) / sizeof(UnicodeDecompMain[0]);
		while (iLow < iHigh)
		{
			umint iEntry = (iLow + iHigh) / 2;
			if (_Code < UnicodeDecompMain[iEntry].codepoint)
				iHigh = iEntry;
			else if (_Code > UnicodeDecompMain[iEntry].codepoint)
				iLow = iEntry + 1;
			else
				return &UnicodeDecompMain[iEntry];
		}

		return nullptr;
	}

	uint8 fg_GetCanonicalClass(uint32 _Code)
	{
		// Without an entry the character is Hangul or has class 0 and no decomposition
		pg_unicode_decomposition const *pEntry = fg_GetCodeEntry(_Code);
		if (!pEntry)
			return 0;

		return pEntry->comb_class;
	}

	// _pInline holds a decomposition stored inline in the entry, and must outlive the returned pointer
	uint32 const *fg_GetCodeDecomposition(pg_unicode_decomposition const *_pEntry, umint &o_Size, uint32 *_pInline)
	{
		if (DECOMPOSITION_IS_INLINE(_pEntry))
		{
			*_pInline = _pEntry->dec_index;
			o_Size = 1;
			return _pInline;
		}

		o_Size = DECOMPOSITION_SIZE(_pEntry);
		return &UnicodeDecomp_codepoints[_pEntry->dec_index];
	}

	// Hangul syllables are decomposed algorithmically, as Unicode specifies, and are not in the tables
	void fg_DecomposeCode(uint32 _Code, bool _bCompatibility, CCodePoints &o_Result)
	{
		if (_Code >= gc_SBase && _Code < gc_SBase + gc_SCount)
		{
			uint32 SIndex = _Code - gc_SBase;
			uint32 TIndex = SIndex % gc_TCount;

			o_Result.f_InsertLast(gc_LBase + SIndex / (gc_VCount * gc_TCount));
			o_Result.f_InsertLast(gc_VBase + (SIndex % (gc_VCount * gc_TCount)) / gc_TCount);
			if (TIndex != 0)
				o_Result.f_InsertLast(gc_TBase + TIndex);

			return;
		}

		pg_unicode_decomposition const *pEntry = fg_GetCodeEntry(_Code);
		if (!pEntry || DECOMPOSITION_SIZE(pEntry) == 0 || (!_bCompatibility && DECOMPOSITION_IS_COMPAT(pEntry)))
		{
			o_Result.f_InsertLast(_Code);
			return;
		}

		uint32 Inline = 0;
		umint DecompositionSize = 0;
		uint32 const *pDecomposition = fg_GetCodeDecomposition(pEntry, DecompositionSize, &Inline);
		for (umint iCode = 0; iCode < DecompositionSize; ++iCode)
			fg_DecomposeCode(pDecomposition[iCode], _bCompatibility, o_Result);
	}

	bool fg_RecomposeCode(uint32 _Start, uint32 _Code, uint32 &o_Result)
	{
		if (_Start >= gc_LBase && _Start < gc_LBase + gc_LCount && _Code >= gc_VBase && _Code < gc_VBase + gc_VCount)
		{
			o_Result = gc_SBase + ((_Start - gc_LBase) * gc_VCount + (_Code - gc_VBase)) * gc_TCount;
			return true;
		}

		if (_Start >= gc_SBase && _Start < gc_SBase + gc_SCount && ((_Start - gc_SBase) % gc_TCount) == 0 && _Code > gc_TBase && _Code < gc_TBase + gc_TCount)
		{
			o_Result = _Start + (_Code - gc_TBase);
			return true;
		}

		for (auto const &Entry : UnicodeDecompMain)
		{
			if (DECOMPOSITION_SIZE(&Entry) != 2 || DECOMPOSITION_NO_COMPOSE(&Entry))
				continue;

			if (_Start == UnicodeDecomp_codepoints[Entry.dec_index] && _Code == UnicodeDecomp_codepoints[Entry.dec_index + 1])
			{
				o_Result = Entry.codepoint;
				return true;
			}
		}

		return false;
	}

	// Unicode normalization form NFKC, as PostgreSQL's unicode_normalize computes it in frontend code
	CCodePoints fg_NormalizeNFKC(CCodePoints const &_Input)
	{
		CCodePoints Decomposed;
		for (uint32 Code : _Input)
			fg_DecomposeCode(Code, true, Decomposed);

		if (Decomposed.f_IsEmpty())
			return Decomposed;

		// Canonical ordering: an adjacent pair is exchanged when neither is a starter and the first has the greater class
		for (umint iCode = 1; iCode < Decomposed.f_GetLen(); ++iCode)
		{
			uint8 PreviousClass = fg_GetCanonicalClass(Decomposed[iCode - 1]);
			uint8 NextClass = fg_GetCanonicalClass(Decomposed[iCode]);
			if (PreviousClass == 0 || NextClass == 0 || PreviousClass <= NextClass)
				continue;

			fg_Swap(Decomposed[iCode - 1], Decomposed[iCode]);

			if (iCode > 1)
				iCode -= 2;
		}

		CCodePoints Recomposed;
		aint LastClass = -1;
		umint iStarter = 0;
		uint32 Starter = Decomposed[0];
		Recomposed.f_InsertLast(Starter);

		for (umint iCode = 1; iCode < Decomposed.f_GetLen(); ++iCode)
		{
			uint32 Code = Decomposed[iCode];
			aint Class = fg_GetCanonicalClass(Code);
			uint32 Composite = 0;

			if (LastClass < Class && fg_RecomposeCode(Starter, Code, Composite))
			{
				Recomposed[iStarter] = Composite;
				Starter = Composite;
			}
			else if (Class == 0)
			{
				iStarter = Recomposed.f_GetLen();
				Starter = Code;
				LastClass = -1;
				Recomposed.f_InsertLast(Code);
			}
			else
			{
				LastClass = Class;
				Recomposed.f_InsertLast(Code);
			}
		}

		return Recomposed;
	}
}

namespace NMib::NSQL
{
	// PostgreSQL's pg_saslprep variant of SASLprep (RFC 4013), which checks prohibited and bidirectional characters in the mapped
	// input rather than the normalized output, so that the result matches the verifier the server derived
	EPostgresSASLprepResult fg_PostgresSASLprep(NStr::CStrSecure const &_Input, NStr::CStrSecure &o_Output)
	{
		using namespace NPrivate::NPostgresSASLprep;

		o_Output.f_Clear();

		uint8 const *pInput = reinterpret_cast<uint8 const *>(_Input.f_GetStr());
		umint InputLength = umint(_Input.f_GetLen());

		CCodePoints Input;
		for (umint iByte = 0; iByte < InputLength;)
		{
			umint CharLength = fg_Utf8Length(pInput + iByte);
			if (InputLength - iByte < CharLength || !fg_Utf8IsLegal(pInput + iByte, CharLength))
				return EPostgresSASLprepResult::mc_InvalidUTF8;

			Input.f_InsertLast(fg_Utf8ToUnicode(pInput + iByte));
			iByte += CharLength;
		}

		CCodePoints Mapped;
		for (uint32 Code : Input)
		{
			if (fg_IsCodeInTable(Code, non_ascii_space_ranges))
				Mapped.f_InsertLast(0x0020);
			else if (!fg_IsCodeInTable(Code, commonly_mapped_to_nothing_ranges))
				Mapped.f_InsertLast(Code);
		}

		// An input that maps to nothing is prohibited.
		if (Mapped.f_IsEmpty())
			return EPostgresSASLprepResult::mc_Prohibited;

		CCodePoints Normalized = fg_NormalizeNFKC(Mapped);

		for (uint32 Code : Mapped)
		{
			if (fg_IsCodeInTable(Code, prohibited_output_ranges) || fg_IsCodeInTable(Code, unassigned_codepoint_ranges))
				return EPostgresSASLprepResult::mc_Prohibited;
		}

		// With a right-to-left character no left-to-right one is allowed, and the first and last characters must be right-to-left.
		bool bContainsRandALCat = false;
		for (uint32 Code : Mapped)
		{
			if (fg_IsCodeInTable(Code, RandALCat_codepoint_ranges))
			{
				bContainsRandALCat = true;
				break;
			}
		}

		if (bContainsRandALCat)
		{
			for (uint32 Code : Mapped)
			{
				if (fg_IsCodeInTable(Code, LCat_codepoint_ranges))
					return EPostgresSASLprepResult::mc_Prohibited;
			}

			if (!fg_IsCodeInTable(Mapped[0], RandALCat_codepoint_ranges) || !fg_IsCodeInTable(Mapped[Mapped.f_GetLen() - 1], RandALCat_codepoint_ranges))
				return EPostgresSASLprepResult::mc_Prohibited;
		}

		NContainer::TCVector<uint8, NMemory::CAllocator_HeapSecure> Utf8;
		for (uint32 Code : Normalized)
			fg_AppendUnicodeAsUtf8(Code, Utf8);

		o_Output = NStr::CStrSecure(reinterpret_cast<ch8 const *>(Utf8.f_GetArray()), aint(Utf8.f_GetLen()));

		return EPostgresSASLprepResult::mc_Success;
	}
}
