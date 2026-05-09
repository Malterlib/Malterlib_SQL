// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLite_Init.h"

#include <Mib/Storage/Aggregate>

#include "../SourceGenerated/SQLite/sqlite3.h"

namespace NMib::NSQL::NPrivate
{
	namespace
	{
		void *fg_SqliteMalloc(int _nBytes)
		{
			if (_nBytes <= 0)
				return nullptr;

			return NMib::NMemory::fg_Alloc(umint(_nBytes));
		}

		void fg_SqliteFree(void *_pMemory)
		{
			if (_pMemory)
				NMib::NMemory::fg_FreeNoSize(_pMemory);
		}

		void *fg_SqliteRealloc(void *_pMemory, int _nBytes)
		{
			if (!_pMemory)
				return fg_SqliteMalloc(_nBytes);

			if (_nBytes <= 0)
			{
				NMib::NMemory::fg_FreeNoSize(_pMemory);

				return nullptr;
			}

			umint NewSize = umint(_nBytes);

			return NMib::NMemory::fg_Resize(_pMemory, NewSize, 0, NMib::EAllocationFlag_SizeNotNeeded);
		}

		int fg_SqliteSize(void *_pMemory)
		{
			return int(NMib::NMemory::fg_Size(_pMemory));
		}

		int fg_SqliteRoundup(int _nBytes)
		{
			if (_nBytes <= 0)
				return 0;

			return int(NMib::NMemory::fg_SizePadded(umint(_nBytes)));
		}

		int fg_SqliteInitAllocator(void *)
		{
			return SQLITE_OK;
		}

		void fg_SqliteShutdownAllocator(void *)
		{
		}

		struct CSqliteInit
		{
			CSqliteInit()
			{
				sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);

				m_MemMethods =
					{
						&fg_SqliteMalloc
						, &fg_SqliteFree
						, &fg_SqliteRealloc
						, &fg_SqliteSize
						, &fg_SqliteRoundup
						, &fg_SqliteInitAllocator
						, &fg_SqliteShutdownAllocator
						, nullptr
					}
				;
				sqlite3_config(SQLITE_CONFIG_MALLOC, &m_MemMethods);
				sqlite3_initialize();
			}

			~CSqliteInit()
			{
				sqlite3_shutdown();
			}

			sqlite3_mem_methods m_MemMethods;
		};

		constinit NStorage::TCAggregate<CSqliteInit> g_SqliteInit = {DAggregateInit};
	}

	void fg_SqliteEnsureInitialized()
	{
		*g_SqliteInit;
	}
}
