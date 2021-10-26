/**
 * Copyright (c) 2011-2020 Bill Greiman
 * This file is part of the SdFat library for SD memory cards.
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
#define DBG_FILE "ExFatPartition.cpp"
#include "../common/DebugMacros.h"
#include "ExFatVolume.h"
#include "../common/FsStructs.h"

#define SUPPORT_GPT 1
#define SUPPORT_EXTENDED 1
//------------------------------------------------------------------------------
// return 0 if error, 1 if no space, else start cluster.
uint32_t ExFatPartition::bitmapFind(uint32_t cluster, uint32_t count) {
  uint32_t start = cluster ? cluster - 2 : m_bitmapStart;
  if (start >= m_clusterCount) {
    start = 0;
  }
  uint32_t endAlloc = start;
  uint32_t bgnAlloc = start;
  uint16_t sectorSize = 1 << m_bytesPerSectorShift;
  size_t i = (start >> 3) & (sectorSize - 1);
  uint8_t* cache;
  uint8_t mask = 1 << (start & 7);
  while (true) {
    uint32_t sector = m_clusterHeapStartSector +
                     (endAlloc >> (m_bytesPerSectorShift + 3));
    cache = bitmapCacheGet(sector, FsCache::CACHE_FOR_READ);
    if (!cache) {
      return 0;
    }
    for (; i < sectorSize; i++) {
      for (; mask; mask <<= 1) {
        endAlloc++;
        if (!(mask & cache[i])) {
          if ((endAlloc - bgnAlloc) == count) {
            if (cluster == 0 && count == 1) {
              // Start at found sector.  bitmapModify may increase this.
              m_bitmapStart = bgnAlloc;
            }
            return bgnAlloc + 2;
          }
        } else {
          bgnAlloc = endAlloc;
        }
        if (endAlloc == start) {
          return 1;
        }
        if (endAlloc >= m_clusterCount) {
          endAlloc = bgnAlloc = 0;
          i = sectorSize;
          break;
        }
      }
      mask = 1;
    }
    i = 0;
  }
  return 0;
}
//------------------------------------------------------------------------------
bool ExFatPartition::bitmapModify(uint32_t cluster,
                                  uint32_t count, bool value) {
  uint32_t sector;
  uint32_t start = cluster - 2;
  size_t i;
  uint8_t* cache;
  uint8_t mask;
  cluster -= 2;
  if ((start + count) > m_clusterCount) {
    DBG_FAIL_MACRO;
    goto fail;
  }
  if (value) {
    if (start  <= m_bitmapStart && m_bitmapStart < (start + count)) {
      m_bitmapStart = (start + count) < m_clusterCount ? start + count : 0;
    }
  } else {
    if (start < m_bitmapStart) {
      m_bitmapStart = start;
    }
  }
  mask = 1 << (start & 7);
  sector = m_clusterHeapStartSector +
                   (start >> (m_bytesPerSectorShift + 3));
  i = (start >> 3) & m_sectorMask;
  while (true) {
    cache = bitmapCacheGet(sector++, FsCache::CACHE_FOR_WRITE);
    if (!cache) {
      DBG_FAIL_MACRO;
      goto fail;
    }
    for (; i < m_bytesPerSector; i++) {
      for (; mask; mask <<= 1) {
        if (value == static_cast<bool>(cache[i] & mask)) {
          DBG_FAIL_MACRO;
          goto fail;
        }
        cache[i] ^= mask;
        if (--count == 0) {
          return true;
        }
      }
      mask = 1;
    }
    i = 0;
  }

 fail:
  return false;
}
//------------------------------------------------------------------------------
uint32_t ExFatPartition::chainSize(uint32_t cluster) {
  uint32_t n = 0;
  int8_t status;
  do {
    status = fatGet(cluster, & cluster);
    if (status < 0) return 0;
    n++;
  } while (status);
  return n;
}
//------------------------------------------------------------------------------
uint8_t* ExFatPartition::dirCache(DirPos_t* pos, uint8_t options) {
  uint32_t sector = clusterStartSector(pos->cluster);
  sector += (m_clusterMask & pos->position) >> m_bytesPerSectorShift;
  uint8_t* cache = dataCacheGet(sector, options);
  return cache ? cache + (pos->position & m_sectorMask) : nullptr;
}
//------------------------------------------------------------------------------
// return -1 error, 0 EOC, 1 OK
int8_t ExFatPartition::dirSeek(DirPos_t* pos, uint32_t offset) {
  int8_t status;
  uint32_t tmp = (m_clusterMask & pos->position) + offset;
  pos->position += offset;
  tmp >>= bytesPerClusterShift();
  while (tmp--) {
    if (pos->isContiguous) {
      pos->cluster++;
    } else {
      status = fatGet(pos->cluster, &pos->cluster);
      if (status != 1) {
        return status;
      }
    }
  }
  return 1;
}
//------------------------------------------------------------------------------
// return -1 error, 0 EOC, 1 OK
int8_t ExFatPartition::fatGet(uint32_t cluster, uint32_t* value) {
  uint8_t* cache;
  uint32_t next;
  uint32_t sector;

  if (cluster > (m_clusterCount + 1)) {
    DBG_FAIL_MACRO;
    return -1;
  }
  sector = m_fatStartSector + (cluster >> (m_bytesPerSectorShift - 2));

  cache = dataCacheGet(sector, FsCache::CACHE_FOR_READ);
  if (!cache) {
    return -1;
  }
  next = getLe32(cache + ((cluster << 2) & m_sectorMask));
  *value = next;
  return next == EXFAT_EOC ? 0 : 1;
}
//------------------------------------------------------------------------------
bool ExFatPartition::fatPut(uint32_t cluster, uint32_t value) {
  uint32_t sector;
  uint8_t* cache;
  if (cluster < 2 || cluster > (m_clusterCount + 1)) {
    DBG_FAIL_MACRO;
    goto fail;
  }
  sector = m_fatStartSector + (cluster >> (m_bytesPerSectorShift - 2));
  cache = dataCacheGet(sector, FsCache::CACHE_FOR_WRITE);
  if (!cache) {
    DBG_FAIL_MACRO;
    goto fail;
  }
  setLe32(cache + ((cluster << 2) & m_sectorMask), value);
  return true;

 fail:
  return false;
}
//------------------------------------------------------------------------------
bool ExFatPartition::freeChain(uint32_t cluster) {
  uint32_t next;
  uint32_t start = cluster;
  int8_t status;
  do {
    status = fatGet(cluster, &next);
    if (status < 0) {
      DBG_FAIL_MACRO;
      goto fail;
    }
    if (!fatPut(cluster, 0)) {
      DBG_FAIL_MACRO;
      goto fail;
    }
    if (status == 0 || (cluster + 1) != next) {
      if (!bitmapModify(start, cluster - start + 1, 0)) {
        DBG_FAIL_MACRO;
        goto fail;
      }
      start = next;
    }
    cluster = next;
  } while (status);

  return true;

 fail:
  return false;
}
//------------------------------------------------------------------------------
uint32_t ExFatPartition::freeClusterCount() {
  uint32_t nc = 0;
  uint32_t sector = m_clusterHeapStartSector;
  uint32_t usedCount = 0;
  uint8_t* cache;

  while (true) {
    cache = dataCacheGet(sector++, FsCache::CACHE_FOR_READ);
    if (!cache) {
      return 0;
    }
    for (size_t i = 0; i < m_bytesPerSector; i++) {
      if (cache[i] == 0XFF) {
        usedCount+= 8;
      } else if (cache[i]) {
        for (uint8_t mask = 1; mask ; mask <<=1) {
          if ((mask & cache[i])) {
            usedCount++;
          }
        }
      }
      nc += 8;
      if (nc >= m_clusterCount) {
        return m_clusterCount - usedCount;
      }
    }
  }
}
//------------------------------------------------------------------------------
bool ExFatPartition::init(BlockDevice* dev, uint8_t part) {
  Serial.printf("ExFatPartition::init(%x, %u)\n", (uint32_t)dev, part); Serial.flush();
  uint32_t volStart = 0;
  uint8_t* cache;
  pbs_t* pbs;
  BpbExFat_t* bpb;
  MbrSector_t* mbr;
  MbrPart_t* mp;
  #if SUPPORT_GPT 
  GPTPartitionHeader_t* gptph;
  GPTPartitionEntrySector_t *gptes;
  GPTPartitionEntryItem_t *gptei;
  #endif

  m_fatType = 0;
  m_blockDev = dev;
  cacheInit(m_blockDev);
  cache = dataCacheGet(0, FsCache::CACHE_FOR_READ);
  if (!cache) { DBG_FAIL_MACRO; goto fail; }
  Serial.println("    After datacacheGet");
  #if SUPPORT_GPT 
  // Lets see if we are on a GPT disk or not look for GPT guard
  mbr = reinterpret_cast<MbrSector_t*>(cache);
  mp = &mbr->part[0];
  if (mp->type == 0xee) {
    cache = dataCacheGet(1, FsCache::CACHE_FOR_READ);
    gptph = reinterpret_cast<GPTPartitionHeader_t*>(cache);

    // Lets do a little validation of this data.
    if (!cache || (memcmp(gptph->signature, F("EFI PART"), 8) != 0)) {
      DBG_FAIL_MACRO;
      goto fail;
    }
    if (part > getLe32(gptph->numberPartitions)) {
      DBG_FAIL_MACRO;
      goto fail;
    }
    part--; // Make it 0 based... 4 entries per sector...
    cache = dataCacheGet(2 + (part >> 2), FsCache::CACHE_FOR_READ);
    if (!cache) {
      DBG_FAIL_MACRO;
      goto fail;      
    }
    gptes = reinterpret_cast<GPTPartitionEntrySector_t*>(cache);
    gptei = &gptes->items[part & 0x3];

    // Now make sure it is q guid we can handle.  For now only Microsoft Basic Data...
    static const uint8_t microsoft_basic_data_partition_guid[16] PROGMEM = {0xA2, 0xA0, 0xD0, 0xEB, 0xE5, 0xB9, 0x33, 0x44, 0x87, 0xC0, 0x68, 0xB6, 0xB7, 0x26, 0x99, 0xC7};
    if (memcmp((uint8_t *)gptei->partitionTypeGUID, microsoft_basic_data_partition_guid, 16) != 0) {
      DBG_FAIL_MACRO;
      goto fail;      
    }
    volStart = getLe64(gptei->firstLBA);
  } else
  #endif
  {
    // MBR handling...
    if (part >= 1) {
      mbr = reinterpret_cast<MbrSector_t*>(cache);

      #if SUPPORT_EXTENDED
      // Extended support we need to walk through the partitions to see if there is an extended partition
      // that we need to walk into. 
      part--; // zero base it.
      // short cut:
      bool part_index_item_valid = false;
      if (part < 4) {
        // try quick way through
        mp = &mbr->part[part];
        if (((mp->boot == 0) || (mp->boot == 0X80)) && (mp->type != 0) && (mp->type != 0xf)) {
          part_index_item_valid = true;
          volStart = getLe32(mp->relativeSectors);
          Serial.println("    >> Found direct"); Serial.flush();
        }
      }  
      if (! part_index_item_valid) {
        Serial.println("    >> Need to walk list look for Extended type");
        uint8_t index_part;
        for (index_part = 0; index_part < 4; index_part++) {
          mp = &mbr->part[index_part];
          if ((mp->boot != 0 && mp->boot != 0X80) || mp->type == 0 || index_part > part) { DBG_FAIL_MACRO; goto fail; }
          if (mp->type == 0xf) break;
        }

        if (index_part == 4) { DBG_FAIL_MACRO; goto fail; } // no extended partition found. 
        Serial.printf("    Found Extended: %u\n", index_part);

        // Our partition if it exists is in extended partition. 
        uint32_t next_mbr = getLe32(mp->relativeSectors);
        for(;;) {
          Serial.printf("    Index: %u Read Block: %u\n", index_part, next_mbr);
          cache = dataCacheGet(next_mbr, FsCache::CACHE_FOR_READ);
          if (!cache) { Serial.println("    Failed read");DBG_FAIL_MACRO; goto fail; }
          Serial.println("    After Read Block"); Serial.flush();
          mbr = reinterpret_cast<MbrSector_t*>(cache);
          if (index_part == part) break; // should be at that entry
          // else we need to see if it points to others...
          mp = &mbr->part[1];
          volStart = getLe32(mp->relativeSectors);
          Serial.printf("    Check for next: type: %u start:%u\n ", mp->type, volStart);
          if ((mp->type == 5) && volStart) {
            next_mbr = next_mbr + volStart;
            index_part++; 
          } else { DBG_FAIL_MACRO; goto fail; }
        }
        // If we are here than we shoul hopefully be at start of segment...
        mp = &mbr->part[0];
        volStart = getLe32(mp->relativeSectors) + next_mbr;
        Serial.printf("    Exit loop %u\n", volStart);
      }
      #else
      // Simple 
      if (part > 4) {
        DBG_FAIL_MACRO;
        goto fail;
      }
      mp = &mbr->part[part - 1];
      if ((mp->boot != 0 && mp->boot != 0X80) || mp->type == 0) {
        DBG_FAIL_MACRO;
        goto fail;
      }
      volStart = getLe32(mp->relativeSectors);
      #endif
    }
  }

  Serial.printf("    Read in Data sector: %u\n", (uint32_t)volStart);
  cache = dataCacheGet(volStart, FsCache::CACHE_FOR_READ);
  if (!cache) {
    DBG_FAIL_MACRO;
    goto fail;
  }
  pbs = reinterpret_cast<pbs_t*>(cache);
  if (strncmp(pbs->oemName, "EXFAT", 5)) {
    DBG_FAIL_MACRO;
    goto fail;
  }
  bpb = reinterpret_cast<BpbExFat_t*>(pbs->bpb);
  if (bpb->bytesPerSectorShift != m_bytesPerSectorShift) {
    DBG_FAIL_MACRO;
    goto fail;
  }
  m_fatStartSector = volStart + getLe32(bpb->fatOffset);
  m_fatLength = getLe32(bpb->fatLength);
  m_clusterHeapStartSector = volStart + getLe32(bpb->clusterHeapOffset);
  m_clusterCount = getLe32(bpb->clusterCount);
  m_rootDirectoryCluster = getLe32(bpb->rootDirectoryCluster);
  m_sectorsPerClusterShift = bpb->sectorsPerClusterShift;
  m_bytesPerCluster = 1UL << (m_bytesPerSectorShift + m_sectorsPerClusterShift);
  m_clusterMask = m_bytesPerCluster - 1;
  // Set m_bitmapStart to first free cluster.
  m_bitmapStart = 0;
  bitmapFind(0, 1);
  m_fatType = FAT_TYPE_EXFAT;
  Serial.println("    Valid ExFat");
  return true;

 fail:
  Serial.println("    Fail");
  return false;
}
//------------------------------------------------------------------------------
uint32_t ExFatPartition::rootLength() {
  uint32_t nc = chainSize(m_rootDirectoryCluster);
  return nc << bytesPerClusterShift();
}
