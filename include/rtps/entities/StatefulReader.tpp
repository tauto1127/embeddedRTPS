/*
The MIT License
Copyright (c) 2019 Lehrstuhl Informatik 11 - RWTH Aachen University
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE

This file is part of embeddedRTPS.

Author: i11 - Embedded Software, RWTH Aachen University
*/

#include "lwip/sys.h"
#include "lwip/tcpip.h"
#include "rtps/entities/StatefulReader.h"
#include "rtps/messages/MessageFactory.h"
#include "rtps/utils/Lock.h"

#include <cstdio>
#include <cstdlib>

#if SFR_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define SFR_LOG(...)                                            \
  if (true)                                                     \
  {                                                             \
    printf("[StatefulReader %s] ", &m_attributes.topicName[0]); \
    printf(__VA_ARGS__);                                        \
    printf("\n");                                               \
  }
#else
#define SFR_LOG(...) //
#endif

using rtps::StatefulReaderT;

// Rare reliability events (skip, GAP, full buffer) go to stdout so experiments
// can see them without enabling the verbose log.
#ifndef SFR_RELIABILITY_LOG
#define SFR_RELIABILITY_LOG 1
#endif
#if SFR_RELIABILITY_LOG
#define SFR_EVENT(...)                                          \
  do                                                            \
  {                                                             \
    printf("[StatefulReader %s] ", &m_attributes.topicName[0]); \
    printf(__VA_ARGS__);                                        \
    printf("\n");                                               \
  } while (0)
#else
#define SFR_EVENT(...)
#endif

template <class NetworkDriver>
StatefulReaderT<NetworkDriver>::~StatefulReaderT()
{
  //  if(sys_mutex_valid(&m_mutex)){ // Getting invalid pointer error, there
  //  seems sth strange
  //    sys_mutex_free(&m_mutex);
  //  }
  for (auto &slot : m_buffered)
  {
    freeSlot(slot);
  }
}

// NetworkDriverの中にパケット受信しのコールバックが指定されてそう
template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::init(const TopicData &attributes,
                                          NetworkDriver &driver)
{
  if (sys_mutex_new(&m_mutex) != ERR_OK)
  {

    SFR_LOG("StatefulReader: Failed to create mutex.\n");

    return;
  }
  m_attributes = attributes;
  m_transport = &driver;
  m_packetInfo.srcPort = attributes.unicastLocator.port;
  m_is_initialized_ = true;
}

// Fast DDS-like reception: deliver in SN order, keep changes that arrive after
// a missing one, and give up on an SN only when the writer says it is gone
// (HEARTBEAT firstSN or GAP).
template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::newChange(
    const ReaderCacheChange &cacheChange)
{
  if (m_callback == nullptr)
  {
    return;
  }
  Lock lock{m_mutex};
  WriterProxy *proxy = findProxy(cacheChange.writerGuid);
  if (proxy == nullptr)
  {
    return;
  }
  if (cacheChange.sn < proxy->expectedSN)
  {
    return; // already delivered or given up
  }
  if (cacheChange.sn == proxy->expectedSN)
  {
    deliver(cacheChange, *proxy);
    deliverInOrder(*proxy);
    return;
  }
  if (!isBuffered(cacheChange.writerGuid, cacheChange.sn))
  {
    store(cacheChange);
  }
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::registerCallback(ddsReaderCallback_fp cb,
                                                      void *callee)
{
  if (cb != nullptr)
  {
    m_callback = cb;
    m_callee = callee; // It's okay if this is null
  }
  else
  {

    SFR_LOG("Passed callback is nullptr\n");
  }
}

template <class NetworkDriver>
bool StatefulReaderT<NetworkDriver>::addNewMatchedWriter(
    const WriterProxy &newProxy)
{
#if SFR_VERBOSE && RTPS_GLOBAL_VERBOSE
  SFR_LOG("New writer added with id: ");
  printGuid(newProxy.remoteWriterGuid);
  SFR_LOG("\n");
#endif
  return m_proxies.add(newProxy);
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::removeWriter(const Guid_t &guid)
{
  Lock lock(m_mutex);
  auto isElementToRemove = [&](const WriterProxy &proxy)
  {
    return proxy.remoteWriterGuid == guid;
  };
  auto thunk = [](void *arg, const WriterProxy &value)
  {
    return (*static_cast<decltype(isElementToRemove) *>(arg))(value);
  };

  m_proxies.remove(thunk, &isElementToRemove);
  dropBufferedOf(guid);
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::removeWriterOfParticipant(
    const GuidPrefix_t &guidPrefix)
{
  Lock lock(m_mutex);
  auto isElementToRemove = [&](const WriterProxy &proxy)
  {
    return proxy.remoteWriterGuid.prefix == guidPrefix;
  };
  auto thunk = [](void *arg, const WriterProxy &value)
  {
    return (*static_cast<decltype(isElementToRemove) *>(arg))(value);
  };

  m_proxies.remove(thunk, &isElementToRemove);
  dropBufferedOfParticipant(guidPrefix);
}

template <class NetworkDriver>
bool StatefulReaderT<NetworkDriver>::onNewHeartbeat(
    const SubmessageHeartbeat &msg, const GuidPrefix_t &sourceGuidPrefix)
{
  Lock lock(m_mutex);
  PacketInfo info;
  info.srcPort = m_packetInfo.srcPort;
  WriterProxy *writer = findProxy(Guid_t{sourceGuidPrefix, msg.writerId});

  if (writer == nullptr)
  {

#if SFR_VERBOSE && RTPS_GLOBAL_VERBOSE
    SFR_LOG("Ignore heartbeat. Couldn't find a matching "
            "writer with id:");
    printEntityId(msg.writerId);
    SFR_LOG("\n");
#endif
    return false;
  }

  if (msg.count.value <= writer->hbCount.value)
  {

    SFR_LOG("Ignore heartbeat. Count too low.\n");
    return false;
  }

  writer->hbCount.value = msg.count.value;

  // The writer no longer has anything before firstSN.
  if (writer->expectedSN < msg.firstSN)
  {
    SFR_EVENT("heartbeat firstSN %u skips from %u",
              static_cast<unsigned>(msg.firstSN.low),
              static_cast<unsigned>(writer->expectedSN.low));
    skipTo(*writer, msg.firstSN);
  }

  info.destAddr = writer->remoteLocator.getIp4Address();
  info.destPort = writer->remoteLocator.port;
  rtps::MessageFactory::addHeader(info.buffer,
                                  m_attributes.endpointGuid.prefix);
  rtps::MessageFactory::addAckNack(info.buffer, msg.writerId, msg.readerId,
                                   getMissing(*writer, msg.lastSN),
                                   writer->getNextAckNackCount(), false);

  SFR_LOG("Sending acknack.\n");
  m_transport->sendPacket(info);
  return true;
}

template <class NetworkDriver>
bool StatefulReaderT<NetworkDriver>::onNewGap(
    const SubmessageGap &msg, const GuidPrefix_t &sourceGuidPrefix)
{
  Lock lock(m_mutex);
  WriterProxy *writer = findProxy(Guid_t{sourceGuidPrefix, msg.writerId});
  if (writer == nullptr)
  {
    return false;
  }
  SFR_EVENT("gap %u..%u (+%u bits), expected %u",
            static_cast<unsigned>(msg.gapStart.low),
            static_cast<unsigned>(msg.gapList.base.low),
            static_cast<unsigned>(msg.gapList.numBits),
            static_cast<unsigned>(writer->expectedSN.low));
  markIrrelevant(*writer, msg.gapStart, msg.gapList.base);
  for (uint32_t i = 0; i < msg.gapList.numBits; ++i)
  {
    if (msg.gapList.isSet(i))
    {
      const uint64_t sn = snToU64(msg.gapList.base) + i;
      markIrrelevant(*writer, snFromU64(sn), snFromU64(sn + 1));
    }
  }
  deliverInOrder(*writer);
  return true;
}

// ---- helpers (all called with m_mutex held) ----

template <class NetworkDriver>
rtps::WriterProxy *
StatefulReaderT<NetworkDriver>::findProxy(const Guid_t &writerGuid)
{
  for (WriterProxy &proxy : m_proxies)
  {
    if (proxy.remoteWriterGuid == writerGuid)
    {
      return &proxy;
    }
  }
  return nullptr;
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::deliver(const ReaderCacheChange &change,
                                             WriterProxy &proxy)
{
  m_callback(m_callee, change);
  proxy.advanceTo(snFromU64(snToU64(proxy.expectedSN) + 1));
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::deliverBuffered(BufferedChange &slot,
                                                     WriterProxy &proxy)
{
  ReaderCacheChange change{slot.kind,     slot.writerGuid, slot.sn,
                           slot.data,     slot.size,       slot.response,
                           slot.identify};
  deliver(change, proxy);
  freeSlot(slot);
}

template <class NetworkDriver>
typename StatefulReaderT<NetworkDriver>::BufferedChange *
StatefulReaderT<NetworkDriver>::findBuffered(const Guid_t &writerGuid,
                                             const SequenceNumber_t &sn)
{
  for (auto &slot : m_buffered)
  {
    if (slot.used && slot.sn == sn && slot.writerGuid == writerGuid)
    {
      return &slot;
    }
  }
  return nullptr;
}

template <class NetworkDriver>
bool StatefulReaderT<NetworkDriver>::isBuffered(const Guid_t &writerGuid,
                                                const SequenceNumber_t &sn)
{
  return findBuffered(writerGuid, sn) != nullptr;
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::store(const ReaderCacheChange &change)
{
  for (auto &slot : m_buffered)
  {
    if (slot.used)
    {
      continue;
    }
    uint8_t *copy = nullptr;
    if (change.size != 0)
    {
      copy = static_cast<uint8_t *>(malloc(change.size));
      if (copy == nullptr || !change.copyInto(copy, change.size))
      {
        free(copy);
        return;
      }
    }
    slot.used = true;
    slot.writerGuid = change.writerGuid;
    slot.sn = change.sn;
    slot.kind = change.kind;
    slot.response = change.response;
    slot.identify = change.identify;
    slot.size = change.size;
    slot.data = copy;
    return;
  }
  // Full: the change will be requested again by the next ACKNACK.
  SFR_EVENT("buffer full, not keeping sn %u",
            static_cast<unsigned>(change.sn.low));
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::freeSlot(BufferedChange &slot)
{
  if (slot.used)
  {
    free(slot.data);
  }
  slot.used = false;
  slot.data = nullptr;
  slot.size = 0;
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::dropBufferedOf(const Guid_t &writerGuid)
{
  for (auto &slot : m_buffered)
  {
    if (slot.used && slot.writerGuid == writerGuid)
    {
      freeSlot(slot);
    }
  }
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::dropBufferedOfParticipant(
    const GuidPrefix_t &prefix)
{
  for (auto &slot : m_buffered)
  {
    if (slot.used && slot.writerGuid.prefix == prefix)
    {
      freeSlot(slot);
    }
  }
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::deliverInOrder(WriterProxy &proxy)
{
  while (true)
  {
    BufferedChange *slot =
        findBuffered(proxy.remoteWriterGuid, proxy.expectedSN);
    if (slot != nullptr)
    {
      deliverBuffered(*slot, proxy);
      continue;
    }
    if ((proxy.irrelevantMask & 1u) != 0)
    {
      proxy.advanceTo(snFromU64(snToU64(proxy.expectedSN) + 1));
      continue;
    }
    return;
  }
}

// Everything before newExpected is either kept here or will never come:
// deliver what we have in order and move on.
template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::skipTo(WriterProxy &proxy,
                                            const SequenceNumber_t &newExpected)
{
  while (proxy.expectedSN < newExpected)
  {
    BufferedChange *next = nullptr;
    for (auto &slot : m_buffered)
    {
      if (slot.used && slot.writerGuid == proxy.remoteWriterGuid &&
          slot.sn < newExpected && (next == nullptr || slot.sn < next->sn))
      {
        next = &slot;
      }
    }
    if (next == nullptr)
    {
      break;
    }
    proxy.advanceTo(next->sn);
    deliverBuffered(*next, proxy);
  }
  proxy.advanceTo(newExpected);
  deliverInOrder(proxy);
}

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::markIrrelevant(
    WriterProxy &proxy, const SequenceNumber_t &from,
    const SequenceNumber_t &toExclusive)
{
  if (!(from < toExclusive) || !(proxy.expectedSN < toExclusive))
  {
    return;
  }
  if (from <= proxy.expectedSN)
  {
    skipTo(proxy, toExclusive);
    return;
  }
  const uint64_t expected = snToU64(proxy.expectedSN);
  const uint64_t end = snToU64(toExclusive);
  for (uint64_t sn = snToU64(from); sn < end && sn - expected < 32; ++sn)
  {
    proxy.irrelevantMask |= uint32_t{1} << (sn - expected);
  }
}

// base = expectedSN; a bit is set for every SN up to lastAvail (at most 32)
// that is neither kept here nor declared irrelevant.
template <class NetworkDriver>
rtps::SequenceNumberSet
StatefulReaderT<NetworkDriver>::getMissing(WriterProxy &proxy,
                                           const SequenceNumber_t &lastAvail)
{
  SequenceNumberSet set;
  set.base = proxy.expectedSN;
  set.bitMap.fill(0);
  if (lastAvail < proxy.expectedSN)
  {
    set.numBits = 0;
    return set;
  }
  uint64_t count = snToU64(lastAvail) - snToU64(proxy.expectedSN) + 1;
  if (count > SNS_NUM_BITS)
  {
    count = SNS_NUM_BITS;
  }
  set.numBits = static_cast<uint32_t>(count);
  for (uint32_t i = 0; i < set.numBits; ++i)
  {
    const SequenceNumber_t sn = snFromU64(snToU64(proxy.expectedSN) + i);
    if ((proxy.irrelevantMask & (uint32_t{1} << i)) == 0 &&
        !isBuffered(proxy.remoteWriterGuid, sn))
    {
      set.bitMap[0] |= uint32_t{1} << (31 - i);
    }
  }
  return set;
}

#undef SFR_EVENT
#undef SFR_VERBOSE
