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
#include "rtps/entities/StatefulWriter.h"
#include "rtps/messages/MessageFactory.h"
#include "rtps/utils/Log.h"
#include <algorithm>
#include <cstring>
#include <stdio.h>

using rtps::StatefulWriterT;

#if SFW_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define SFW_LOG(...)                                               \
  if (true)                                                        \
  {                                                                \
    printf("[Stateful Writer %s] ", this->m_attributes.topicName); \
    printf(__VA_ARGS__);                                           \
    printf("\n");                                                  \
  }
#else
#define SFW_LOG(...) //
#endif

template <class NetworkDriver>
StatefulWriterT<NetworkDriver>::~StatefulWriterT()
{
  m_running = false;
  while (m_thread_running)
  {
    sys_msleep(500); // Required for tests/ Join currently not available /
    //  increased because Segfault in Tests if(sys_mutex_valid(&m_mutex)){
    //    sys_mutex_free(&m_mutex);
    //  }
  }
}

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::init(TopicData attributes,
                                          TopicKind_t topicKind,
                                          ThreadPool *threadPool,
                                          NetworkDriver &driver,
                                          bool enfUnicast)
{
  if (sys_mutex_new(&m_mutex) != ERR_OK)
  {

    SFW_LOG("Failed to create mutex.\n");

    return false;
  }

  m_transport = &driver;
  m_attributes = attributes;
  m_topicKind = topicKind;
  m_packetInfo.srcPort = attributes.unicastLocator.port;
  m_enforceUnicast = enfUnicast;
  mp_threadPool = threadPool;
  if (m_attributes.endpointGuid.entityId ==
      ENTITYID_SEDP_BUILTIN_PUBLICATIONS_WRITER)
  {
    m_heartbeatThread = sys_thread_new("HBThreadPub", hbFunctionJumppad, this,
                                       Config::HEARTBEAT_STACKSIZE,
                                       Config::THREAD_POOL_WRITER_PRIO);
  }
  else if (m_attributes.endpointGuid.entityId ==
           ENTITYID_SEDP_BUILTIN_SUBSCRIPTIONS_WRITER)
  {
    m_heartbeatThread = sys_thread_new("HBThreadSub", hbFunctionJumppad, this,
                                       Config::HEARTBEAT_STACKSIZE,
                                       Config::THREAD_POOL_WRITER_PRIO);
  }
  else
  {
    m_heartbeatThread = sys_thread_new("HBThread", hbFunctionJumppad, this,
                                       Config::HEARTBEAT_STACKSIZE,
                                       Config::THREAD_POOL_WRITER_PRIO);
  }

  m_is_initialized_ = true;
  return true;
}

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::addNewMatchedReader(
    const ReaderProxy &newProxy)
{
#if SFW_VERBOSE && RTPS_GLOBAL_VERBOSE
  SFW_LOG("New reader added with id: ");
  printGuid(newProxy.remoteReaderGuid);
#endif

  bool success = m_proxies.add(newProxy);
  if (!m_enforceUnicast)
  {
    manageSendOptions();
  }
  return success;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::manageSendOptions()
{

  SFW_LOG("Search for Multicast Partners!\n");

  for (auto &proxy : m_proxies)
  {
    if (proxy.remoteMulticastLocator.kind ==
        LocatorKind_t::LOCATOR_KIND_INVALID)
    {
      proxy.suppressUnicast = false;
      proxy.useMulticast = false;
    }
    else
    {
      bool found = false;
      for (auto &avproxy : m_proxies)
      {
        if (avproxy.remoteMulticastLocator.kind ==
                LocatorKind_t::LOCATOR_KIND_UDPv4 &&
            avproxy.remoteMulticastLocator.getIp4Address().addr ==
                proxy.remoteMulticastLocator.getIp4Address().addr &&
            avproxy.remoteLocator.getIp4Address().addr !=
                proxy.remoteLocator.getIp4Address().addr)
        {
          if (avproxy.suppressUnicast == false)
          {
            avproxy.useMulticast = false;
            avproxy.suppressUnicast = true;
            proxy.useMulticast = true;
            proxy.suppressUnicast = true;

            SFW_LOG("Found Multicast Partner!\n");

            if (avproxy.remoteReaderGuid.entityId !=
                proxy.remoteReaderGuid.entityId)
            {
              proxy.unknown_eid = true;

              SFW_LOG("Found different EntityIds, using UNKNOWN_ENTITYID\n");
            }
          }
          found = true;
        }
      }
      if (!found)
      {
        proxy.useMulticast = false;
        proxy.suppressUnicast = false;
      }
    }
  }
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::resetSendOptions()
{
  for (auto &proxy : m_proxies)
  {
    proxy.suppressUnicast = false;
    proxy.useMulticast = false;
    proxy.unknown_eid = false;
  }
  manageSendOptions();
}

// TODO: manage Multicast Options again...
template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::removeReader(const Guid_t &guid)
{
  Lock lock(m_mutex);
  auto isElementToRemove = [&](const ReaderProxy &proxy)
  {
    return proxy.remoteReaderGuid == guid;
  };
  auto thunk = [](void *arg, const ReaderProxy &value)
  {
    return (*static_cast<decltype(isElementToRemove) *>(arg))(value);
  };

  m_proxies.remove(thunk, &isElementToRemove);
  resetSendOptions();
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::removeReaderOfParticipant(
    const GuidPrefix_t &guidPrefix)
{
  Lock lock(m_mutex);
  auto isElementToRemove = [&](const ReaderProxy &proxy)
  {
    return proxy.remoteReaderGuid.prefix == guidPrefix;
  };
  auto thunk = [](void *arg, const ReaderProxy &value)
  {
    return (*static_cast<decltype(isElementToRemove) *>(arg))(value);
  };

  m_proxies.remove(thunk, &isElementToRemove);
  resetSendOptions();
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::commitPreparedDiscoveryAlive(
    PBufWrapper &&payload) noexcept
{
  Lock lock{m_mutex};

  CacheChange change{};
  change.kind = ChangeKind_t::ALIVE;
  change.sequenceNumber = SEQUENCENUMBER_UNKNOWN;
  change.data = std::move(payload);
  change.serializerCallback = nullptr;
  change.sizeToBeSerialized = 0;
  change.serviceRespFlag = false;
  change.identify = {};

  m_history.addPreparedChange(std::move(change));

  const SequenceNumber_t &newMin = m_history.getSeqNumMin();
  if (m_nextSequenceNumberToSend < newMin)
  {
    m_nextSequenceNumberToSend = newMin;
  }
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::notifyPreparedDiscovery() noexcept
{
  if (mp_threadPool != nullptr)
  {
    mp_threadPool->addWorkload(this);
  }
}

template <class NetworkDriver>
const rtps::CacheChange *StatefulWriterT<NetworkDriver>::newChangeIdentify(
    ChangeKind_t kind, const uint8_t *data, DataSize_t size, Sample_Indetify identify)
{
  if (isIrrelevant(kind))
  {
    return nullptr;
  }

  Lock lock{m_mutex};

  if (m_history.isFull())
  {
    // Right now we drop elements anyway because we cannot detect non-responding
    // readers yet. return nullptr;
    SequenceNumber_t newMin = ++SequenceNumber_t(m_history.getSeqNumMin());
    if (m_nextSequenceNumberToSend < newMin)
    {
      m_nextSequenceNumberToSend =
          newMin; // Make sure we have the correct sn to send
    }
  }

  auto *result = m_history.addChange(data, size, identify);
  // A client request enters here with an empty SampleIdentity.  Fill it only
  // after the history assigned the real writer sequence number.  A service
  // response already carries the request identity received by the server and
  // must preserve that identity verbatim.
  const bool empty_sample_identity =
      std::all_of(identify.guidPrefix.id.begin(), identify.guidPrefix.id.end(),
                  [](uint8_t byte) { return byte == 0; }) &&
      std::all_of(identify.writerId.entityKey.begin(),
                  identify.writerId.entityKey.end(),
                  [](uint8_t byte) { return byte == 0; }) &&
      identify.writerId.entityKind == rtps::EntityKind_t::USER_DEFINED_UNKNOWN;
  if (empty_sample_identity && result != nullptr)
  {
    auto *change = const_cast<CacheChange *>(result);
    change->identify.guidPrefix = m_attributes.endpointGuid.prefix;
    change->identify.writerId = m_attributes.endpointGuid.entityId;
    change->identify.sn = change->sequenceNumber;
  }
  if (mp_threadPool != nullptr)
  {
    mp_threadPool->addWorkload(this);
  }

  SFW_LOG("Adding new data.\n");

  return result;
}

template <class NetworkDriver>
const rtps::CacheChange *StatefulWriterT<NetworkDriver>::newChangeCallback(
    ChangeKind_t kind, CacheChange::SerializerCallback func, FragDataSize_t size)
{
  // Not supported
  return nullptr;
}

template <class NetworkDriver>
const rtps::CacheChange *StatefulWriterT<NetworkDriver>::newChange(
    ChangeKind_t kind, const uint8_t *data, DataSize_t size)
{
  if (isIrrelevant(kind))
  {
    return nullptr;
  }

  Lock lock{m_mutex};

  if (m_history.isFull())
  {
    // Right now we drop elements anyway because we cannot detect non-responding
    // readers yet. return nullptr;
    SequenceNumber_t newMin = ++SequenceNumber_t(m_history.getSeqNumMin());
    if (m_nextSequenceNumberToSend < newMin)
    {
      m_nextSequenceNumberToSend =
          newMin; // Make sure we have the correct sn to send
    }
  }

  auto *result = m_history.addChange(data, size);
  if (mp_threadPool != nullptr)
  {
    mp_threadPool->addWorkload(this);
  }

  SFW_LOG("Adding new data.\n");

  return result;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::progress()
{

  // newChangeIdentify() marks service traffic with this project-specific
  // SampleIdentity high word.  A generated request is otherwise silently
  // skipped when discovery has removed (or never installed) its reader proxy.
  if (m_proxies.isEmpty())
  {
    Lock lock(m_mutex);
    const CacheChange *next =
        m_history.getChangeBySN(m_nextSequenceNumberToSend);
    if (next != nullptr && next->serviceRespFlag)
    {
      printf("[RTPS TX] service request UNSENT: no matched reader proxy: request_id=%u\n",
             next->sequenceNumber.low);
    }
  }

  for (const auto &proxy : m_proxies)
  {
    bool success;
    if (!m_enforceUnicast)
    {
      success = sendDataWRMulticast(proxy, m_nextSequenceNumberToSend);
    }
    else
    {
      success = sendData(proxy, m_nextSequenceNumberToSend);
    }
    if (!success)
    {
      continue;
    }
  }
  ++m_nextSequenceNumberToSend;
}

template <typename NetworkDriver>
bool StatefulWriterT<NetworkDriver>::isIrrelevant(ChangeKind_t kind) const
{
  // Right now we only allow alive changes
  // return kind == ChangeKind_t::INVALID || (m_topicKind == TopicKind_t::NO_KEY
  // && kind != ChangeKind_t::ALIVE);
  return kind != ChangeKind_t::ALIVE;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::setAllChangesToUnsent()
{
  Lock lock(m_mutex);

  m_nextSequenceNumberToSend = m_history.getSeqNumMin();

  if (mp_threadPool != nullptr)
  {
    mp_threadPool->addWorkload(this);
  }
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::onNewAckNack(
    const SubmessageAckNack &msg, const GuidPrefix_t &sourceGuidPrefix)
{
  const bool service_request_writer =
      std::strncmp(m_attributes.topicName, "rq/", 3) == 0;
  ReaderProxy *reader = nullptr;
  for (auto &proxy : m_proxies)
  {
    if (proxy.remoteReaderGuid.prefix == sourceGuidPrefix &&
        proxy.remoteReaderGuid.entityId == msg.readerId)
    {
      reader = &proxy;
      break;
    }
  }

  if (reader == nullptr)
  {
    if (service_request_writer)
    {
      printf("[RTPS TX] service ACKNACK ignored: no matching reader proxy: base=%d:%u count=%u\n",
             msg.readerSNState.base.high, msg.readerSNState.base.low,
             msg.count.value);
    }
#if SFW_VERBOSE && RTPS_GLOBAL_VERBOSE
    SFW_LOG("No proxy found with id: ");
    printEntityId(msg.readerId);
    SFW_LOG(" Dropping acknack.\n");
#endif
    return;
  }

  if (service_request_writer)
  {
    printf("[RTPS TX] service ACKNACK: base=%d:%u num_bits=%u bitmap=0x%08x count=%u previous_count=%u\n",
           msg.readerSNState.base.high, msg.readerSNState.base.low,
           msg.readerSNState.numBits, msg.readerSNState.bitMap[0],
           msg.count.value, reader->ackNackCount.value);
  }

  uint8_t hash = 0;
  for (unsigned int i = 0; i < sourceGuidPrefix.id.size(); i++)
  {
    hash += sourceGuidPrefix.id.at(i);
  }

  char bfr[20];
  size_t size = snprintf(bfr, sizeof(bfr), "%u <= %u", msg.count.value,
                         reader->ackNackCount.value);
  if (!(size < sizeof(bfr)))
  {
    while (1)
      ;
  }

  if (msg.count.value <= reader->ackNackCount.value)
  {

    SFW_LOG("Count too small. Dropping acknack.\n");

    return;
  }

  reader->ackNackCount = msg.count;

  // Send missing packets
  SequenceNumber_t nextSN = msg.readerSNState.base;

  if (nextSN.low == 0 && nextSN.high == 0)
  {
    // A zero/empty ACKNACK is the RTPS preemptive handshake.  It does not
    // describe missing sequence numbers.  Falling through from SN 0 caused
    // every retained service request to be retransmitted on each handshake.
    // Answer it with a HEARTBEAT so the reader can return a real ACKNACK.
    if (service_request_writer)
    {
      printf("[RTPS TX] service preemptive ACKNACK: send HEARTBEAT instead of replaying history\n");
    }
    sendHeartBeat();
    return;
  }
  else
  {
    SFW_LOG("Received non-preemptive acknack.\n");
  }

  for (uint32_t i = 0; i < msg.readerSNState.numBits; ++i, ++nextSN)
  {
    if (msg.readerSNState.isSet(i))
    {

      SFW_LOG("Send Packet on acknack.\n");

      sendData(*reader, nextSN);
    }
  }
  // Check for sequence numbers after defined range
  SequenceNumber_t maxSN;
  {
    Lock lock(m_mutex);
    maxSN = m_history.getSeqNumMax();
  }
  while (nextSN <= maxSN)
  {
    sendData(*reader, nextSN);
    ++nextSN;
  }
}

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::sendData(
    const ReaderProxy &reader, const SequenceNumber_t &snMissing)
{
  // TODO smarter packaging e.g. by creating MessageStruct and serialize after
  // adjusting values Reusing the pbuf is not possible. See
  // https://www.nongnu.org/lwip/2_0_x/raw_api.html (Zero-Copy MACs)

  PacketInfo info;
  info.srcPort = m_packetInfo.srcPort;

  MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
  MessageFactory::addSubMessageTimeStamp(info.buffer);

  // Just usable for IPv4
  const LocatorIPv4 &locator = reader.remoteLocator;

  info.destAddr = locator.getIp4Address();
  info.destPort = (Ip4Port_t)locator.port;

  bool is_service_request = false;
  uint32_t request_id = 0;
  {
    Lock lock(m_mutex);
    const CacheChange *next = m_history.getChangeBySN(snMissing);
    if (next == nullptr)
    {

      SFW_LOG("Couldn't get a CacheChange with SN (%i,%u)\n", snMissing.high,
              snMissing.low);

      return false;
    }
    SFW_LOG("[debug]  Sending change with service identify SN (%ld,%ld)\n", next->identify.sn.high, next->identify.sn.low);
    // SFW_LOG("[debug]  Sending change with service identify SN (%ld)\n", next->identify.sn.low);
    // next->identify.sn.highに値が入っていれば、service通信のメッセージ
    if (next->serviceRespFlag)
    {
      SFW_LOG("[debug]  Sending service identify messege \n");
      // MessageFactory::addSubMessageData(
      //     info.buffer, next->data, true, next->sequenceNumber,
      //     m_attributes.endpointGuid.entityId, reader.remoteReaderGuid.entityId, next->identify);
      MessageFactory::addSubMessageData(
          info.buffer, next->data, true, next->sequenceNumber,
          m_attributes.endpointGuid.entityId, reader.remoteReaderGuid.entityId, next->identify);
    }
    else
    {
      MessageFactory::addSubMessageData(
          info.buffer, next->data, false, next->sequenceNumber,
          m_attributes.endpointGuid.entityId, reader.remoteReaderGuid.entityId, next->identify);
    }
    is_service_request = next->serviceRespFlag &&
                         next->identify.guidPrefix == m_attributes.endpointGuid.prefix &&
                         next->identify.writerId.entityKey == m_attributes.endpointGuid.entityId.entityKey &&
                         next->identify.writerId.entityKind == m_attributes.endpointGuid.entityId.entityKind &&
                         next->identify.sn == next->sequenceNumber;
    request_id = next->sequenceNumber.low;
  }

  if (is_service_request)
  {
    printf("[RTPS TX] service request RETRANSMIT: request_id=%u src_port=%u dst_port=%u bytes=%u\n",
           request_id, info.srcPort, info.destPort,
           info.buffer.firstElement->tot_len);
  }
  m_transport->sendPacket(info);
  return true;
}

template <class NetworkDriver>
bool StatefulWriterT<NetworkDriver>::sendDataWRMulticast(
    const ReaderProxy &reader, const SequenceNumber_t &snMissing)
{

  if (reader.useMulticast || reader.suppressUnicast == false)
  {
    PacketInfo info;
    info.srcPort = m_packetInfo.srcPort;

    MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
    MessageFactory::addSubMessageTimeStamp(info.buffer);

    // Deceide whether multicast or not
    if (reader.useMulticast)
    {
      const LocatorIPv4 &locator = reader.remoteMulticastLocator;
      info.destAddr = locator.getIp4Address();
      info.destPort = (Ip4Port_t)locator.port;
    }
    else
    {
      const LocatorIPv4 &locator = reader.remoteLocator;
      info.destAddr = locator.getIp4Address();
      info.destPort = (Ip4Port_t)locator.port;
    }

    bool is_service_request = false;
    uint32_t request_id = 0;
    {
      Lock lock(m_mutex);
      const CacheChange *next = m_history.getChangeBySN(snMissing);
      if (next == nullptr)
      {

        SFW_LOG("Couldn't get a CacheChange with SN (%i,%u)\n", snMissing.high,
                snMissing.low);

        return false;
      }

      EntityId_t reid;
      if (reader.useMulticast)
      {
        reid = ENTITYID_UNKNOWN;
      }
      else
      {
        reid = reader.remoteReaderGuid.entityId;
      }

      if (next->serviceRespFlag)
      {
        // SFW_LOG("[debug]  Sending service identify messege \n");
        SFW_LOG("[debug]  multicast Sending change with service identify SN (%lx,%lx)\n", next->identify.sn.high, next->identify.sn.low);
        MessageFactory::addSubMessageData(
            info.buffer, next->data, true, next->sequenceNumber,
            m_attributes.endpointGuid.entityId, reid, next->identify);
      }
      else
      {
        MessageFactory::addSubMessageData(
            info.buffer, next->data, false, next->sequenceNumber,
            m_attributes.endpointGuid.entityId, reid, next->identify);
      }
      is_service_request = next->serviceRespFlag &&
                           next->identify.guidPrefix == m_attributes.endpointGuid.prefix &&
                           next->identify.writerId.entityKey == m_attributes.endpointGuid.entityId.entityKey &&
                           next->identify.writerId.entityKind == m_attributes.endpointGuid.entityId.entityKind &&
                           next->identify.sn == next->sequenceNumber;
      request_id = next->sequenceNumber.low;
    }
    if (is_service_request)
    {
      printf("[RTPS TX] service request SENT: request_id=%u src_port=%u dst_port=%u bytes=%u\n",
             request_id, info.srcPort, info.destPort,
             info.buffer.firstElement->tot_len);
    }
    m_transport->sendPacket(info);
    // Do not depend solely on the per-writer heartbeat thread.  In the WASI
    // threads runtime that thread may be delayed, while a reliable DDS reader
    // needs a HEARTBEAT to turn its preemptive ACKNACK (base 0:0) into an ACK
    // for the actual request sequence number.
    if (is_service_request)
    {
      sendHeartBeat();
    }
  }
  return true;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::hbFunctionJumppad(void *thisPointer)
{
  auto *writer = static_cast<StatefulWriterT<NetworkDriver> *>(thisPointer);
  writer->sendHeartBeatLoop();
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::sendHeartBeatLoop()
{
  m_thread_running = true;
  while (m_running)
  {
    sendHeartBeat();
#ifdef OS_IS_FREERTOS
    vTaskDelay(pdMS_TO_TICKS(Config::SF_WRITER_HB_PERIOD_MS));
#else
    sys_msleep(Config::SF_WRITER_HB_PERIOD_MS);
#endif
  }
  m_thread_running = false;
}

template <class NetworkDriver>
void StatefulWriterT<NetworkDriver>::sendHeartBeat()
{
  if (m_proxies.isEmpty())
  {

    SFW_LOG("Skipping heartbeat. No proxies.\n");
    return;
  }

  for (auto &proxy : m_proxies)
  {

    PacketInfo info;
    info.srcPort = m_packetInfo.srcPort;

    SequenceNumber_t firstSN;
    SequenceNumber_t lastSN;
    MessageFactory::addHeader(info.buffer, m_attributes.endpointGuid.prefix);
    {
      Lock lock(m_mutex);
      firstSN = m_history.getSeqNumMin();
      lastSN = m_history.getSeqNumMax();
    }
    if (firstSN == SEQUENCENUMBER_UNKNOWN || lastSN == SEQUENCENUMBER_UNKNOWN)
    {

      if (strlen(&this->m_attributes.typeName[0]) != 0)
      {
        SFW_LOG("Skipping heartbeat. No data.\n");
      }
      return;
    }

    MessageFactory::addHeartbeat(
        info.buffer, m_attributes.endpointGuid.entityId,
        proxy.remoteReaderGuid.entityId, firstSN, lastSN, m_hbCount);

    info.destAddr = proxy.remoteLocator.getIp4Address();
    info.destPort = proxy.remoteLocator.port;
    if (std::strncmp(m_attributes.topicName, "rq/", 3) == 0)
    {
      printf("[RTPS TX] service HEARTBEAT: first=%d:%u last=%d:%u count=%u src_port=%u dst_port=%u\n",
             firstSN.high, firstSN.low, lastSN.high, lastSN.low,
             m_hbCount.value, info.srcPort, info.destPort);
    }
    m_transport->sendPacket(info);
  }
  m_hbCount.value++;
}

#undef SFW_VERBOSE
