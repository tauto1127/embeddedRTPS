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

template <class NetworkDriver>
StatefulReaderT<NetworkDriver>::~StatefulReaderT()
{
  //  if(sys_mutex_valid(&m_mutex)){ // Getting invalid pointer error, there
  //  seems sth strange
  //    sys_mutex_free(&m_mutex);
  //  }
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

template <class NetworkDriver>
void StatefulReaderT<NetworkDriver>::newChange(
    const ReaderCacheChange &cacheChange)
{
  if (m_callback == nullptr)
  {
    return;
  }
  Lock lock{m_mutex};
  for (auto &proxy : m_proxies)
  {
    if (proxy.remoteWriterGuid == cacheChange.writerGuid)
    {
      const bool is_service_reply =
          m_attributes.topicName[0] == 'r' &&
          m_attributes.topicName[1] == 'r' &&
          m_attributes.topicName[2] == '/';
      const bool service_reply_after_gap =
          is_service_reply && proxy.expectedSN < cacheChange.sn;

      // Service replies are correlated by PID_RELATED_SAMPLE_IDENTITY rather
      // than by the response writer's RTPS sequence number.  If an earlier
      // reply was lost (or intentionally abandoned after an application
      // timeout), blocking every later independent reply behind that gap makes
      // the client fail permanently.  This reader has no out-of-order cache,
      // so service replies resynchronize at the newest delivered sample.
      if (service_reply_after_gap)
      {
        printf("[RTPS RX] service response gap skipped: expected=%d:%u received=%d:%u\n",
               proxy.expectedSN.high, proxy.expectedSN.low,
               cacheChange.sn.high, cacheChange.sn.low);
      }

      if (proxy.expectedSN == cacheChange.sn || service_reply_after_gap)
      {
        m_callback(m_callee, cacheChange);
        proxy.expectedSN = cacheChange.sn;
        ++proxy.expectedSN;

        // A ROS 2 service reply reader must release the server writer's
        // reliable history promptly.  Waiting for a later HEARTBEAT can leave
        // the last delivered reply unacknowledged indefinitely; with the
        // default service history depth this eventually stalls replies after
        // roughly ten calls.  Send a cumulative final ACK immediately after
        // accepting each Reply sample.
        if (is_service_reply)
        {
          PacketInfo info;
          info.srcPort = m_packetInfo.srcPort;
          info.destAddr = proxy.remoteLocator.getIp4Address();
          info.destPort = proxy.remoteLocator.port;

          SequenceNumberSet acked;
          acked.base = proxy.expectedSN;
          acked.numBits = 0;
          const Count_t count = proxy.getNextAckNackCount();

          rtps::MessageFactory::addHeader(
              info.buffer, m_attributes.endpointGuid.prefix);
          rtps::MessageFactory::addAckNack(
              info.buffer, proxy.remoteWriterGuid.entityId,
              m_attributes.endpointGuid.entityId, acked, count, true);

          printf("[RTPS RX] service response ACK: received=%d:%u base=%d:%u count=%d src_port=%u dst_port=%u\n",
                 cacheChange.sn.high, cacheChange.sn.low,
                 acked.base.high, acked.base.low, count.value,
                 static_cast<unsigned int>(info.srcPort),
                 static_cast<unsigned int>(info.destPort));
          m_transport->sendPacket(info);
        }
        return;
      }

      if (is_service_reply)
      {
        printf("[RTPS RX] service response DATA ignored: received=%d:%u expected=%d:%u\n",
               cacheChange.sn.high, cacheChange.sn.low,
               proxy.expectedSN.high, proxy.expectedSN.low);
      }
    }
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
}

template <class NetworkDriver>
bool StatefulReaderT<NetworkDriver>::onNewHeartbeat(
    const SubmessageHeartbeat &msg, const GuidPrefix_t &sourceGuidPrefix)
{
  Lock lock(m_mutex);
  PacketInfo info;
  info.srcPort = m_packetInfo.srcPort;
  WriterProxy *writer = nullptr;
  // Search for writer
  for (WriterProxy &proxy : m_proxies)
  {
    if (proxy.remoteWriterGuid.prefix == sourceGuidPrefix &&
        proxy.remoteWriterGuid.entityId == msg.writerId)
    {
      writer = &proxy;
      break;
    }
  }

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
  info.destAddr = writer->remoteLocator.getIp4Address();
  info.destPort = writer->remoteLocator.port;
  rtps::MessageFactory::addHeader(info.buffer,
                                  m_attributes.endpointGuid.prefix);
  rtps::MessageFactory::addAckNack(info.buffer, msg.writerId, msg.readerId,
                                   writer->getMissing(msg.firstSN, msg.lastSN),
                                   writer->getNextAckNackCount(), false);

  SFR_LOG("Sending acknack.\n");
  m_transport->sendPacket(info);
  return true;
}

#undef SFR_VERBOSE
