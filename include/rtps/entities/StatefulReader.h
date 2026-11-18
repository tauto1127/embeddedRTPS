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

#ifndef RTPS_STATEFULREADER_H
#define RTPS_STATEFULREADER_H

#include "lwip/sys.h"
#include "rtps/communication/UdpDriver.h"
#include "rtps/config.h"
#include "rtps/entities/Reader.h"
#include "rtps/entities/WriterProxy.h"
#include "rtps/storages/MemoryPool.h"

#include <cstdint>

namespace rtps {
struct SubmessageHeartbeat;
struct SubmessageGap;

//! Out-of-order changes kept per reader until the missing ones arrive. A
//! change is at most one DATA submessage (DataSize_t), so a reader keeps at
//! most 16 x 64 KiB. When full, a change is dropped and requested again.
constexpr uint32_t SFR_MAX_BUFFERED_CHANGES = 16;

template <class NetworkDriver> class StatefulReaderT final : public Reader {
public:
  ~StatefulReaderT() override;
  void init(const TopicData &attributes, NetworkDriver &driver);
  void newChange(const ReaderCacheChange &cacheChange) override;
  void registerCallback(ddsReaderCallback_fp cb, void *callee) override;
  bool addNewMatchedWriter(const WriterProxy &newProxy) override;
  void removeWriter(const Guid_t &guid) override;
  void removeWriterOfParticipant(const GuidPrefix_t &guidPrefix) override;
  bool onNewHeartbeat(const SubmessageHeartbeat &msg,
                      const GuidPrefix_t &remotePrefix) override;
  bool onNewGap(const SubmessageGap &msg,
                const GuidPrefix_t &remotePrefix) override;

private:
  struct BufferedChange {
    bool used = false;
    Guid_t writerGuid;
    SequenceNumber_t sn;
    ChangeKind_t kind;
    uint32_t response;
    Sample_Indetify identify;
    DataSize_t size;
    uint8_t *data;
  };
  BufferedChange m_buffered[SFR_MAX_BUFFERED_CHANGES];

  WriterProxy *findProxy(const Guid_t &writerGuid);
  void deliver(const ReaderCacheChange &change, WriterProxy &proxy);
  void deliverBuffered(BufferedChange &slot, WriterProxy &proxy);
  BufferedChange *findBuffered(const Guid_t &writerGuid,
                               const SequenceNumber_t &sn);
  bool isBuffered(const Guid_t &writerGuid, const SequenceNumber_t &sn);
  void store(const ReaderCacheChange &change);
  void freeSlot(BufferedChange &slot);
  void dropBufferedOf(const Guid_t &writerGuid);
  void dropBufferedOfParticipant(const GuidPrefix_t &prefix);
  void deliverInOrder(WriterProxy &proxy);
  void skipTo(WriterProxy &proxy, const SequenceNumber_t &newExpected);
  void markIrrelevant(WriterProxy &proxy, const SequenceNumber_t &from,
                      const SequenceNumber_t &toExclusive);
  SequenceNumberSet getMissing(WriterProxy &proxy,
                               const SequenceNumber_t &lastAvail);

  PacketInfo
      m_packetInfo; // TODO intended for reuse but buffer not used as such
  NetworkDriver *m_transport;

  ddsReaderCallback_fp m_callback = nullptr;
  void *m_callee = nullptr;
  sys_mutex_t m_mutex;
};

using StatefulReader = StatefulReaderT<UdpDriver>;

} // namespace rtps

#include "StatefulReader.tpp"

#endif // RTPS_STATEFULREADER_H
