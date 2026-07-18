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

#ifndef RTPS_MESSAGEFACTORY_H
#define RTPS_MESSAGEFACTORY_H

#include "rtps/common/types.h"
#include "rtps/config.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/sysFunctions.h"

#include <array>
#include <cstdint>
// service communication
// #include <mutex>
// #include <queue>
// #include "rtps/rtps.h"

namespace rtps
{
  namespace MessageFactory
  {
    const std::array<uint8_t, 4> PROTOCOL_TYPE{'R', 'T', 'P', 'S'};
    const uint8_t numBytesUntilEndOfLength =
        4; // The first bytes incl. submessagelength don't count

    template <class Buffer>
    void addHeader(Buffer &buffer, const GuidPrefix_t &guidPrefix)
    {

      Header header;
      header.protocolName = PROTOCOL_TYPE;
      header.protocolVersion = PROTOCOLVERSION;
      header.vendorId = Config::VENDOR_ID;
      header.guidPrefix = guidPrefix;

      serializeMessage(buffer, header);
    }

    template <class Buffer>
    bool addSubMessageInfoDST(Buffer &buffer, GuidPrefix_t &dst)
    {
      SubmessageInfoDST msg;
      msg.header.submessageId = SubmessageKind::INFO_DST;

#if IS_LITTLE_ENDIAN
      msg.header.flags = FLAG_LITTLE_ENDIAN;
#else
      msg.header.flags = FLAG_BIG_ENDIAN;
#endif

      msg.header.octetsToNextHeader = sizeof(GuidPrefix_t);
      msg.guidPrefix = dst;

      return serializeMessage(buffer, msg);
    }

    template <class Buffer>
    void addSubMessageTimeStamp(Buffer &buffer, bool setInvalid = false)
    {
      SubmessageHeader header;
      header.submessageId = SubmessageKind::INFO_TS;

#if IS_LITTLE_ENDIAN
      header.flags = FLAG_LITTLE_ENDIAN;
#else
      header.flags = FLAG_BIG_ENDIAN;
#endif

      if (setInvalid)
      {
        header.flags |= FLAG_INVALIDATE;
        header.octetsToNextHeader = 0;
      }
      else
      {
        header.octetsToNextHeader = sizeof(Time_t);
      }

      serializeMessage(buffer, header);

      if (!setInvalid)
      {
        buffer.reserve(header.octetsToNextHeader);
        Time_t now = getCurrentTimeStamp();
        buffer.append(reinterpret_cast<uint8_t *>(&now.seconds),
                      sizeof(Time_t::seconds));
        buffer.append(reinterpret_cast<uint8_t *>(&now.fraction),
                      sizeof(Time_t::fraction));
      }
    }

    template <class Buffer>
    void addSubMessageDataFrag(Buffer &buffer, const Buffer &filledPayload,
                               bool containsInlineQos, const SequenceNumber_t &SN,
                               uint32_t fragStartingNumber,
                               uint16_t fragSize,
                               uint32_t sampleSize,
                               const EntityId_t &writerID, const EntityId_t &readerID)
    {
      SubmessageDataFrag msg;
      msg.header.submessageId = SubmessageKind::DATA_FRAG;
#if IS_LITTLE_ENDIAN
      msg.header.flags = FLAG_LITTLE_ENDIAN;
#else
      msg.header.flags = FLAG_BIG_ENDIAN;
#endif

      msg.header.octetsToNextHeader = 0;

      if (containsInlineQos)
      {
        msg.header.flags |= FLAG_INLINE_QOS;
      }

      msg.writerSN = SN;
      msg.extraFlags = 0;
      msg.readerId = readerID;
      msg.writerId = writerID;
      msg.fragStartingNumber = fragStartingNumber;
      msg.fragmentsInSubmessage = 1;
      msg.fragmentSize = fragSize;
      msg.sampleSize = sampleSize;

      constexpr uint16_t octetsToInlineQoS =
          4 + 4 + 8 + 4 + 2 + 2 + 4;
      // EntityIds + SequenceNumber + FragmentNumber + fragmentsInSubmessage + fragmentSize + sampleSize
      msg.octetsToInlineQos = octetsToInlineQoS;

      serializeMessage(buffer, msg);

      if (filledPayload.isValid())
      {
        Buffer shallowCopy = filledPayload;
        buffer.append(std::move(shallowCopy));
      }
    }

    /*
    MessageFactory::addSubMessageDataFrag(info.buffer, data, false,
                                                        next->sequenceNumber,
                                                        fragment_start_number++,
                                                        serialized_buf.second,
                                                        sampleSize,
                                                        m_attributes.endpointGuid.entityId,
                                                        reid);
    */
    template <class Buffer>
    void addSubMessageData(Buffer &buffer, const Buffer &filledPayload,
                           bool containsInlineQos, const SequenceNumber_t &SN,
                           const EntityId_t &writerID, const EntityId_t &readerID, const Sample_Indetify &identify)
    {
      SubmessageData msg;
      msg.header.submessageId = SubmessageKind::DATA;
#if IS_LITTLE_ENDIAN
      msg.header.flags = FLAG_LITTLE_ENDIAN;
#else
      msg.header.flags = FLAG_BIG_ENDIAN;
#endif

      msg.header.octetsToNextHeader = SubmessageData::getRawSize() +
                                      filledPayload.spaceUsed() -
                                      numBytesUntilEndOfLength;
      // inlineQosがある場合はここで追加する

      // containsInlineQos = true;
      // containsInlineQos = false;
      if (containsInlineQos)
      {
        msg.header.flags |= FLAG_INLINE_QOS;
      }

      // for service debug
      if (containsInlineQos)
      {
        msg.header.octetsToNextHeader += 32;
      }

      if (filledPayload.isValid())
      {
        msg.header.flags |= FLAG_DATA_PAYLOAD;
      }

      msg.writerSN = SN;
      msg.extraFlags = 0;
      msg.readerId = readerID;
      msg.writerId = writerID;

      constexpr uint16_t octetsToInlineQoS =
          4 + 4 + 8; // EntityIds + SequenceNumber
      msg.octetsToInlineQos = octetsToInlineQoS;

      serializeMessage(buffer, msg);

      if (containsInlineQos)
      {
        // CacheChangeInfo info;
        // {
        //   // std::lock_guard<std::mutex> lock(bufferMutex);
        //   if (!cacheChangeQueue.empty())
        //   {
        //     // 最新の情報を取得
        //     info = cacheChangeQueue.front();
        //     cacheChangeQueue.pop();
        //   }
        //   else
        //   {
        //     // エラー処理：バッファが空の場合の対処
        //     // 例：デフォルト値を使用する、処理をスキップするなど
        //     // return;
        //     printf("service: cacheChangeQueue is empty\n");
        //   }
        // }

        // for service communication add inlineQos and sampleIdentity
        if (!buffer.reserve(32))
        {
          printf("Failed to reserve space for submessage\n");
        }
        uint16_t PID_CUSTOM_RELATED_SAMPLE_IDENTITY = 0x800f;
        uint16_t PID_LENGTH = 24;
        // 12byteのguidprefixを追加
        // std::array<uint8_t, 12> guidPrefix = {0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78};
        // uint32_t entity_id = 0x22222222;
        // uint32_t sequenceNumber_high = 0xffffffff;
        // uint32_t sequenceNumber_low = 0x00000000;
        std::array<uint8_t, 12> guidPrefix = identify.guidPrefix.id;
        std::array<uint8_t, 3> entityKey = identify.writerId.entityKey;
        EntityKind_t entityKind = identify.writerId.entityKind;
        uint32_t sequenceNumber_high = identify.sn.high;
        uint32_t sequenceNumber_low = identify.sn.low;

        uint16_t PID_SENTINEL = 0x0001;
        uint16_t PID_SENTINEL_LENGTH = 0;

        buffer.append(reinterpret_cast<uint8_t *>(&PID_CUSTOM_RELATED_SAMPLE_IDENTITY),
                      sizeof(PID_CUSTOM_RELATED_SAMPLE_IDENTITY));
        buffer.append(reinterpret_cast<uint8_t *>(&PID_LENGTH),
                      sizeof(PID_LENGTH));
        /* */
        buffer.append(guidPrefix.data(), guidPrefix.size());
        // buffer.append(msg.Header.guidPrefix.id.data(), sizeof(GuidPrefix_t));

        /* entity_id  reader reqeuestを送るときに使うwriterのentity_idと対応したもの  reader.remoteReaderGuid.entityId;とかが使えるといいかも*/
        // buffer.append(reinterpret_cast<uint8_t *>(&entity_id), sizeof(entity_id));
        // buffer.append(msg.readerId.entityKey.data(), msg.readerId.entityKey.size());
        buffer.append(entityKey.data(), entityKey.size());
        buffer.append(reinterpret_cast<uint8_t *>(&entityKind),
                      sizeof(EntityKind_t));
        /* sequecenumber */
        buffer.append(reinterpret_cast<uint8_t *>(&sequenceNumber_high),
                      sizeof(sequenceNumber_high));
        buffer.append(reinterpret_cast<uint8_t *>(&sequenceNumber_low), sizeof(sequenceNumber_low));

        buffer.append(reinterpret_cast<uint8_t *>(&PID_SENTINEL), sizeof(PID_SENTINEL));
        // ParameterList entries always contain a PID and a uint16 length.
        // The previous code advertised 32 inline-QoS bytes but appended only
        // 30, letting the first two CDR payload bytes masquerade as this
        // sentinel length and making the DATA submessage malformed.
        buffer.append(reinterpret_cast<uint8_t *>(&PID_SENTINEL_LENGTH),
                      sizeof(PID_SENTINEL_LENGTH));
      }
      if (filledPayload.isValid())
      {
        Buffer shallowCopy = filledPayload;
        buffer.append(std::move(shallowCopy)); // ここでデータを追加している?
      }
    }

    template <class Buffer>
    void addHeartbeat(Buffer &buffer, EntityId_t writerId, EntityId_t readerId,
                      SequenceNumber_t firstSN, SequenceNumber_t lastSN,
                      Count_t count)
    {
      SubmessageHeartbeat subMsg;
      subMsg.header.submessageId = SubmessageKind::HEARTBEAT;
      subMsg.header.octetsToNextHeader =
          SubmessageHeartbeat::getRawSize() - numBytesUntilEndOfLength;
#if IS_LITTLE_ENDIAN
      subMsg.header.flags = FLAG_LITTLE_ENDIAN;
#else
      subMsg.header.flags = FLAG_BIG_ENDIAN;
#endif
      // Force response by not setting final flag.

      subMsg.writerId = writerId;
      subMsg.readerId = readerId;
      subMsg.firstSN = firstSN;
      subMsg.lastSN = lastSN;
      subMsg.count = count;

      serializeMessage(buffer, subMsg);
    }

    template <class Buffer>
    void addAckNack(Buffer &buffer, EntityId_t writerId, EntityId_t readerId,
                    SequenceNumberSet readerSNState, Count_t count,
                    bool final_flag)
    {
      SubmessageAckNack subMsg;
      subMsg.header.submessageId = SubmessageKind::ACKNACK;
#if IS_LITTLE_ENDIAN
      subMsg.header.flags = FLAG_LITTLE_ENDIAN;
#else
      subMsg.header.flags = FLAG_BIG_ENDIAN;
#endif
      if (final_flag)
      {
        subMsg.header.flags |= FLAG_FINAL; // For now, we don't want any response
      }
      else
      {
        subMsg.header.flags &= ~FLAG_FINAL; // For now, we don't want any response
      }
      subMsg.header.octetsToNextHeader =
          SubmessageAckNack::getRawSize(readerSNState) - numBytesUntilEndOfLength;

      subMsg.writerId = writerId;
      subMsg.readerId = readerId;
      subMsg.readerSNState = readerSNState;
      subMsg.count = count;

      serializeMessage(buffer, subMsg);
    }
  } // namespace MessageFactory
} // namespace rtps

#endif // RTPS_MESSAGEFACTORY_H
