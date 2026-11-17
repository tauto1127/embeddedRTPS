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

#include "rtps/messages/MessageReceiver.h"
#include <rtps/entities/Participant.h>

#include "rtps/entities/Reader.h"
#include "rtps/entities/Writer.h"
#include "rtps/messages/MessageTypes.h"
#include "rtps/utils/Log.h"

using rtps::MessageReceiver;

#if RECV_VERBOSE && RTPS_GLOBAL_VERBOSE
#include "rtps/utils/printutils.h"
#define RECV_LOG(...)    \
  if (true)              \
  {                      \
    printf("[RECV] ");   \
    printf(__VA_ARGS__); \
    printf("\n");        \
  }
#else
#define RECV_LOG(...) //
#endif

MessageReceiver::MessageReceiver(Participant *part) : mp_part(part) {}

void MessageReceiver::resetState()
{
  sourceGuidPrefix = GUIDPREFIX_UNKNOWN;
  sourceVersion = PROTOCOLVERSION;
  sourceVendor = VENDOR_UNKNOWN;
  haveTimeStamp = false;
}

/*
headerとsubmessageの解析をしている
exampleinput :echobackstring
payload:RTPS\002\003\001\017\001\017\r\027t\276\034a プロトコルバージョンとかが含まれる
len:120
*/
bool MessageReceiver::processMessage(const uint8_t *data, DataSize_t size)
{
  resetState();
  MessageProcessingInfo msgInfo(data, size); // headerの解析用

  if (!processHeader(msgInfo))
  {
    return false;
  }
  SubmessageHeader submsgHeader;
  while (msgInfo.nextPos < msgInfo.size)
  {
    if (!deserializeMessage(msgInfo, submsgHeader))
    {
      return false;
    }
    processSubmessage(msgInfo, submsgHeader);
  }

  return true;
}

// headerの解析をしてそう
bool MessageReceiver::processHeader(MessageProcessingInfo &msgInfo)
{
  Header header;
  if (!deserializeMessage(msgInfo, header))
  {
    return false;
  }

  if (header.guidPrefix.id == mp_part->m_guidPrefix.id)
  {
    RECV_LOG("[MessageReceiver]: Received own message.\n");
    return false; // Don't process our own packet
  }

  if (header.protocolName != RTPS_PROTOCOL_NAME ||
      header.protocolVersion.major != PROTOCOLVERSION.major)
  {
    return false;
  }

  sourceGuidPrefix = header.guidPrefix;
  sourceVendor = header.vendorId; // service通信の識別に使えそう
  sourceVersion = header.protocolVersion;

  msgInfo.nextPos += Header::getRawSize();
  return true;
}

bool MessageReceiver::processSubmessage(MessageProcessingInfo &msgInfo,
                                        const SubmessageHeader &submsgHeader)
{
  bool success = false;

  switch (submsgHeader.submessageId)
  {
  case SubmessageKind::ACKNACK:
    RECV_LOG("Processing AckNack submessage\n");
    success = processAckNackSubmessage(msgInfo);
    break;
  case SubmessageKind::DATA:
    RECV_LOG("Processing Data submessage\n");
    success = processDataSubmessage(msgInfo, submsgHeader);
    break;
  case SubmessageKind::HEARTBEAT:
    RECV_LOG("Processing Heartbeat submessage\n");
    success = processHeartbeatSubmessage(msgInfo);
    break;
  case SubmessageKind::GAP:
    RECV_LOG("Processing Gap submessage\n");
    success = processGapSubmessage(msgInfo);
    break;
  case SubmessageKind::INFO_DST:
    RECV_LOG("Info_DST submessage not relevant.\n");
    success = true; // Not relevant
    break;
  case SubmessageKind::INFO_TS:
    RECV_LOG("Info_TS submessage not relevant.\n");
    success = true; // Not relevant now
    break;
  default:
    RECV_LOG("Submessage of type %u currently not supported. Skipping..\n",
             static_cast<uint8_t>(submsgHeader.submessageId));
    success = false;
  }
  msgInfo.nextPos +=
      submsgHeader.octetsToNextHeader + SubmessageHeader::getRawSize();
  return success;
}

// submessegeのdataを処理している
bool MessageReceiver::processDataSubmessage(
    MessageProcessingInfo &msgInfo, const SubmessageHeader &submsgHeader)
{
  SubmessageData dataSubmsg;
  if (!deserializeMessage(msgInfo, dataSubmsg))
  {
    return false;
  }

  const uint8_t *serializedData =
      msgInfo.getPointerToCurrentPos() + SubmessageData::getRawSize(); // service通信独自の項目で条件分けする方法を考える

  const DataSize_t size = submsgHeader.octetsToNextHeader -
                          SubmessageData::getRawSize() +
                          SubmessageHeader::getRawSize();

  RECV_LOG("Received data message size %u", (int)size);

  Reader *reader;
  if (dataSubmsg.readerId == ENTITYID_UNKNOWN)
  {
#if RECV_VERBOSE && RTPS_GLOBAL_VERBOSE
    RECV_LOG("Received ENTITYID_UNKNOWN readerID, searching for writer ID = ");
    printGuid(Guid_t{sourceGuidPrefix, dataSubmsg.writerId});
    printf("\n");
#endif
    reader = mp_part->getReaderByWriterId(
        Guid_t{sourceGuidPrefix, dataSubmsg.writerId}); // writerIDからreaderを取得
    if (reader != nullptr)
      RECV_LOG("Found reader!");
  }
  else
  {
    reader = mp_part->getReader(dataSubmsg.readerId); // readerの取得
#if RECV_VERBOSE && RTPS_GLOBAL_VERBOSE
    auto reader_by_writer = mp_part->getReaderByWriterId(
        Guid_t{sourceGuidPrefix, dataSubmsg.writerId});

    if (reader_by_writer == nullptr && reader != nullptr)
    {
      RECV_LOG("FOUND By READER ID, NOT BY WRITER ID =");
      printGuid(Guid_t{sourceGuidPrefix, dataSubmsg.writerId});
      printf("\n");
    }
#endif
  }
  if (reader != nullptr)
  {
    // serializedDataの先頭2byteを比較することでservice通信の識別を行う　0x800f
    if (serializedData[0] == 0x0f && serializedData[1] == 0x80)
    {
      RECV_LOG("Received service message\n");
      // デシリアライズ
      Sample_Indetify identify; // service通信の識別子
      if (!deserializeMessage(msgInfo, identify))
      {
        RECV_LOG("Falut deeserialize service message identify\n");
        return false;
      }

      Guid_t writerGuid{sourceGuidPrefix, dataSubmsg.writerId};
      // serializedDataのサスデータを28byteずらす
      // doCopyAndMoveOn(reinterpret_cast<uint8_t *>(&msg.writerSN.high), currentPos,
      // sizeof(msg.writerSN.high));

      // std::array<uint8_t, 12> writerGuidPrefix = {0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78};
      // memcpy(reinterpret_cast<uint8_t *>(&writerGuidPrefix), &serializedData[2], 12);

      // readerにresponseメッセージか通知する変数を追加
      uint32_t response = 1;
      serializedData += 32;
      ReaderCacheChange change{ChangeKind_t::ALIVE, writerGuid,
                               dataSubmsg.writerSN, serializedData, size, response, identify}; // inlineqosのサイズ分だけoffsetをずらす28byte→32byteだったかも
      reader->newChange(change);                                                               // service通信を受信した時に、writerGuidを別の場所に持っていくとかしたい。でもどこに保存したらいいのかわからない
      return true;
    }

    uint32_t response = 0;
    Guid_t writerGuid{sourceGuidPrefix, dataSubmsg.writerId};
    Sample_Indetify identify; // service通信の識別子

    ReaderCacheChange change{ChangeKind_t::ALIVE, writerGuid,
                             dataSubmsg.writerSN, serializedData, size, response, identify};

    // Guid_t writerGuid{sourceGuidPrefix, dataSubmsg.writerId};
    // ReaderCacheChange change{ChangeKind_t::ALIVE, writerGuid,
    //                          dataSubmsg.writerSN, serializedData, size}; // readerに通知するデータを作成？

    reader->newChange(change);
  }
  else
  {
#if RECV_VERBOSE && RTPS_GLOBAL_VERBOSE
    RECV_LOG("Couldn't find a reader with id: ");
    printEntityId(dataSubmsg.readerId);
    printf("\n");
#endif
  }

  return true;
}

bool MessageReceiver::processHeartbeatSubmessage(
    MessageProcessingInfo &msgInfo)
{
  SubmessageHeartbeat submsgHB;
  if (!deserializeMessage(msgInfo, submsgHB))
  {
    return false;
  }

  Reader *reader = mp_part->getReader(submsgHB.readerId);
  if (reader != nullptr)
  {
    reader->onNewHeartbeat(submsgHB, sourceGuidPrefix);
    mp_part->addHeartbeat(sourceGuidPrefix);
    return true;
  }
  else
  {
    return false;
  }
}

bool MessageReceiver::processAckNackSubmessage(MessageProcessingInfo &msgInfo)
{
  SubmessageAckNack submsgAckNack;
  if (!deserializeMessage(msgInfo, submsgAckNack))
  {
    return false;
  }

  Writer *writer = mp_part->getWriter(submsgAckNack.writerId);
  if (writer != nullptr)
  {
    writer->onNewAckNack(submsgAckNack, sourceGuidPrefix);
    return true;
  }
  else
  {
    return false;
  }
}

bool MessageReceiver::processGapSubmessage(MessageProcessingInfo &msgInfo)
{
  SubmessageGap submsgGap;
  if (!deserializeMessage(msgInfo, submsgGap))
  {
    return false;
  }

  Reader *reader;
  if (submsgGap.readerId == ENTITYID_UNKNOWN)
  {
    reader = mp_part->getReaderByWriterId(
        Guid_t{sourceGuidPrefix, submsgGap.writerId});
  }
  else
  {
    reader = mp_part->getReader(submsgGap.readerId);
  }
  if (reader == nullptr)
  {
    return false;
  }
  return reader->onNewGap(submsgGap, sourceGuidPrefix);
}

#undef RECV_VERBOSE
