/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* ================================================================ */
/*                                                                  */
/* Licensed Materials - Property of IBM                             */
/*                                                                  */
/* Blue Gene/Q                                                      */
/*                                                                  */
/* (C) Copyright IBM Corp.  2011, 2012                              */
/*                                                                  */
/* US Government Users Restricted Rights -                          */
/* Use, duplication or disclosure restricted                        */
/* by GSA ADP Schedule Contract with IBM Corp.                      */
/*                                                                  */
/* This software is available to you under the                      */
/* Eclipse Public License (EPL).                                    */
/*                                                                  */
/* ================================================================ */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */

//! \file  MercuryController.cc
//! \brief Methods for bgcios::stdio::MercuryController class.

// Includes
#include "MercuryController.h"
#include <ramdisk/include/services/common/RdmaError.h>
#include <ramdisk/include/services/common/RdmaDevice.h>
#include <ramdisk/include/services/common/RdmaCompletionQueue.h>
#include <ramdisk/include/services/common/logging.h>
#include <ramdisk/include/services/MessageUtility.h>
#include <ramdisk/include/services/ServicesConstants.h>
#include <poll.h>
#include <errno.h>
#include <iomanip>
#include <sstream>
#include <queue>
#include <stdio.h>

#include "cscs_messages.h"

using namespace bgcios::stdio;

LOG_DECLARE_FILE("jb");

/*
#undef LOG_CIOS_DEBUG_MSG
#undef LOG_DEBUG_MSG
#define LOG_CIOS_DEBUG_MSG(x) LOG_CIOS_DEBUG_MSG(x);
#define LOG_DEBUG_MSG(x) LOG_CIOS_DEBUG_MSG(x);
*/

const uint64_t LargeRegionSize  = 8192;
/*---------------------------------------------------------------------------*/
MercuryController::MercuryController(const char *device, const char *interface, int port)
{
  this->_device = device;
  this->_interface = interface;
  this->_port = port;
 }
/*---------------------------------------------------------------------------*/
MercuryController::~MercuryController()
{
  LOG_CIOS_DEBUG_MSG("MercuryController destructor clearing clients");
  _dequeUnexpectedInClient.clear();
  _dequeExpectedInClient.clear();
  _clients.clear();
  LOG_CIOS_DEBUG_MSG("MercuryController destructor closing server");
  this->_rdmaListener.reset();
  LOG_CIOS_DEBUG_MSG("MercuryController destructor closing regions");
  this->_largeRegion.reset();
  this->_protectionDomain.reset();
  this->_completionChannel.reset();
  LOG_CIOS_DEBUG_MSG("MercuryController destructor done");
}
/*---------------------------------------------------------------------------*/

//////////////////////////////////////////////////////
//
// This file contains the function to generate a 32 bit
// CRC utilizing a 16 entry table.
//
//
// The CRC polynomial used here is:
//
// x^32 + x^26 + x^23 + x^22 + x^16 +
// x^12 + x^11 + x^10 + x^8 + x^7 + x^5 + x^4 + x^1 + x^0
//
//////////////////////////////////////////////////////

/*---------------------------------------------------------------------------*/
static uint32_t Crc32x4_Table[16] = {
    0x00000000, 0x1db71064, 0x3b6e20c8, 0x26d930ac,
    0x76dc4190, 0x6b6b51f4, 0x4db26158, 0x5005713c,
    0xedb88320, 0xf00f9344, 0xd6d6a3e8, 0xcb61b38c,
    0x9b64c2b0, 0x86d3d2d4, 0xa00ae278, 0xbdbdf21c
};
/*---------------------------------------------------------------------------*/

//////////////////////////////////////////////////////
//
// Calcuate the CRC for a given buffer of data.
// To do just one buffer start with an ulInitialCrc of 0.
// To continue a multibuffer CRC provide the value
// returned from the last call to Crc32n as the ulInitialCrcValue.
//
// inputs:
//    ulInitialCrc -- initial value for the CRC.
//    pData -- pointer to the data to calculate the CRC for.
//    ulLen -- length of the data to calculate the CRC for.
// outputs:
//    returns -- the CRC of the buffer.
//
//////////////////////////////////////////////////////
uint32_t crc32n(uint32_t ulInitialCrc,
                     unsigned char *pData,
                     uint32_t ulLen)
{
  uint32_t n;
  uint32_t t;
  unsigned char *p;
  uint32_t ulCrc = ulInitialCrc;

  for (n = ulLen, p = pData; n > 0; n--, p++) {
    unsigned char c;
    c = *p;                     // gbrab the character.

    t = ulCrc ^ (c & 0x0f);                         // lower nibble
    ulCrc = (ulCrc >> 4) ^ Crc32x4_Table[t & 0xf];
    t = (uint32_t)(ulCrc ^ ((uint32_t)(c >> 4)));               // upper nibble.
    ulCrc = (ulCrc >> 4) ^ Crc32x4_Table[t & 0xf];
  }

  return (ulCrc);
}
/*---------------------------------------------------------------------------*/
int MercuryController::startup()
{
   // Find the address of the I/O link device.
   RdmaDevicePtr linkDevice;
   try {
      LOG_CIOS_DEBUG_MSG("creating InfiniBand device for " << _device << " using interface " << _interface);
      linkDevice = RdmaDevicePtr(new RdmaDevice(_device, _interface ));
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error opening InfiniBand device: " << e.what());
      return e.errcode();
   }
   LOG_CIOS_DEBUG_MSG("created InfiniBand device for " << linkDevice->getDeviceName() << " using interface " << linkDevice->getInterfaceName());

   in_addr_t addr2 = linkDevice->getAddress();
   LOG_CIOS_DEBUG_MSG("Device returns IP address "
           << (int)((uint8_t*)&addr2)[0] << "."
           << (int)((uint8_t*)&addr2)[1] << "."
           << (int)((uint8_t*)&addr2)[2] << "."
           << (int)((uint8_t*)&addr2)[3] << "."
       );

   // Create listener for RDMA connections.
   try {
      _rdmaListener = bgcios::RdmaServerPtr(new bgcios::RdmaServer(linkDevice->getAddress(), this->_port));
      if (_rdmaListener->getLocalPort()!=this->_port) {
        this->_port = _rdmaListener->getLocalPort();
        LOG_CIOS_DEBUG_MSG("RdmaServer port changed to " << this->_port);
      }
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error creating listening RDMA connection: " << e.what());
      return e.errcode();
   }

   in_addr_t addr = linkDevice->getAddress();
   LOG_CIOS_DEBUG_MSG("created listening RDMA connection on port " << this->_port << " using address " << linkDevice->getAddress()
       << "\t IP address "
       << (int)((uint8_t*)&addr)[0] << "."
       << (int)((uint8_t*)&addr)[1] << "."
       << (int)((uint8_t*)&addr)[2] << "."
       << (int)((uint8_t*)&addr)[3] << "."
   );

   // Create a protection domain object.
   try {
      _protectionDomain = RdmaProtectionDomainPtr(new RdmaProtectionDomain(_rdmaListener->getContext()));
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error allocating protection domain: " << e.what());
      return e.errcode();
   }
   LOG_CIOS_DEBUG_MSG("created protection domain " << _protectionDomain->getHandle());

   // Create a completion channel object.
   try {
      _completionChannel = RdmaCompletionChannelPtr(new RdmaCompletionChannel(_rdmaListener->getContext(), false));
   }
   catch (bgcios::RdmaError& e) {
      LOG_ERROR_MSG("error constructing completion channel: " << e.what());
      return e.errcode();
   }
   LOG_CIOS_DEBUG_MSG("created completion channel using fd " << _completionChannel->getChannelFd());

   // Listen for connections.
   int err = _rdmaListener->listen(256);
   if (err != 0) {
      LOG_ERROR_MSG("error listening for new RDMA connections: " << bgcios::errorString(err));
      return err;
   }
   LOG_CIOS_DEBUG_MSG("listening for new RDMA connections on fd " << _rdmaListener->getEventChannelFd());

   // Create a large memory region.
   _largeRegion = RdmaMemoryRegionPtr(new RdmaMemoryRegion());
   err = _largeRegion->allocate(_protectionDomain, LargeRegionSize);
   if (err != 0)
   {
       LOG_ERROR_MSG("error allocating large memory region: " << bgcios::errorString(err));
       return err;
   }
   LOG_CIOS_DEBUG_MSG("created large memory region with local key " << _largeRegion->getLocalKey());

   return 0;
}
/*---------------------------------------------------------------------------*/
int MercuryController::cleanup(void)
{
  return 0;
}
/*---------------------------------------------------------------------------*/
void MercuryController::eventMonitor(int Nevents)
{
  static int counter = 0;
  if (counter++ % 50000 == 0 || Nevents>0){
//    LOG_DEBUG_MSG("entering event monitor for N : %d\n" << Nevents);
//    printf("entering event monitor for %d %d \n", Nevents, counter);
  }
  const int eventChannel = 0;
  const int compChannel  = 1;
  const int numFds       = 2;
  //
  bool _done = false;

  pollfd pollInfo[numFds];
  int polltimeout = 0; // seconds*1000; // 10000 == 10 sec

  LOG_CIOS_TRACE_MSG("Polling on File Descriptor " << _rdmaListener->getEventChannelFd());
  pollInfo[eventChannel].fd = _rdmaListener->getEventChannelFd();
  pollInfo[eventChannel].events = POLLIN;
  pollInfo[eventChannel].revents = 0;

  pollInfo[compChannel].fd = _completionChannel->getChannelFd();
  pollInfo[compChannel].events = POLLIN;
  pollInfo[compChannel].revents = 0;

  // Process events until told to stop - or timeout.
  while (!_done)
  {

    // Wait for an event on one of the descriptors.
    int rc = poll(pollInfo, numFds, polltimeout);

    // There was no data so try again.
    if (rc == 0)
    {
      if (Nevents>0) continue;
      else break;
    }
    LOG_CIOS_TRACE_MSG("Got something");

    // There was an error so log the failure and try again.
    if (rc == -1)
    {
      int err = errno;
      if (err == EINTR)
      {
        LOG_CIOS_TRACE_MSG("poll returned EINTR, continuing ...");
        continue;
      }
      LOG_ERROR_MSG("error polling socket descriptors: " << bgcios::errorString(err));
      return;
    }

    // Check for an event on the event channel.
    if (pollInfo[eventChannel].revents & POLLIN)
    {
      LOG_CIOS_TRACE_MSG("input event available on event channel");
      eventChannelHandler();
      pollInfo[eventChannel].revents = 0;
      Nevents--;
    }

    // Check for an event on the completion channel.
    if (pollInfo[compChannel].revents & POLLIN)
    {
      LOG_CIOS_TRACE_MSG("input event available on data channel");
      completionChannelHandler(0);
      pollInfo[compChannel].revents = 0;
      Nevents--;
    }

    if (Nevents<=0)
    {
      _done = true;
    }
  }

  return;
}
/*---------------------------------------------------------------------------*/
void MercuryController::eventChannelHandler(void)
{
   int err;

   // Wait for the event (it should be here now).
   err = _rdmaListener->waitForEvent();

   if (err != 0) {
      return;
   }

   // Handle the event.
   rdma_cm_event_type type = _rdmaListener->getEventType();

   switch (type) {

      case RDMA_CM_EVENT_CONNECT_REQUEST:
      {
         printf("RDMA_CM_EVENT_CONNECT_REQUEST in event channel handler\n");
         // Construct a RdmaCompletionQueue object for the new client.
         RdmaCompletionQueuePtr completionQ;
         try {
             completionQ = RdmaCompletionQueuePtr(new RdmaCompletionQueue(_rdmaListener->getEventContext(), RdmaCompletionQueue::MaxQueueSize, _completionChannel->getChannel()));
         }
         catch (bgcios::RdmaError& e) {
            LOG_ERROR_MSG("error creating completion queue: " << e.what());
            return;
         }

         // Construct a new RdmaClient object for the new client.
         RdmaClientPtr client;
         try {
             client = RdmaClientPtr(new RdmaClient(_rdmaListener->getEventId(), _protectionDomain, completionQ));
         }
         catch (bgcios::RdmaError& e) {
           LOG_ERROR_MSG("error creating rdma client: %s\n" << e.what());
           completionQ.reset();
           return;
         }

         printf("qpnum = %d\n", client->getQpNum());
         // Add new client to map of active clients.
         _clients.add(client->getQpNum(), client);

         // Add completion queue to completion channel.
         _completionChannel->addCompletionQ(completionQ);

         LOG_DEBUG_MSG("Pre-posting a receive to get the first message");
         // Post a receive to get the first message.
         client->postRecvMessage();
         // we need the wr_id that the request corresponds to
         std::pair<uint32_t,uint64_t> temp = std::make_pair(client->getQpNum(), client->getLastPostRecvKey());
         LOG_DEBUG_MSG("New connection posted unexpected receive with wr_id " << temp.second);

         // Accept the connection from the new client.
         err = client->accept();
         if (err != 0) {
             printf("error accepting client connection: %s\n", bgcios::errorString(err));
            _clients.remove(client->getQpNum());
            _completionChannel->removeCompletionQ(completionQ);
            client->reject(); // Tell client the bad news
            client.reset();
            completionQ.reset();
            break;
         }
         // Add connection qp to new connections for retrieval by main app when receiving unexpected
         LOG_DEBUG_MSG("Adding a new connection for client");
         _newConnections.push_back(temp);
         if (this->_connectionFunction) {
           LOG_DEBUG_MSG("Calling connectionFunction callback");
           this->_connectionFunction(temp, client);
         }
         printf("accepted connection from %s\n", client->getRemoteAddressString().c_str());
//         cout << client->getTag() << "connection accepted from " << client->getRemoteAddressString() << " is using completion queue " << completionQ->getHandle() << endl;

         break;
      }

      case RDMA_CM_EVENT_ESTABLISHED:
      {
         printf("RDMA_CM_EVENT_ESTABLISHED\n");
         // Find connection associated with this event.
         RdmaClientPtr client = _clients.get(_rdmaListener->getEventQpNum());
         LOG_CIOS_INFO_MSG(client->getTag() << "connection established with " << client->getRemoteAddressString());

         break;
      }

      case RDMA_CM_EVENT_DISCONNECTED:
      {
         printf("RDMA_CM_EVENT_DISCONNECTED\n");
         // Find connection associated with this event.
         uint32_t qp = _rdmaListener->getEventQpNum();
         RdmaClientPtr client = _clients.get(qp);
         RdmaCompletionQueuePtr completionQ = client->getCompletionQ();

         // Complete disconnect initiated by peer.
         err = client->disconnect(false);
         if (err == 0) {
            LOG_CIOS_INFO_MSG(client->getTag() << "disconnected from " << client->getRemoteAddressString());
         }
         else {
            LOG_ERROR_MSG(client->getTag() << "error disconnecting from peer: " << bgcios::errorString(err));
         }

         // Acknowledge the event (must be done before removing the rdma cm id).
         _rdmaListener->ackEvent();

         // Remove connection from map of active connections.
         _clients.remove(qp);

         // Destroy connection object.
         LOG_CIOS_DEBUG_MSG("destroying RDMA connection to client " << client->getRemoteAddressString());
         client.reset();

         // Remove completion queue from the completion channel.
         _completionChannel->removeCompletionQ(completionQ);

         // Destroy the completion queue.
         LOG_CIOS_DEBUG_MSG("destroying completion queue " << completionQ->getHandle());
         completionQ.reset();

         //clear any job info for the connection--find Job ID(s) associated with client, and handle like CloseStdio message
         //_jobs.clear();

         break;
      }

      default:
      {
          printf("RDMA event: %s is not supported\n", rdma_event_str(type));
          break;
      }
   }

   // Acknowledge the event.  Should this always be done?
   if (type != RDMA_CM_EVENT_DISCONNECTED) {
      _rdmaListener->ackEvent();
   }

   return;
}
/*---------------------------------------------------------------------------*/
bool MercuryController::completionChannelHandler(uint64_t requestId)
{
  bool rc = false;
  uint64_t* ptr;
  uint32_t rdma_rkey;
  uint32_t rdma_len;
  uint64_t rdma_addr;
  uint32_t rdma_err;
  char     *Message_text;
  char     *Data_text;
  ErrorAckMessage *outMsg;
  RdmaClientPtr client;
  try {
    // Get the notification event from the completion channel.
    RdmaCompletionQueuePtr completionQ = _completionChannel->getEvent();

    // Remove work completions from the completion queue until it is empty.
    while (completionQ->removeCompletions() != 0) {

      // Get the next work completion.
      struct ibv_wc *completion = completionQ->popCompletion();

      // Find the connection that received a message.
      client = _clients.get(completion->qp_num);

      printf("\n\nHandling a completion\n\n");

      if (this->_completionFunction) {
        printf("\n\nCalling completion function\n\n");
        this->_completionFunction(completion, client);
      }
    }
    printf("done processing completions\n");
  }

  catch (const RdmaError& e) {
    LOG_ERROR_MSG("error removing work completions from completion queue: " << bgcios::errorString(e.errcode()));
  }

  return rc;
}



/*
            // Handle the message.
            bgcios::MessageHeader *msghdr = (bgcios::MessageHeader *)client->getInboundMessagePtr();

            switch (msghdr->type) {
              case 1:
                //
                // legacy code, ignore this, left for future use
                //
                printf("write message type %d received from client %s\n", msghdr->type, bgcios::printHeader(*msghdr).c_str());
                ptr = (uint64_t*)(msghdr+1);
                rdma_rkey = (uint32_t)ptr[0];
                rdma_addr = ptr[1];
                rdma_len  = (uint32_t)ptr[2];
                printf("getting data rkey=%d  raddr=%lx  rlen=%d\n", rdma_rkey, rdma_addr, rdma_len);
                getData(client, rdma_addr, rdma_rkey, rdma_len);
                printf("received data CRC = %x   1st words=%lx %lx %lx %lx\n", crc32n(0, (unsigned char*)_largeRegion->getAddress(), rdma_len), ((uint64_t*)_largeRegion->getAddress())[0], ((uint64_t*)_largeRegion->getAddress())[1], ((uint64_t*)_largeRegion->getAddress())[2], ((uint64_t*)_largeRegion->getAddress())[3]);

                //                            outMsg = (ErrorAckMessage *)client->getOutboundMessagePtr();
                //                            memcpy(&(outMsg->header), msghdr, sizeof(MessageHeader));
                //                            outMsg->header.type = ErrorAck;
                //                            outMsg->header.returnCode = bgcios::UnsupportedType;
                //                            outMsg->header.errorCode = 0;
                //                            outMsg->header.length = sizeof(ErrorAckMessage);
                //                            client->setOutboundMessageLength(outMsg->header.length);
                break;

              case 2:
                //
                // legacy code, ignore this, left for future use
                //
                printf("read message type %d received from client %s\n", msghdr->type, bgcios::printHeader(*msghdr).c_str());
                ptr = (uint64_t*)(msghdr+1);
                rdma_rkey = (uint32_t)ptr[0];
                rdma_addr = ptr[1];
                rdma_len  = (uint32_t)ptr[2];

                memset(_largeRegion->getAddress(), 0xa7, rdma_len);
                printf("putting data rkey=%d  raddr=%lx  rlen=%d  CRC %x\n", rdma_rkey, rdma_addr, rdma_len, crc32n(0, (unsigned char*)_largeRegion->getAddress(), rdma_len));

                putData(client, rdma_addr, rdma_rkey, rdma_len);
                printf("pushed data\n");

                outMsg = (ErrorAckMessage *)client->getOutboundMessagePtr();
                memcpy(&(outMsg->header), msghdr, sizeof(MessageHeader));
                outMsg->header.type = ErrorAck;
                outMsg->header.returnCode = bgcios::UnsupportedType;
                outMsg->header.errorCode = 0;
                outMsg->header.length = sizeof(ErrorAckMessage);
                client->setOutboundMessageLength(outMsg->header.length);
                break;

              case 3:
                //
                // legacy code, ignore this, left for future use
                //
                printf("Text message type %d received from client %s\n", msghdr->type, bgcios::printHeader(*msghdr).c_str());
                ptr = (uint64_t*)(msghdr+1);
                rdma_rkey =  (uint32_t)ptr[0];
                rdma_addr =            ptr[1];
                rdma_len  =  (uint32_t)ptr[2];
                Message_text = (char*)&ptr[3];
                printf("received message text \n\n***\n%s\n***\n", Message_text);

                // test my theory by waiting 45 seconds before fetching data

                printf("getting data rkey=%d  raddr=%lx  rlen=%d\n", rdma_rkey, rdma_addr, rdma_len);
                rdma_err = getData(client, rdma_addr, rdma_rkey, rdma_len);
                if (rdma_err==0) {
                  LOG_CIOS_DEBUG_MSG("RDMA read of client data was successful ");
                  Data_text = (char*)(_largeRegion->getAddress());
                  printf("received Data text %s", Data_text);
                }
                else {
                  LOG_CIOS_DEBUG_MSG("GetData() failed with " << bgcios::errorString(rdma_err));
                }


                //                            outMsg = (ErrorAckMessage *)client->getOutboundMessagePtr();
                //                            memcpy(&(outMsg->header), msghdr, sizeof(MessageHeader));
                //                            outMsg->header.type = ErrorAck;
                //                            outMsg->header.returnCode = bgcios::UnsupportedType;
                //                            outMsg->header.errorCode = 0;
                //                            outMsg->header.length = sizeof(ErrorAckMessage);
                //                            client->setOutboundMessageLength(outMsg->header.length);
                break;

              case CSCS_user_message::UnexpectedMessage:
                printf("Unexpected message type %d received from client %s\n", msghdr->type, bgcios::printHeader(*msghdr).c_str());
                // put all the Unexpected messages onto a processing queue
                if ( addUnexpectedMsg(client, completion->qp_num) ) continue;
                break;

              case CSCS_user_message::ExpectedMessage:
                printf("Expected message type %d received from client %s\n", msghdr->type, bgcios::printHeader(*msghdr).c_str());
                ptr = (uint64_t*)(msghdr+1);
                Message_text = (char*)&ptr[3];
                printf("received message text \n\n***\n%s\n***\n", Message_text);
                // put all the Unexpected messages onto a processing queue
                if ( addUnexpectedMsg(client, completion->qp_num) ) continue;
                break;

              default:
                printf("unsupported message type %d received from client %s\n", msghdr->type, bgcios::printHeader(*msghdr).c_str());
                outMsg = (ErrorAckMessage *)client->getOutboundMessagePtr();
                memcpy(&(outMsg->header), msghdr, sizeof(MessageHeader));
                outMsg->header.type = ErrorAck;
                outMsg->header.returnCode = bgcios::UnsupportedType;
                outMsg->header.errorCode = 0;
                outMsg->header.length = sizeof(ErrorAckMessage);
                client->setOutboundMessageLength(outMsg->header.length);

                break;
            }

            printf("posting receive\n");
            // Post a receive to get next message.
            client->postRecvMessage();

            // Send reply message in outbound message buffer to client.
            if (client->isOutboundMessageReady()) {
              printf("posting send\n");
              client->postSendMessage();
              printf("send posted\n");
            }

          }
          break;
        }

        case IBV_WC_RDMA_READ:
        {
          if (completion->wr_id == requestId) {
            rc = true;
          }

          LOG_CIOS_DEBUG_MSG("rdma read operation completed successfully for queue pair " << completion->qp_num);
          break;
        }

        default:
        {
          LOG_ERROR_MSG("unsupported operation " << completion->opcode << " in work completion");
          break;
        }
      }
      */
/*---------------------------------------------------------------------------*/
std::pair<uint32_t,uint64_t> MercuryController::getNewConnection()
{
  std::pair<uint32_t,uint64_t> result;
  this->eventMonitor(0);
  if (_newConnections.empty()) {
    throw std::runtime_error("No new connections available to pop");
  }
  else {
    result = _newConnections.front();
    _newConnections.pop_front();
  }
  return result;
}
/*---------------------------------------------------------------------------*/
bool MercuryController::addUnexpectedMsg(const RdmaClientPtr & client, uint32_t qp_id)
{
  // Get pointer to inbound WriteStdio message.
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();

  /*
   // Build WriteStdioAck message in outbound message region.
   WriteStdioAckMessage *outMsg = (WriteStdioAckMessage *)client->getOutboundMessagePtr();
   memcpy(&(outMsg->header), &(inMsg->header), sizeof(MessageHeader));
   outMsg->header.type = inMsg->header.type == WriteStdout ? WriteStdoutAck : WriteStderrAck;
   outMsg->header.length = sizeof(WriteStdioAckMessage);
   client->setOutboundMessageLength(outMsg->header.length);
   */
  // Validate the job id.
  //   const JobPtr& job = _jobs.get(inMsg->header.jobId);

  _dequeUnexpectedInClient.push_back(ClientMapPair(qp_id,client));
  return true;
}
/*---------------------------------------------------------------------------*/
bool MercuryController::addExpectedMsg(const RdmaClientPtr & client, uint32_t qp_id)
{
  // Get pointer to inbound WriteStdio message.
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();

  /*
   // Build WriteStdioAck message in outbound message region.
   WriteStdioAckMessage *outMsg = (WriteStdioAckMessage *)client->getOutboundMessagePtr();
   memcpy(&(outMsg->header), &(inMsg->header), sizeof(MessageHeader));
   outMsg->header.type = inMsg->header.type == WriteStdout ? WriteStdoutAck : WriteStderrAck;
   outMsg->header.length = sizeof(WriteStdioAckMessage);
   client->setOutboundMessageLength(outMsg->header.length);
   */
  // Validate the job id.
  //   const JobPtr& job = _jobs.get(inMsg->header.jobId);

  _dequeExpectedInClient.push_back(ClientMapPair(qp_id,client));
  return true;
}
/*---------------------------------------------------------------------------*/
bool MercuryController::fetchUnexpectedMsg(void *buf, uint64_t buf_size, uint32_t &qp_id)
{
  RdmaClientPtr client;
  if (!_dequeUnexpectedInClient.empty() )
  {
    client = _dequeUnexpectedInClient.front().second;
    qp_id = _dequeUnexpectedInClient.front().first;
    _dequeUnexpectedInClient.pop_front();
  }
  else
  {
    throw std::runtime_error("Nothing to receive in unexpected queue");
    return false;
  }
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();
//  if (buf_size>inMsg->header2.cnk_bytes) throw std::runtime_error("Not enough space for message in UserRDMA msg");
  //
//  memcpy(buf, inMsg->MessageData, inMsg->header2.cnk_bytes);

  return true;
}
/*---------------------------------------------------------------------------*/
bool MercuryController::fetchExpectedMsg(void *buf, uint64_t buf_size, uint32_t &qp_id)
{
  RdmaClientPtr client;
  if (!_dequeExpectedInClient.empty() )
  {
    client = _dequeExpectedInClient.front().second;
    qp_id = _dequeExpectedInClient.front().first;
    _dequeExpectedInClient.pop_front();
  }
  else
  {
    return false;
  }
  CSCS_user_message::UserRDMA_message *inMsg = (CSCS_user_message::UserRDMA_message *)client->getInboundMessagePtr();
//  if (buf_size>inMsg->header2.cnk_bytes) throw std::runtime_error("Not enough space for message in UserRDMA msg");
  //
//  memcpy(buf, inMsg->MessageData, inMsg->header2.cnk_bytes);

  return true;
}

/*
auto start_time = std::chrono::high_resolution_clock::now();

auto t2 = std::chrono::high_resolution_clock::now();
auto sec = std::chrono::duration_cast < std::chrono::seconds
    > (t2 - start_time).count();
     if (sec>seconds) _done=true;
*/
