#pragma once

#ifndef ai_train_H_
#define ai_train_H_

/*************************************************************/
/*           ���ļ��ɱ��������ɣ����������޸�                */
/*************************************************************/
#include "OsResource.h"
#include "ZRSequence.h"
#include "TypeCode.h"
#include "CDRStream.h"
#include "ZRDDSCppWrapper.h"
#include "ZRBuiltinTypes.h"

typedef struct ZRMemPool ZRMemPool;


namespace ai_train {
    typedef DDS_ULong RoundId;

    typedef DDS_ULong ClientId;

    DDS_USER_SEQUENCE_CPP(Bytes, DDS_Octet);

    typedef struct TrainCmd {
        ai_train::RoundId round_id; // @ID(0)
        DDS_ULong subset_size; // @ID(1)
        DDS_ULong epochs; // @ID(2)
        DDS_Double lr; // @ID(3)
        DDS_ULong seed; // @ID(4)
    } TrainCmd; // @Extensibility(EXTENSIBLE)

    DDS_USER_SEQUENCE_CPP(TrainCmdSeq, TrainCmd);

    // �û�ʹ�ýӿ�
    DDS_Boolean TrainCmdInitialize(
        TrainCmd* self);

    DDS_Boolean TrainCmdInitializeEx(
        TrainCmd* self,
        ZRMemPool* pool,
        DDS_Boolean allocateMemory);

    void TrainCmdFinalize(
        TrainCmd* self);

    void TrainCmdFinalizeEx(
        TrainCmd* self,
        ZRMemPool* pool,
        DDS_Boolean deletePointers);

    DDS_Boolean TrainCmdCopy(
        TrainCmd* dst,
        const TrainCmd* src);

    DDS_Boolean TrainCmdCopyEx(
        TrainCmd* dst,
        const TrainCmd* src,
        ZRMemPool* pool);

    void TrainCmdPrintData(
        const TrainCmd* sample);

    DDS::TypeCode* TrainCmdGetTypeCode();

    // �ײ�ʹ�ú���
    TrainCmd* TrainCmdCreateSample(
        ZRMemPool* pool,
        DDS_Boolean allocMutable);

    void TrainCmdDestroySample(
        ZRMemPool* pool,
        TrainCmd* sample);

    DDS_ULong TrainCmdGetSerializedSampleMaxSize();

    DDS_ULong TrainCmdGetSerializedSampleSize(
        const TrainCmd* sample,
        DDS_ULong currentAlignment);

    DDS_Long TrainCmdSerialize(
        const TrainCmd* sample,
        CDRSerializer* cdr);

    DDS_Long TrainCmdDeserialize(
        TrainCmd* sample,
        CDRDeserializer* cdr,
        ZRMemPool* pool);

    DDS_ULong TrainCmdGetSerializedKeyMaxSize();

    DDS_ULong TrainCmdGetSerializedKeySize(
        const TrainCmd* sample,
        DDS_ULong currentAlignment);

    DDS_Long TrainCmdSerializeKey(
        const TrainCmd* sample,
        CDRSerializer* cdr);

    DDS_Long TrainCmdDeserializeKey(
        TrainCmd* sample,
        CDRDeserializer* cdr,
        ZRMemPool* pool);

    DDS_Long TrainCmdGetKeyHash(
        const TrainCmd* sample,
        CDRSerializer* cdr,
        DDS::KeyHash_t* result);

    DDS_Boolean TrainCmdHasKey();

    TypeCodeHeader* TrainCmdGetInnerTypeCode();

#ifdef _ZRDDS_INCLUDE_ONSITE_DESERILIZE
    DDS_Boolean TrainCmdNoSerializingSupported();

    DDS_ULong TrainCmdFixedHeaderLength();

    DDS_Long TrainCmdOnSiteDeserialize(CDRDeserializer* cdr,
                                       TrainCmd* sample,
                                       DDS_ULong offset,
                                       DDS_ULong totalSize,
                                       DDS_Char* payload,
                                       DDS_ULong payloadLen,
                                       DDS_ULong fixedHeaderLen);

#endif/*_ZRDDS_INCLUDE_ONSITE_DESERILIZE*/

#ifdef _ZRDDS_INCLUDE_NO_SERIALIZE_MODE
    DDS_Char* TrainCmdLoanSampleBuf(TrainCmd* sample, DDS_Boolean takeBuffer);

    void TrainCmdReturnSampleBuf(DDS_Char* sampleBuf);

    DDS_Long TrainCmdLoanDeserialize(TrainCmd* sampleBuf,
                                     CDRDeserializer* cdr,
                                     DDS_ULong curIndex,
                                     DDS_ULong totalNum,
                                     DDS_Char* base,
                                     DDS_ULong offset,
                                     DDS_ULong space,
                                     DDS_ULong fixedHeaderLen);

#endif/*_ZRDDS_INCLUDE_NO_SERIALIZE_MODE*/
    typedef struct ClientUpdate {
        ai_train::ClientId client_id; // @ID(0)
        ai_train::RoundId round_id; // @ID(1)
        DDS_ULongLong num_samples; // @ID(2)
        ai_train::Bytes data; // @ID(3)
    } ClientUpdate; // @Extensibility(EXTENSIBLE)

    DDS_USER_SEQUENCE_CPP(ClientUpdateSeq, ClientUpdate);

    // �û�ʹ�ýӿ�
    DDS_Boolean ClientUpdateInitialize(
        ClientUpdate* self);

    DDS_Boolean ClientUpdateInitializeEx(
        ClientUpdate* self,
        ZRMemPool* pool,
        DDS_Boolean allocateMemory);

    void ClientUpdateFinalize(
        ClientUpdate* self);

    void ClientUpdateFinalizeEx(
        ClientUpdate* self,
        ZRMemPool* pool,
        DDS_Boolean deletePointers);

    DDS_Boolean ClientUpdateCopy(
        ClientUpdate* dst,
        const ClientUpdate* src);

    DDS_Boolean ClientUpdateCopyEx(
        ClientUpdate* dst,
        const ClientUpdate* src,
        ZRMemPool* pool);

    void ClientUpdatePrintData(
        const ClientUpdate* sample);

    DDS::TypeCode* ClientUpdateGetTypeCode();

    // �ײ�ʹ�ú���
    ClientUpdate* ClientUpdateCreateSample(
        ZRMemPool* pool,
        DDS_Boolean allocMutable);

    void ClientUpdateDestroySample(
        ZRMemPool* pool,
        ClientUpdate* sample);

    DDS_ULong ClientUpdateGetSerializedSampleMaxSize();

    DDS_ULong ClientUpdateGetSerializedSampleSize(
        const ClientUpdate* sample,
        DDS_ULong currentAlignment);

    DDS_Long ClientUpdateSerialize(
        const ClientUpdate* sample,
        CDRSerializer* cdr);

    DDS_Long ClientUpdateDeserialize(
        ClientUpdate* sample,
        CDRDeserializer* cdr,
        ZRMemPool* pool);

    DDS_ULong ClientUpdateGetSerializedKeyMaxSize();

    DDS_ULong ClientUpdateGetSerializedKeySize(
        const ClientUpdate* sample,
        DDS_ULong currentAlignment);

    DDS_Long ClientUpdateSerializeKey(
        const ClientUpdate* sample,
        CDRSerializer* cdr);

    DDS_Long ClientUpdateDeserializeKey(
        ClientUpdate* sample,
        CDRDeserializer* cdr,
        ZRMemPool* pool);

    DDS_Long ClientUpdateGetKeyHash(
        const ClientUpdate* sample,
        CDRSerializer* cdr,
        DDS::KeyHash_t* result);

    DDS_Boolean ClientUpdateHasKey();

    TypeCodeHeader* ClientUpdateGetInnerTypeCode();

#ifdef _ZRDDS_INCLUDE_ONSITE_DESERILIZE
    DDS_Boolean ClientUpdateNoSerializingSupported();

    DDS_ULong ClientUpdateFixedHeaderLength();

    DDS_Long ClientUpdateOnSiteDeserialize(CDRDeserializer* cdr,
                                           ClientUpdate* sample,
                                           DDS_ULong offset,
                                           DDS_ULong totalSize,
                                           DDS_Char* payload,
                                           DDS_ULong payloadLen,
                                           DDS_ULong fixedHeaderLen);

#endif/*_ZRDDS_INCLUDE_ONSITE_DESERILIZE*/

#ifdef _ZRDDS_INCLUDE_NO_SERIALIZE_MODE
    DDS_Char* ClientUpdateLoanSampleBuf(ClientUpdate* sample, DDS_Boolean takeBuffer);

    void ClientUpdateReturnSampleBuf(DDS_Char* sampleBuf);

    DDS_Long ClientUpdateLoanDeserialize(ClientUpdate* sampleBuf,
                                         CDRDeserializer* cdr,
                                         DDS_ULong curIndex,
                                         DDS_ULong totalNum,
                                         DDS_Char* base,
                                         DDS_ULong offset,
                                         DDS_ULong space,
                                         DDS_ULong fixedHeaderLen);

#endif/*_ZRDDS_INCLUDE_NO_SERIALIZE_MODE*/
    typedef struct ModelBlob {
        ai_train::RoundId round_id; // @ID(0)
        ai_train::Bytes data; // @ID(1)
    } ModelBlob; // @Extensibility(EXTENSIBLE)

    DDS_USER_SEQUENCE_CPP(ModelBlobSeq, ModelBlob);

    // �û�ʹ�ýӿ�
    DDS_Boolean ModelBlobInitialize(
        ModelBlob* self);

    DDS_Boolean ModelBlobInitializeEx(
        ModelBlob* self,
        ZRMemPool* pool,
        DDS_Boolean allocateMemory);

    void ModelBlobFinalize(
        ModelBlob* self);

    void ModelBlobFinalizeEx(
        ModelBlob* self,
        ZRMemPool* pool,
        DDS_Boolean deletePointers);

    DDS_Boolean ModelBlobCopy(
        ModelBlob* dst,
        const ModelBlob* src);

    DDS_Boolean ModelBlobCopyEx(
        ModelBlob* dst,
        const ModelBlob* src,
        ZRMemPool* pool);

    void ModelBlobPrintData(
        const ModelBlob* sample);

    DDS::TypeCode* ModelBlobGetTypeCode();

    // �ײ�ʹ�ú���
    ModelBlob* ModelBlobCreateSample(
        ZRMemPool* pool,
        DDS_Boolean allocMutable);

    void ModelBlobDestroySample(
        ZRMemPool* pool,
        ModelBlob* sample);

    DDS_ULong ModelBlobGetSerializedSampleMaxSize();

    DDS_ULong ModelBlobGetSerializedSampleSize(
        const ModelBlob* sample,
        DDS_ULong currentAlignment);

    DDS_Long ModelBlobSerialize(
        const ModelBlob* sample,
        CDRSerializer* cdr);

    DDS_Long ModelBlobDeserialize(
        ModelBlob* sample,
        CDRDeserializer* cdr,
        ZRMemPool* pool);

    DDS_ULong ModelBlobGetSerializedKeyMaxSize();

    DDS_ULong ModelBlobGetSerializedKeySize(
        const ModelBlob* sample,
        DDS_ULong currentAlignment);

    DDS_Long ModelBlobSerializeKey(
        const ModelBlob* sample,
        CDRSerializer* cdr);

    DDS_Long ModelBlobDeserializeKey(
        ModelBlob* sample,
        CDRDeserializer* cdr,
        ZRMemPool* pool);

    DDS_Long ModelBlobGetKeyHash(
        const ModelBlob* sample,
        CDRSerializer* cdr,
        DDS::KeyHash_t* result);

    DDS_Boolean ModelBlobHasKey();

    TypeCodeHeader* ModelBlobGetInnerTypeCode();

#ifdef _ZRDDS_INCLUDE_ONSITE_DESERILIZE
    DDS_Boolean ModelBlobNoSerializingSupported();

    DDS_ULong ModelBlobFixedHeaderLength();

    DDS_Long ModelBlobOnSiteDeserialize(CDRDeserializer* cdr,
                                        ModelBlob* sample,
                                        DDS_ULong offset,
                                        DDS_ULong totalSize,
                                        DDS_Char* payload,
                                        DDS_ULong payloadLen,
                                        DDS_ULong fixedHeaderLen);

#endif/*_ZRDDS_INCLUDE_ONSITE_DESERILIZE*/

#ifdef _ZRDDS_INCLUDE_NO_SERIALIZE_MODE
    DDS_Char* ModelBlobLoanSampleBuf(ModelBlob* sample, DDS_Boolean takeBuffer);

    void ModelBlobReturnSampleBuf(DDS_Char* sampleBuf);

    DDS_Long ModelBlobLoanDeserialize(ModelBlob* sampleBuf,
                                      CDRDeserializer* cdr,
                                      DDS_ULong curIndex,
                                      DDS_ULong totalNum,
                                      DDS_Char* base,
                                      DDS_ULong offset,
                                      DDS_ULong space,
                                      DDS_ULong fixedHeaderLen);

#endif/*_ZRDDS_INCLUDE_NO_SERIALIZE_MODE*/
}
#endif
