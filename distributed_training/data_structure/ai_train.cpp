/*************************************************************/
/*           ���ļ��ɱ��������ɣ����������޸�                */
/*************************************************************/
#include "ZRMemPool.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ai_train.h"

namespace ai_train
{

#define T DDS_Octet
#define TSeq Bytes

#include "ZRSequence.cpp"
#include "ZRCPlusPlusSequence.cpp"

#undef TSeq
#undef T

#define T TrainCmd
#define TSeq TrainCmdSeq
#define TINITIALIZE TrainCmdInitializeEx
#define TFINALIZE TrainCmdFinalizeEx
#define TCOPY TrainCmdCopyEx

#include "ZRSequence.cpp"
#include "ZRCPlusPlusSequence.cpp"

#undef TCOPY
#undef TFINALIZE
#undef TINITIALIZE
#undef TSeq
#undef T

    DDS_Boolean TrainCmdInitialize(TrainCmd *self)
    {
        return TrainCmdInitializeEx(self, NULL, true);
    }

    void TrainCmdFinalize(TrainCmd *self)
    {
        TrainCmdFinalizeEx(self, NULL, true);
    }

    DDS_Boolean TrainCmdCopy(
        TrainCmd *dst,
        const TrainCmd *src)
    {
        return TrainCmdCopyEx(dst, src, NULL);
    }

    TrainCmd *TrainCmdCreateSample(
        ZRMemPool *pool,
        DDS_Boolean allocMutable)
    {
        TrainCmd *newSample = (TrainCmd *)ZRMalloc(pool, sizeof(TrainCmd));
        if (newSample == NULL)
        {
            printf("malloc for TrainCmd failed.");
            return NULL;
        }
        if (!TrainCmdInitializeEx(newSample, pool, allocMutable))
        {
            printf("initial Sample failed.");
            TrainCmdDestroySample(pool, newSample);
            return NULL;
        }
        return newSample;
    }

    void TrainCmdDestroySample(ZRMemPool *pool, TrainCmd *sample)
    {
        if (sample == NULL)
            return;
        TrainCmdFinalizeEx(sample, pool, true);
        ZRDealloc(pool, sample);
    }

    DDS_ULong TrainCmdGetSerializedSampleMaxSize()
    {
        return 28;
    }

    DDS_ULong TrainCmdGetSerializedKeyMaxSize()
    {
        return 28;
    }

    DDS_Long TrainCmdGetKeyHash(
        const TrainCmd *sample,
        CDRSerializer *cdr,
        DDS::KeyHash_t *result)
    {
        DDS_Long ret = TrainCmdSerializeKey(sample, cdr);
        if (ret < 0)
        {
            printf("serialize key failed.");
            *result = DDS_HANDLE_NIL_NATIVE;
            return -1;
        }
        ret = CDRSerializeGetKeyHash(cdr, result->value, true);
        if (ret < 0)
        {
            printf("get keyhash failed.");
            *result = DDS_HANDLE_NIL_NATIVE;
            return -1;
        }
        result->valid = true;
        return 0;
    }

    DDS_Boolean TrainCmdHasKey()
    {
        return false;
    }

    TypeCodeHeader *TrainCmdGetInnerTypeCode()
    {
#ifdef _ZRDDS_INCLUDE_TYPECODE
        DDS::TypeCode *userTypeCode = TrainCmdGetTypeCode();
        if (userTypeCode == NULL)
            return NULL;
        return userTypeCode->getImpl();
#else
        return NULL;
#endif
    }

    DDS_Boolean TrainCmdInitializeEx(
        TrainCmd *self,
        ZRMemPool *pool,
        DDS_Boolean allocateMemory)
    {
        self->round_id = 0;

        self->subset_size = 0;

        self->epochs = 0;

        self->lr = 0;

        self->seed = 0;

        if (allocateMemory)
        {
        }
        else
        {
        }
        return true;
    }

    void TrainCmdFinalizeEx(
        TrainCmd *self,
        ZRMemPool *pool,
        DDS_Boolean deletePointers)
    {
        if (deletePointers)
        {
        }
    }

    DDS_Boolean TrainCmdCopyEx(
        TrainCmd *dst,
        const TrainCmd *src,
        ZRMemPool *pool)
    {
        dst->round_id = src->round_id;
        dst->subset_size = src->subset_size;
        dst->epochs = src->epochs;
        dst->lr = src->lr;
        dst->seed = src->seed;
        return true;
    }

    void TrainCmdPrintData(const TrainCmd *sample)
    {
        if (sample == NULL)
        {
            printf("NULL\n");
            return;
        }
        printf("sample->round_id: %u\n", sample->round_id);
        printf("\n");

        printf("sample->subset_size: %u\n", sample->subset_size);
        printf("\n");

        printf("sample->epochs: %u\n", sample->epochs);
        printf("\n");

        printf("sample->lr: %lf\n", sample->lr);
        printf("\n");

        printf("sample->seed: %u\n", sample->seed);
        printf("\n");
    }

    DDS::TypeCode *TrainCmdGetTypeCode()
    {
        static DDS::TypeCode *s_typeCode = NULL;
        if (s_typeCode != NULL)
            return s_typeCode;
        DDS::TypeCodeFactory &factory = DDS::TypeCodeFactory::getInstance();

        s_typeCode = factory.createStructTC(
            "ai_train::TrainCmd",
            DDS_EXTENSIBLE_EXTENSIBILITY);
        if (s_typeCode == NULL)
        {
            printf("create struct TrainCmd typecode failed.");
            return s_typeCode;
        }
        DDS_Long ret = 0;
        DDS::TypeCode *memberTc = NULL;
        DDS::TypeCode *eleTc = NULL;

        memberTc = factory.getPrimitiveTC(DDS_TK_UINT);
        if (memberTc == NULL)
        {
            printf("Get Member round_id TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            0,
            0,
            "round_id",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_UINT);
        if (memberTc == NULL)
        {
            printf("Get Member subset_size TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            1,
            1,
            "subset_size",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_UINT);
        if (memberTc == NULL)
        {
            printf("Get Member epochs TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            2,
            2,
            "epochs",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_DOUBLE);
        if (memberTc == NULL)
        {
            printf("Get Member lr TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            3,
            3,
            "lr",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_UINT);
        if (memberTc == NULL)
        {
            printf("Get Member seed TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            4,
            4,
            "seed",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        return s_typeCode;
    }

    DDS_Long TrainCmdSerialize(const TrainCmd *sample, CDRSerializer *cdr)
    {
        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
        {
            printf("serialize sample->round_id failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->subset_size, 4))
        {
            printf("serialize sample->subset_size failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->epochs, 4))
        {
            printf("serialize sample->epochs failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->lr, 8))
        {
            printf("serialize sample->lr failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->seed, 4))
        {
            printf("serialize sample->seed failed.");
            return -2;
        }

        return 0;
    }

    DDS_Long TrainCmdDeserialize(
        TrainCmd *sample,
        CDRDeserializer *cdr,
        ZRMemPool *pool)
    {
        // no key
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
        {
            sample->round_id = 0;
            sample->subset_size = 0;
            sample->epochs = 0;
            sample->lr = 0;
            sample->seed = 0;
            return 0;
        }
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->subset_size, 4))
        {
            sample->subset_size = 0;
            sample->epochs = 0;
            sample->lr = 0;
            sample->seed = 0;
            return 0;
        }
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->epochs, 4))
        {
            sample->epochs = 0;
            sample->lr = 0;
            sample->seed = 0;
            return 0;
        }
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->lr, 8))
        {
            sample->lr = 0;
            sample->seed = 0;
            return 0;
        }
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->seed, 4))
        {
            sample->seed = 0;
            return 0;
        }
        return 0;
    }

    DDS_ULong TrainCmdGetSerializedSampleSize(const TrainCmd *sample, DDS_ULong currentAlignment)
    {
        DDS_ULong initialAlignment = currentAlignment;

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(8, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);

        return currentAlignment - initialAlignment;
    }

    DDS_Long TrainCmdSerializeKey(const TrainCmd *sample, CDRSerializer *cdr)
    {
        if (TrainCmdSerialize(sample, cdr) < 0)
        {
            return -1;
        }
        return 0;
    }

    DDS_Long TrainCmdDeserializeKey(
        TrainCmd *sample,
        CDRDeserializer *cdr,
        ZRMemPool *pool)
    {
        if (TrainCmdDeserialize(sample, cdr, pool) < 0)
        {
            return -1;
        }
        return 0;
    }

    DDS_ULong TrainCmdGetSerializedKeySize(const TrainCmd *sample, DDS_ULong currentAlignment)
    {
        DDS_ULong initialAlignment = currentAlignment;

        currentAlignment += TrainCmdGetSerializedSampleSize(sample, currentAlignment);
        return currentAlignment - initialAlignment;
    }

#ifdef _ZRDDS_INCLUDE_NO_SERIALIZE_MODE
    DDS_Char *TrainCmdLoanSampleBuf(TrainCmd *sample, DDS_Boolean takeBuffer)
    {
        return NULL;
    }

    void TrainCmdReturnSampleBuf(DDS_Char *sampleBuf)
    {
        ;
    }

    DDS_Long TrainCmdLoanDeserialize(TrainCmd *sampleBuf,
                                     CDRDeserializer *cdr,
                                     DDS_ULong curIndex,
                                     DDS_ULong totalNum,
                                     DDS_Char *base,
                                     DDS_ULong offset,
                                     DDS_ULong space,
                                     DDS_ULong fixedHeaderLen)
    {
        return 0;
    }

#endif /*_ZRDDS_INCLUDE_NO_SERIALIZE_MODE*/

#ifdef _ZRDDS_INCLUDE_ONSITE_DESERILIZE
    DDS_Long TrainCmdOnSiteDeserialize(CDRDeserializer *cdr,
                                       TrainCmd *sample,
                                       DDS_ULong offset,
                                       DDS_ULong totalSize,
                                       DDS_Char *payload,
                                       DDS_ULong payloadLen,
                                       DDS_ULong fixedHeaderLen)
    {
        return 0;
    }

    DDS_Boolean TrainCmdNoSerializingSupported()
    {
        return false;
    }

    DDS_ULong TrainCmdFixedHeaderLength()
    {
        return 0;
    }

#endif /*_ZRDDS_INCLUDE_ONSITE_DESERILIZE*/
#define T ClientUpdate
#define TSeq ClientUpdateSeq
#define TINITIALIZE ClientUpdateInitializeEx
#define TFINALIZE ClientUpdateFinalizeEx
#define TCOPY ClientUpdateCopyEx

#include "ZRSequence.cpp"
#include "ZRCPlusPlusSequence.cpp"

#undef TCOPY
#undef TFINALIZE
#undef TINITIALIZE
#undef TSeq
#undef T

    DDS_Boolean ClientUpdateInitialize(ClientUpdate *self)
    {
        return ClientUpdateInitializeEx(self, NULL, true);
    }

    void ClientUpdateFinalize(ClientUpdate *self)
    {
        ClientUpdateFinalizeEx(self, NULL, true);
    }

    DDS_Boolean ClientUpdateCopy(
        ClientUpdate *dst,
        const ClientUpdate *src)
    {
        return ClientUpdateCopyEx(dst, src, NULL);
    }

    ClientUpdate *ClientUpdateCreateSample(
        ZRMemPool *pool,
        DDS_Boolean allocMutable)
    {
        ClientUpdate *newSample = (ClientUpdate *)ZRMalloc(pool, sizeof(ClientUpdate));
        if (newSample == NULL)
        {
            printf("malloc for ClientUpdate failed.");
            return NULL;
        }
        if (!ClientUpdateInitializeEx(newSample, pool, allocMutable))
        {
            printf("initial Sample failed.");
            ClientUpdateDestroySample(pool, newSample);
            return NULL;
        }
        return newSample;
    }

    void ClientUpdateDestroySample(ZRMemPool *pool, ClientUpdate *sample)
    {
        if (sample == NULL)
            return;
        ClientUpdateFinalizeEx(sample, pool, true);
        ZRDealloc(pool, sample);
    }

    DDS_ULong ClientUpdateGetSerializedSampleMaxSize()
    {
        return MAX_UINT32_VALUE;
    }

    DDS_ULong ClientUpdateGetSerializedKeyMaxSize()
    {
        return MAX_UINT32_VALUE;
    }

    DDS_Long ClientUpdateGetKeyHash(
        const ClientUpdate *sample,
        CDRSerializer *cdr,
        DDS::KeyHash_t *result)
    {
        DDS_Long ret = ClientUpdateSerializeKey(sample, cdr);
        if (ret < 0)
        {
            printf("serialize key failed.");
            *result = DDS_HANDLE_NIL_NATIVE;
            return -1;
        }
        ret = CDRSerializeGetKeyHash(cdr, result->value, true);
        if (ret < 0)
        {
            printf("get keyhash failed.");
            *result = DDS_HANDLE_NIL_NATIVE;
            return -1;
        }
        result->valid = true;
        return 0;
    }

    DDS_Boolean ClientUpdateHasKey()
    {
        return false;
    }

    TypeCodeHeader *ClientUpdateGetInnerTypeCode()
    {
#ifdef _ZRDDS_INCLUDE_TYPECODE
        DDS::TypeCode *userTypeCode = ClientUpdateGetTypeCode();
        if (userTypeCode == NULL)
            return NULL;
        return userTypeCode->getImpl();
#else
        return NULL;
#endif
    }

    DDS_Boolean ClientUpdateInitializeEx(
        ClientUpdate *self,
        ZRMemPool *pool,
        DDS_Boolean allocateMemory)
    {
        self->client_id = 0;

        self->round_id = 0;

        self->num_samples = 0;

        ai_train::Bytes_initialize_ex(&self->data, pool, allocateMemory);

        if (allocateMemory)
        {
        }
        else
        {
        }
        return true;
    }

    void ClientUpdateFinalizeEx(
        ClientUpdate *self,
        ZRMemPool *pool,
        DDS_Boolean deletePointers)
    {
        ai_train::Bytes_finalize(&self->data);
        if (deletePointers)
        {
        }
    }

    DDS_Boolean ClientUpdateCopyEx(
        ClientUpdate *dst,
        const ClientUpdate *src,
        ZRMemPool *pool)
    {
        dst->client_id = src->client_id;
        dst->round_id = src->round_id;
        dst->num_samples = src->num_samples;
        if (!ai_train::Bytes_copy(&dst->data, &src->data))
        {
            printf("copy member data failed.");
            return false;
        }
        return true;
    }

    void ClientUpdatePrintData(const ClientUpdate *sample)
    {
        if (sample == NULL)
        {
            printf("NULL\n");
            return;
        }
        printf("sample->client_id: %u\n", sample->client_id);
        printf("\n");

        printf("sample->round_id: %u\n", sample->round_id);
        printf("\n");

        printf("sample->num_samples: %llu\n", sample->num_samples);
        printf("\n");

        DDS_ULong dataTmpLen = ai_train::Bytes_get_length(&sample->data);
        printf("sample->data: %d\n", dataTmpLen);
        for (DDS_ULong i = 0; i < dataTmpLen; ++i)
        {
            printf("sample->data[%u]: 0x%02x\n", i, *ai_train::Bytes_get_reference(&sample->data, i));
        }
        printf("\n");
    }

    DDS::TypeCode *ClientUpdateGetTypeCode()
    {
        static DDS::TypeCode *s_typeCode = NULL;
        if (s_typeCode != NULL)
            return s_typeCode;
        DDS::TypeCodeFactory &factory = DDS::TypeCodeFactory::getInstance();

        s_typeCode = factory.createStructTC(
            "ai_train::ClientUpdate",
            DDS_EXTENSIBLE_EXTENSIBILITY);
        if (s_typeCode == NULL)
        {
            printf("create struct ClientUpdate typecode failed.");
            return s_typeCode;
        }
        DDS_Long ret = 0;
        DDS::TypeCode *memberTc = NULL;
        DDS::TypeCode *eleTc = NULL;

        memberTc = factory.getPrimitiveTC(DDS_TK_UINT);
        if (memberTc == NULL)
        {
            printf("Get Member client_id TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            0,
            0,
            "client_id",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_UINT);
        if (memberTc == NULL)
        {
            printf("Get Member round_id TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            1,
            1,
            "round_id",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_ULONGLONG);
        if (memberTc == NULL)
        {
            printf("Get Member num_samples TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            2,
            2,
            "num_samples",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_UCHAR);
        if (memberTc != NULL)
        {
            memberTc = factory.createSequenceTC(MAX_UINT32_VALUE, memberTc);
        }
        if (memberTc == NULL)
        {
            printf("Get Member data TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            3,
            3,
            "data",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        return s_typeCode;
    }

    DDS_Long ClientUpdateSerialize(const ClientUpdate *sample, CDRSerializer *cdr)
    {
        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->client_id, 4))
        {
            printf("serialize sample->client_id failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
        {
            printf("serialize sample->round_id failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->num_samples, 8))
        {
            printf("serialize sample->num_samples failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&(sample->data)._length, 4))
        {
            printf("serialize length of sample->data failed.");
            return -2;
        }
        if ((sample->data)._contiguousBuffer)
        {
            if (!CDRSerializerPutUntypeArray(cdr, (DDS_Octet *)(sample->data)._contiguousBuffer, (sample->data)._length, 1))
            {
                printf("serialize sample->data failed.");
                return -2;
            }
        }
        else
        {
            for (DDS_ULong i = 0; i < (sample->data)._length; ++i)
            {
                if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&*ai_train::Bytes_get_reference(&sample->data, i), 1))
                {
                    printf("serialize sample->data failed.");
                    return -2;
                }
            }
        }

        return 0;
    }

    DDS_Long ClientUpdateDeserialize(
        ClientUpdate *sample,
        CDRDeserializer *cdr,
        ZRMemPool *pool)
    {
        // no key
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->client_id, 4))
        {
            sample->client_id = 0;
            sample->round_id = 0;
            sample->num_samples = 0;
            ai_train::Bytes_initialize_ex(&sample->data, pool, true);
            return 0;
        }
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
        {
            sample->round_id = 0;
            sample->num_samples = 0;
            ai_train::Bytes_initialize_ex(&sample->data, pool, true);
            return 0;
        }
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->num_samples, 8))
        {
            sample->num_samples = 0;
            ai_train::Bytes_initialize_ex(&sample->data, pool, true);
            return 0;
        }
        DDS_ULong dataTmpLen = 0;
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&dataTmpLen, 4))
        {
            ai_train::Bytes_initialize_ex(&sample->data, pool, true);
            return 0;
        }
        if (!ai_train::Bytes_ensure_length(&sample->data, dataTmpLen, dataTmpLen))
        {
            printf("Set maxiumum member sample->data failed.");
            return -3;
        }
        if (sample->data._contiguousBuffer)
        {
            if (!CDRDeserializerGetUntypeArray(cdr, (DDS_Octet *)sample->data._contiguousBuffer, dataTmpLen, 1))
            {
                printf("deserialize sample->data failed.");
                return -2;
            }
        }
        else
        {
            for (DDS_ULong i = 0; i < dataTmpLen; ++i)
            {
                if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&*ai_train::Bytes_get_reference(&sample->data, i), 1))
                {
                    printf("deserialize sample->data failed.");
                    return -2;
                }
            }
        }
        return 0;
    }

    DDS_ULong ClientUpdateGetSerializedSampleSize(const ClientUpdate *sample, DDS_ULong currentAlignment)
    {
        DDS_ULong initialAlignment = currentAlignment;

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(8, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);
        DDS_ULong dataLen = ai_train::Bytes_get_length(&sample->data);
        if (dataLen != 0)
        {
            currentAlignment += 1 * dataLen;
        }

        return currentAlignment - initialAlignment;
    }

    DDS_Long ClientUpdateSerializeKey(const ClientUpdate *sample, CDRSerializer *cdr)
    {
        if (ClientUpdateSerialize(sample, cdr) < 0)
        {
            return -1;
        }
        return 0;
    }

    DDS_Long ClientUpdateDeserializeKey(
        ClientUpdate *sample,
        CDRDeserializer *cdr,
        ZRMemPool *pool)
    {
        if (ClientUpdateDeserialize(sample, cdr, pool) < 0)
        {
            return -1;
        }
        return 0;
    }

    DDS_ULong ClientUpdateGetSerializedKeySize(const ClientUpdate *sample, DDS_ULong currentAlignment)
    {
        DDS_ULong initialAlignment = currentAlignment;

        currentAlignment += ClientUpdateGetSerializedSampleSize(sample, currentAlignment);
        return currentAlignment - initialAlignment;
    }

#ifdef _ZRDDS_INCLUDE_NO_SERIALIZE_MODE
    DDS_Char *ClientUpdateLoanSampleBuf(ClientUpdate *sample, DDS_Boolean takeBuffer)
    {
        DDS_Char *rst = (DDS_Char *)(sample->data._contiguousBuffer);
        if (takeBuffer)
        {
            sample->data._length = 0;
            sample->data._maximum = 0;
            sample->data._contiguousBuffer = NULL;
        }
        return rst;
    }

    void ClientUpdateReturnSampleBuf(DDS_Char *sampleBuf)
    {
        ZRDealloc(NULL, sampleBuf);
    }

    DDS_Long ClientUpdateLoanDeserialize(ClientUpdate *sampleBuf,
                                         CDRDeserializer *cdr,
                                         DDS_ULong curIndex,
                                         DDS_ULong totalNum,
                                         DDS_Char *base,
                                         DDS_ULong offset,
                                         DDS_ULong space,
                                         DDS_ULong fixedHeaderLen)
    {
#ifdef _ZRDDS_INCLUDE_DR_NO_SERIALIZE_MODE
        if (totalNum == 1 || curIndex == 0)
        {
            if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&(sampleBuf->num_samples), 8))
            {
                printf("deserialize num_samples failed.");
                return -2;
            }
        }
        DDS_Char **fragments = sampleSeq->_fixedFragments;
        DDS_Char **headers = sampleSeq->_fixedHeader;
        if (totalNum > 64)
        {
            if (sampleSeq->_variousFragments == NULL || sampleSeq->_fragmentNum < totalNum)
            {
                ZRDealloc(NULL, sampleSeq->_variousFragments);
                ZRDealloc(NULL, sampleSeq->_variousHeader);
                // ��Ƭ��������64����Ҫ��̬����
                sampleSeq->_variousFragments = (DDS_Char **)ZRMalloc(NULL, totalNum * sizeof(DDS_Char *));
                sampleSeq->_variousHeader = (DDS_Char **)ZRMalloc(NULL, totalNum * sizeof(DDS_Char *));
                if (NULL == sampleSeq->_variousFragments || NULL == sampleSeq->_variousHeader)
                {
                    printf("malloc for _variousFragments failed.\n");
                    return -1;
                }
                memset(sampleSeq->_variousFragments, 0, sizeof(totalNum * sizeof(DDS_Char *)));
                memset(sampleSeq->_variousHeader, 0, sizeof(totalNum * sizeof(DDS_Char *)));
            }
            fragments = sampleSeq->_variousFragments;
            headers = sampleSeq->_variousHeader;
        }
        sampleSeq->_fragmentNum = totalNum;
        if (totalNum == 1)
        {
            sampleSeq->_length = *(DDS_ULong *)(base + offset + fixedHeaderLen - 4);
            sampleSeq->_maximum = sampleSeq->_length;
            sampleSeq->_firstFragSize = space - fixedHeaderLen;
            fragments[curIndex] = base + offset + fixedHeaderLen;
            headers[curIndex] = base;
        }
        else if (curIndex == 0)
        {
            sampleSeq->_length = *(DDS_ULong *)(base + offset + fixedHeaderLen - 4);
            sampleSeq->_maximum = sampleSeq->_length;
            sampleSeq->_firstFragSize = space - fixedHeaderLen;
            fragments[curIndex] = base + offset + fixedHeaderLen;
            headers[curIndex] = base;
        }
        else if (curIndex == totalNum - 1)
        {
            sampleSeq->_lastFragSize = space;
            fragments[curIndex] = base + offset;
            headers[curIndex] = base;
        }
        else
        {
            sampleSeq->_fragmentSize = space;
            fragments[curIndex] = base + offset;
            headers[curIndex] = base;
        }
#endif /* _ZRDDS_INCLUDE_DR_NO_SERIALIZE_MODE */
        return 0;
    }

#endif /*_ZRDDS_INCLUDE_NO_SERIALIZE_MODE*/

#ifdef _ZRDDS_INCLUDE_ONSITE_DESERILIZE
    DDS_Long ClientUpdateOnSiteDeserialize(CDRDeserializer *cdr,
                                           ClientUpdate *sample,
                                           DDS_ULong offset,
                                           DDS_ULong totalSize,
                                           DDS_Char *payload,
                                           DDS_ULong payloadLen,
                                           DDS_ULong fixedHeaderLen)
    {
        ai_train::Bytes *seqMember = &(sample->data);
        if (!ai_train::Bytes_set_maximum(seqMember, totalSize - fixedHeaderLen))
        {
            printf("Set maxiumum member data failed.");
            return -3;
        }
        if (offset == 0)
        {
            if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->client_id, 4))
            {
                printf("deserialize sample->client_id failed.");
                return -2;
            }
            if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
            {
                printf("deserialize sample->round_id failed.");
                return -2;
            }
            if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->num_samples, 8))
            {
                printf("deserialize sample->num_samples failed.");
                return -2;
            }
            if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&seqMember->_length, 4))
            {
                printf("get data length failed.");
                return -1;
            }
            memcpy(seqMember->_contiguousBuffer,
                   (DDS_Char *)payload + fixedHeaderLen,
                   payloadLen - fixedHeaderLen);
            return 0;
        }
        memcpy(seqMember->_contiguousBuffer + offset - fixedHeaderLen,
               payload, payloadLen);
        return 0;
    }

    DDS_Boolean ClientUpdateNoSerializingSupported()
    {
        return true;
    }

    DDS_ULong ClientUpdateFixedHeaderLength()
    {
        DDS_ULong curLen = 0;
        curLen += CDRSerializerGetUntypeSize(4, curLen);
        curLen += CDRSerializerGetUntypeSize(4, curLen);
        curLen += CDRSerializerGetUntypeSize(8, curLen);
        curLen += CDRSerializerGetUntypeSize(4, curLen);
        return curLen;
    }

#endif /*_ZRDDS_INCLUDE_ONSITE_DESERILIZE*/
#define T ModelBlob
#define TSeq ModelBlobSeq
#define TINITIALIZE ModelBlobInitializeEx
#define TFINALIZE ModelBlobFinalizeEx
#define TCOPY ModelBlobCopyEx

#include "ZRSequence.cpp"
#include "ZRCPlusPlusSequence.cpp"

#undef TCOPY
#undef TFINALIZE
#undef TINITIALIZE
#undef TSeq
#undef T

    DDS_Boolean ModelBlobInitialize(ModelBlob *self)
    {
        return ModelBlobInitializeEx(self, NULL, true);
    }

    void ModelBlobFinalize(ModelBlob *self)
    {
        ModelBlobFinalizeEx(self, NULL, true);
    }

    DDS_Boolean ModelBlobCopy(
        ModelBlob *dst,
        const ModelBlob *src)
    {
        return ModelBlobCopyEx(dst, src, NULL);
    }

    ModelBlob *ModelBlobCreateSample(
        ZRMemPool *pool,
        DDS_Boolean allocMutable)
    {
        ModelBlob *newSample = (ModelBlob *)ZRMalloc(pool, sizeof(ModelBlob));
        if (newSample == NULL)
        {
            printf("malloc for ModelBlob failed.");
            return NULL;
        }
        if (!ModelBlobInitializeEx(newSample, pool, allocMutable))
        {
            printf("initial Sample failed.");
            ModelBlobDestroySample(pool, newSample);
            return NULL;
        }
        return newSample;
    }

    void ModelBlobDestroySample(ZRMemPool *pool, ModelBlob *sample)
    {
        if (sample == NULL)
            return;
        ModelBlobFinalizeEx(sample, pool, true);
        ZRDealloc(pool, sample);
    }

    DDS_ULong ModelBlobGetSerializedSampleMaxSize()
    {
        return MAX_UINT32_VALUE;
    }

    DDS_ULong ModelBlobGetSerializedKeyMaxSize()
    {
        return MAX_UINT32_VALUE;
    }

    DDS_Long ModelBlobGetKeyHash(
        const ModelBlob *sample,
        CDRSerializer *cdr,
        DDS::KeyHash_t *result)
    {
        DDS_Long ret = ModelBlobSerializeKey(sample, cdr);
        if (ret < 0)
        {
            printf("serialize key failed.");
            *result = DDS_HANDLE_NIL_NATIVE;
            return -1;
        }
        ret = CDRSerializeGetKeyHash(cdr, result->value, true);
        if (ret < 0)
        {
            printf("get keyhash failed.");
            *result = DDS_HANDLE_NIL_NATIVE;
            return -1;
        }
        result->valid = true;
        return 0;
    }

    DDS_Boolean ModelBlobHasKey()
    {
        return false;
    }

    TypeCodeHeader *ModelBlobGetInnerTypeCode()
    {
#ifdef _ZRDDS_INCLUDE_TYPECODE
        DDS::TypeCode *userTypeCode = ModelBlobGetTypeCode();
        if (userTypeCode == NULL)
            return NULL;
        return userTypeCode->getImpl();
#else
        return NULL;
#endif
    }

    DDS_Boolean ModelBlobInitializeEx(
        ModelBlob *self,
        ZRMemPool *pool,
        DDS_Boolean allocateMemory)
    {
        self->round_id = 0;

        ai_train::Bytes_initialize_ex(&self->data, pool, allocateMemory);

        if (allocateMemory)
        {
        }
        else
        {
        }
        return true;
    }

    void ModelBlobFinalizeEx(
        ModelBlob *self,
        ZRMemPool *pool,
        DDS_Boolean deletePointers)
    {
        ai_train::Bytes_finalize(&self->data);
        if (deletePointers)
        {
        }
    }

    DDS_Boolean ModelBlobCopyEx(
        ModelBlob *dst,
        const ModelBlob *src,
        ZRMemPool *pool)
    {
        dst->round_id = src->round_id;
        if (!ai_train::Bytes_copy(&dst->data, &src->data))
        {
            printf("copy member data failed.");
            return false;
        }
        return true;
    }

    void ModelBlobPrintData(const ModelBlob *sample)
    {
        if (sample == NULL)
        {
            printf("NULL\n");
            return;
        }
        printf("sample->round_id: %u\n", sample->round_id);
        printf("\n");

        DDS_ULong dataTmpLen = ai_train::Bytes_get_length(&sample->data);
        printf("sample->data: %d\n", dataTmpLen);
        for (DDS_ULong i = 0; i < dataTmpLen; ++i)
        {
            printf("sample->data[%u]: 0x%02x\n", i, *ai_train::Bytes_get_reference(&sample->data, i));
        }
        printf("\n");
    }

    DDS::TypeCode *ModelBlobGetTypeCode()
    {
        static DDS::TypeCode *s_typeCode = NULL;
        if (s_typeCode != NULL)
            return s_typeCode;
        DDS::TypeCodeFactory &factory = DDS::TypeCodeFactory::getInstance();

        s_typeCode = factory.createStructTC(
            "ai_train::ModelBlob",
            DDS_EXTENSIBLE_EXTENSIBILITY);
        if (s_typeCode == NULL)
        {
            printf("create struct ModelBlob typecode failed.");
            return s_typeCode;
        }
        DDS_Long ret = 0;
        DDS::TypeCode *memberTc = NULL;
        DDS::TypeCode *eleTc = NULL;

        memberTc = factory.getPrimitiveTC(DDS_TK_UINT);
        if (memberTc == NULL)
        {
            printf("Get Member round_id TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            0,
            0,
            "round_id",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        memberTc = factory.getPrimitiveTC(DDS_TK_UCHAR);
        if (memberTc != NULL)
        {
            memberTc = factory.createSequenceTC(MAX_UINT32_VALUE, memberTc);
        }
        if (memberTc == NULL)
        {
            printf("Get Member data TypeCode failed.");
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }
        ret = s_typeCode->addMemberToStruct(
            1,
            1,
            "data",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.deleteTC(s_typeCode);
            s_typeCode = NULL;
            return NULL;
        }

        return s_typeCode;
    }

    DDS_Long ModelBlobSerialize(const ModelBlob *sample, CDRSerializer *cdr)
    {
        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
        {
            printf("serialize sample->round_id failed.");
            return -2;
        }

        if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&(sample->data)._length, 4))
        {
            printf("serialize length of sample->data failed.");
            return -2;
        }
        if ((sample->data)._contiguousBuffer)
        {
            if (!CDRSerializerPutUntypeArray(cdr, (DDS_Octet *)(sample->data)._contiguousBuffer, (sample->data)._length, 1))
            {
                printf("serialize sample->data failed.");
                return -2;
            }
        }
        else
        {
            for (DDS_ULong i = 0; i < (sample->data)._length; ++i)
            {
                if (!CDRSerializerPutUntype(cdr, (DDS_Octet *)&*ai_train::Bytes_get_reference(&sample->data, i), 1))
                {
                    printf("serialize sample->data failed.");
                    return -2;
                }
            }
        }

        return 0;
    }

    DDS_Long ModelBlobDeserialize(
        ModelBlob *sample,
        CDRDeserializer *cdr,
        ZRMemPool *pool)
    {
        // no key
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
        {
            sample->round_id = 0;
            ai_train::Bytes_initialize_ex(&sample->data, pool, true);
            return 0;
        }
        DDS_ULong dataTmpLen = 0;
        if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&dataTmpLen, 4))
        {
            ai_train::Bytes_initialize_ex(&sample->data, pool, true);
            return 0;
        }
        if (!ai_train::Bytes_ensure_length(&sample->data, dataTmpLen, dataTmpLen))
        {
            printf("Set maxiumum member sample->data failed.");
            return -3;
        }
        if (sample->data._contiguousBuffer)
        {
            if (!CDRDeserializerGetUntypeArray(cdr, (DDS_Octet *)sample->data._contiguousBuffer, dataTmpLen, 1))
            {
                printf("deserialize sample->data failed.");
                return -2;
            }
        }
        else
        {
            for (DDS_ULong i = 0; i < dataTmpLen; ++i)
            {
                if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&*ai_train::Bytes_get_reference(&sample->data, i), 1))
                {
                    printf("deserialize sample->data failed.");
                    return -2;
                }
            }
        }
        return 0;
    }

    DDS_ULong ModelBlobGetSerializedSampleSize(const ModelBlob *sample, DDS_ULong currentAlignment)
    {
        DDS_ULong initialAlignment = currentAlignment;

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);

        currentAlignment += CDRSerializerGetUntypeSize(4, currentAlignment);
        DDS_ULong dataLen = ai_train::Bytes_get_length(&sample->data);
        if (dataLen != 0)
        {
            currentAlignment += 1 * dataLen;
        }

        return currentAlignment - initialAlignment;
    }

    DDS_Long ModelBlobSerializeKey(const ModelBlob *sample, CDRSerializer *cdr)
    {
        if (ModelBlobSerialize(sample, cdr) < 0)
        {
            return -1;
        }
        return 0;
    }

    DDS_Long ModelBlobDeserializeKey(
        ModelBlob *sample,
        CDRDeserializer *cdr,
        ZRMemPool *pool)
    {
        if (ModelBlobDeserialize(sample, cdr, pool) < 0)
        {
            return -1;
        }
        return 0;
    }

    DDS_ULong ModelBlobGetSerializedKeySize(const ModelBlob *sample, DDS_ULong currentAlignment)
    {
        DDS_ULong initialAlignment = currentAlignment;

        currentAlignment += ModelBlobGetSerializedSampleSize(sample, currentAlignment);
        return currentAlignment - initialAlignment;
    }

#ifdef _ZRDDS_INCLUDE_NO_SERIALIZE_MODE
    DDS_Char *ModelBlobLoanSampleBuf(ModelBlob *sample, DDS_Boolean takeBuffer)
    {
        DDS_Char *rst = (DDS_Char *)(sample->data._contiguousBuffer);
        if (takeBuffer)
        {
            sample->data._length = 0;
            sample->data._maximum = 0;
            sample->data._contiguousBuffer = NULL;
        }
        return rst;
    }

    void ModelBlobReturnSampleBuf(DDS_Char *sampleBuf)
    {
        ZRDealloc(NULL, sampleBuf);
    }

    DDS_Long ModelBlobLoanDeserialize(ModelBlob *sampleBuf,
                                      CDRDeserializer *cdr,
                                      DDS_ULong curIndex,
                                      DDS_ULong totalNum,
                                      DDS_Char *base,
                                      DDS_ULong offset,
                                      DDS_ULong space,
                                      DDS_ULong fixedHeaderLen)
    {
#ifdef _ZRDDS_INCLUDE_DR_NO_SERIALIZE_MODE
        DDS_Char **fragments = sampleSeq->_fixedFragments;
        DDS_Char **headers = sampleSeq->_fixedHeader;
        if (totalNum > 64)
        {
            if (sampleSeq->_variousFragments == NULL || sampleSeq->_fragmentNum < totalNum)
            {
                ZRDealloc(NULL, sampleSeq->_variousFragments);
                ZRDealloc(NULL, sampleSeq->_variousHeader);
                // ��Ƭ��������64����Ҫ��̬����
                sampleSeq->_variousFragments = (DDS_Char **)ZRMalloc(NULL, totalNum * sizeof(DDS_Char *));
                sampleSeq->_variousHeader = (DDS_Char **)ZRMalloc(NULL, totalNum * sizeof(DDS_Char *));
                if (NULL == sampleSeq->_variousFragments || NULL == sampleSeq->_variousHeader)
                {
                    printf("malloc for _variousFragments failed.\n");
                    return -1;
                }
                memset(sampleSeq->_variousFragments, 0, sizeof(totalNum * sizeof(DDS_Char *)));
                memset(sampleSeq->_variousHeader, 0, sizeof(totalNum * sizeof(DDS_Char *)));
            }
            fragments = sampleSeq->_variousFragments;
            headers = sampleSeq->_variousHeader;
        }
        sampleSeq->_fragmentNum = totalNum;
        if (totalNum == 1)
        {
            sampleSeq->_length = *(DDS_ULong *)(base + offset + fixedHeaderLen - 4);
            sampleSeq->_maximum = sampleSeq->_length;
            sampleSeq->_firstFragSize = space - fixedHeaderLen;
            fragments[curIndex] = base + offset + fixedHeaderLen;
            headers[curIndex] = base;
        }
        else if (curIndex == 0)
        {
            sampleSeq->_length = *(DDS_ULong *)(base + offset + fixedHeaderLen - 4);
            sampleSeq->_maximum = sampleSeq->_length;
            sampleSeq->_firstFragSize = space - fixedHeaderLen;
            fragments[curIndex] = base + offset + fixedHeaderLen;
            headers[curIndex] = base;
        }
        else if (curIndex == totalNum - 1)
        {
            sampleSeq->_lastFragSize = space;
            fragments[curIndex] = base + offset;
            headers[curIndex] = base;
        }
        else
        {
            sampleSeq->_fragmentSize = space;
            fragments[curIndex] = base + offset;
            headers[curIndex] = base;
        }
#endif /* _ZRDDS_INCLUDE_DR_NO_SERIALIZE_MODE */
        return 0;
    }

#endif /*_ZRDDS_INCLUDE_NO_SERIALIZE_MODE*/

#ifdef _ZRDDS_INCLUDE_ONSITE_DESERILIZE
    DDS_Long ModelBlobOnSiteDeserialize(CDRDeserializer *cdr,
                                        ModelBlob *sample,
                                        DDS_ULong offset,
                                        DDS_ULong totalSize,
                                        DDS_Char *payload,
                                        DDS_ULong payloadLen,
                                        DDS_ULong fixedHeaderLen)
    {
        ai_train::Bytes *seqMember = &(sample->data);
        if (!ai_train::Bytes_set_maximum(seqMember, totalSize - fixedHeaderLen))
        {
            printf("Set maxiumum member data failed.");
            return -3;
        }
        if (offset == 0)
        {
            if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&sample->round_id, 4))
            {
                printf("deserialize sample->round_id failed.");
                return -2;
            }
            if (!CDRDeserializerGetUntype(cdr, (DDS_Octet *)&seqMember->_length, 4))
            {
                printf("get data length failed.");
                return -1;
            }
            memcpy(seqMember->_contiguousBuffer,
                   (DDS_Char *)payload + fixedHeaderLen,
                   payloadLen - fixedHeaderLen);
            return 0;
        }
        memcpy(seqMember->_contiguousBuffer + offset - fixedHeaderLen,
               payload, payloadLen);
        return 0;
    }

    DDS_Boolean ModelBlobNoSerializingSupported()
    {
        return true;
    }

    DDS_ULong ModelBlobFixedHeaderLength()
    {
        DDS_ULong curLen = 0;
        curLen += CDRSerializerGetUntypeSize(4, curLen);
        curLen += CDRSerializerGetUntypeSize(4, curLen);
        return curLen;
    }

#endif /*_ZRDDS_INCLUDE_ONSITE_DESERILIZE*/
}
