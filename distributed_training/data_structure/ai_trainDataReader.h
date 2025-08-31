#pragma once
#ifndef ai_trainDataReader_h__
#define ai_trainDataReader_h__
/*************************************************************/
/*           ���ļ��ɱ��������ɣ����������޸�                */
/*************************************************************/

#include "ai_train.h"
#include "ZRDDSDataReader.h"

namespace ai_train {
    typedef struct TrainCmdSeq TrainCmdSeq;

    typedef DDS::ZRDDSDataReader<TrainCmd, TrainCmdSeq> TrainCmdDataReader;

    typedef struct ClientUpdateSeq ClientUpdateSeq;

    typedef DDS::ZRDDSDataReader<ClientUpdate, ClientUpdateSeq> ClientUpdateDataReader;

    typedef struct ModelBlobSeq ModelBlobSeq;

    typedef DDS::ZRDDSDataReader<ModelBlob, ModelBlobSeq> ModelBlobDataReader;

}
#endif

