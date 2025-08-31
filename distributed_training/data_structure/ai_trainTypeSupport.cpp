/*************************************************************/
/*           此文件由编译器生成，请勿随意修改                */
/*************************************************************/
#include <stdlib.h>
#include "ZRDDSTypePlugin.h"
#include "ai_train.h"
#include "ai_trainTypeSupport.h"
#include "ai_trainDataReader.h"
#include "ai_trainDataWriter.h"
#include "ZRDDSTypeSupport.cpp"

namespace ai_train
{

const DDS_Char* TrainCmd_TYPENAME = "TrainCmd";
DDSTypeSupportImpl(TrainCmdTypeSupport, TrainCmd, TrainCmd_TYPENAME);


const DDS_Char* ClientUpdate_TYPENAME = "ClientUpdate";
DDSTypeSupportImpl(ClientUpdateTypeSupport, ClientUpdate, ClientUpdate_TYPENAME);


const DDS_Char* ModelBlob_TYPENAME = "ModelBlob";
DDSTypeSupportImpl(ModelBlobTypeSupport, ModelBlob, ModelBlob_TYPENAME);

}
