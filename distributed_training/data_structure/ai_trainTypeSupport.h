#pragma once
#ifndef ai_trainTypeSupport_h__
#define ai_trainTypeSupport_h__
/*************************************************************/
/*           ���ļ��ɱ��������ɣ����������޸�                */
/*************************************************************/

#include "ZRDDSTypeSupport.h"

namespace ai_train {
    DDSTypeSupport(TrainCmdTypeSupport, TrainCmd);
    DDSTypeSupport(ClientUpdateTypeSupport, ClientUpdate);
    DDSTypeSupport(ModelBlobTypeSupport, ModelBlob);
}
#endif

