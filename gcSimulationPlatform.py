# -*- coding: utf-8 -*-

'''
Create data: 2018/10/13
Author: liangqiseu 

'''

import random
import math

class Ppn(object):
    def __init__(self, sn=0, offset=0):
        self.rpbnSn = sn
        self.offset = offset


class Rpbn(Ppn):
    def __init__(self, sn=0, crt=0, sid=0, vpc=0, invalidLpa=0, vpcPerRpbn=10000):
        self.sn = sn
        self.crt = crt   
        self.sid = sid
        self.vpc = vpc
        self.lpaList = [invalidLpa] * vpcPerRpbn
        self.wrPos = 0


class SsdCfg(object):
    def __init__(self, op=0.2, vpcPerRpbn=10000, rpbnNum=1000, sidNum=3, msGcEn=True):
        self.op = op
        self.vpcPerRpbn = vpcPerRpbn
        self.rpbnNum = rpbnNum
        self.sidNum = sidNum
        self.msGcEn = msGcEn


class IoMoudle(object):
    # default module is JDEDC219
    def __init__(self, c=[50,30,20], v=[5,15,80], type="rand", runLoops=100000):
        self.sidCredit = c
        self.sidRange = v
        self.wrType = type
        self.runLoops = runLoops

        self.showIoModule()

    def showIoModule(self):
        print("sidCredit=",self.sidCredit)
        print("sidRange=", self.sidRange)
        print("wrType=", self.wrType)
        print("runLoops=", self.runLoops)


class SsdModule(SsdCfg):
    # rpbn management
    rpbnList = []
    freeRpbnList = []
    closeRpbnList = []
    openRpbnList = []
    freeRpbnCnt = 0
    invalidRpbnSn = 0

    # ftl
    ftlMap = []
    maxLpa = 0
    invalidLpa = 0
    
    # gc management
    gcStartLevel = 0

    # multi-stream
    sidTrafficStats = []
    sidVpcStats = []

    # wa staticts
    ioCnt = 0
    nandCnt = 0

    # others
    createTimes = 0


    def __init__(self,mCfg=None):
        if (mCfg):
            self.op = mCfg.op
            self.rpbnNum = mCfg.rpbnNum
            self.vpcPerRpbn = mCfg.vpcPerRpbn
            self.msGcEnable = mCfg.msGcEnable
            print("custom cfg:")
        else:
            super().__init__()
            print("default cfg:")

        self.moduleInit()
        self.printAll()
    

    def rpbnMgtInit(self):
        self.invalidRpbnSn = self.rpbnNum + 10
  
        for i in range(self.rpbnNum):
            rpbn = Rpbn(i,0,0,0,self.invalidLpa,self.vpcPerRpbn)
            self.rpbnList.append(rpbn)
            self.freeRpbnList.append(rpbn.sn)
            self.freeRpbnCnt += 1

        for i in range(self.sidNum):
            self.openRpbnList.append(self.invalidRpbnSn)
        

    def ftlInit(self):
        self.maxLpa = int(self.rpbnNum * self.vpcPerRpbn * (1 - self.op))
        self.invalidLpa = self.maxLpa + 100

        for i in range(self.maxLpa):
            self.ftlMap.append(Ppn(10,10))


    def gcInit(self):
        self.gcStartLevel = self.sidNum


    def msInit(self):
        self.sidTrafficStats = [0] * self.sidNum
        self.sidVpcStats = [0] * self.sidNum
        self.gcSid = self.sidNum + 1
        self.invalidSid = self.sidNum + 2


    def moduleInit(self):
        self.ftlInit()
        self.rpbnMgtInit()
        self.gcInit()
        self.msInit()


    def mCfgPrint(self):
        print("op=%f" % (self.op))
        print("rpbnNum=%d" % (self.rpbnNum))
        print("vpcPerRpbn=%d" % (self.vpcPerRpbn))
        print("invalidRpbnSn=%d" % (self.invalidRpbnSn))
        print("maxLpa=%d" % (self.maxLpa))
        print("invalidLpa=%d" % (self.invalidLpa))


    def curStatPrint(self):    
        print("freeRpbnCnt=", self.freeRpbnCnt)
        print("freeRpbnSnList", self.freeRpbnList)
        print("openRpbnList=", self.openRpbnList)
        print("closeRpbnList=", self.closeRpbnList)


    def printAll(self):
        self.mCfgPrint()
        self.curStatPrint()


class WaTest(SsdModule):

    lpaRange = []

    def __init__(self,mCfg=None):
        super(WaTest,self).__init__(mCfg)    


    def lpaInit(self, sidRangeList):
        sidLpaRange = 0
        lpaSum = -1
        sumRange = sum(sidRangeList)

        for i in sidRangeList:
            sidLpaRange = int(i * self.maxLpa / sumRange + lpaSum)
            self.lpaRange.append(sidLpaRange)
            lpaSum = sidLpaRange
        print("lpaRange=", self.lpaRange)


    def assertWithStatPrint(self):
        self.curStatPrint()
        assert 1==0


    def runIo(self,ioModule):
        self.lpaInit(ioModule.sidRange)
        self.writeData(ioModule)


    def writeData(self, ioModule):
        curLoop = 0
        while (curLoop < ioModule.runLoops):
            sidIdx = 0
            for sidCredit in ioModule.sidCredit:
                cnt = 0
                while (cnt < sidCredit):
                    lpa = self.getLpaBySid(sidIdx)
                    self.writeOneLpa(sidIdx, lpa, True)
                    cnt += 1
                sidIdx += 1


    def getLpaBySid(self, sid):
        lpaRange = [0] + self.lpaRange
        startLpa = lpaRange[sid]
        endLpa = lpaRange[sid+1]
        return random.randint(startLpa,endLpa)


    def writeOneLpa(self, sid, lpa, isHostIo):
        if (self.invalidRpbnSn == self.openRpbnList[sid]):
            self.openOneRpbn(sid)
        rpbnSn = self.openRpbnList[sid]
        rpbn = self.rpbnList[rpbnSn]

        #print("write: rpbn=%d offset=%d lpa=%d" % (rpbn.sn, rpbn.wrPos, lpa))
        if (self.invalidLpa != rpbn.lpaList[rpbn.wrPos]):
            self.assertWithStatPrint()

        self.updateStats(sid,isHostIo)
        self.updateFtlMap(lpa,rpbn)
        rpbn.lpaList[rpbn.wrPos] = lpa
        rpbn.wrPos += 1
        rpbn.vpc += 1 

        if (self.vpcPerRpbn == rpbn.wrPos):
            rpbn.wrPos = 0
            self.closeOneRpbn(rpbn)


    def updateStats(self, sid, isHostIo):
        if (True == isHostIo):
            self.sidTrafficStats[sid] += 1
            self.ioCnt += 1
        self.sidVpcStats[sid] += 1
        self.nandCnt += 1

        if (0 == self.ioCnt % self.maxLpa and 0 != self.ioCnt):
            print("wa=%f" %(self.nandCnt/self.ioCnt))
            self.ioCnt = 0
            self.nandCnt = 0


    def updateFtlMap(self,lpa,newRpbn):
        ppn = self.ftlMap[lpa]
        if (self.invalidRpbnSn != ppn.rpbnSn):
            oldRpbn = self.rpbnList[ppn.rpbnSn]
            if (self.invalidLpa != oldRpbn.lpaList[ppn.offset]):
                oldRpbn.lpaList[ppn.offset] = self.invalidLpa
                self.sidVpcStats[oldRpbn.sid] -= 1
                oldRpbn.vpc -= 1
            

        # update ppn   
        ppn.rpbnSn = newRpbn.sn
        ppn.offset = newRpbn.wrPos


    def getOneFreeRpbn(self):
        if (0 == self.freeRpbnCnt):
            self.assertWithStatPrint()

        rpbnSn = self.freeRpbnList.pop(0)
        self.freeRpbnCnt -= 1 
        rpbn = self.rpbnList[rpbnSn]
        return rpbn


    def addToFreeRpbnList(self, rpbnSn):
        self.freeRpbnList.append(rpbnSn)
        self.freeRpbnCnt += 1


    def openOneRpbn(self,sid):
        rpbn = self.getOneFreeRpbn()
        if (0 != rpbn.vpc):
            self.assertWithStatPrint()

        rpbn.sid = sid
        rpbn.crt = self.createTimes
        self.createTimes += 1
        self.openRpbnList[sid] = rpbn.sn

        #print("open %d sid=%d freeCnt=%d gcDoing=%d" % (rpbn.sn, sid, self.freeRpbnCnt, self.gcDoingFlag))
        return rpbn


    def closeOneRpbn(self,rpbn):
        self.closeRpbnList.append(rpbn.sn)
        self.openRpbnList[rpbn.sid] = self.invalidRpbnSn
        
        #print("close %d freeCnt=%d vpc=%d" % (rpbn.sn, self.freeRpbnCnt, rpbn.vpc))
        if (self.gcStartLevel >= self.freeRpbnCnt):
            self.runGc()
        

    def runGc(self):
        targetRpbnSn = self.gcSearchOptimalRpbn()
        self.closeRpbnList.remove(targetRpbnSn)
        self.moveOneRpbn(targetRpbnSn)
        self.addToFreeRpbnList(targetRpbnSn)


    def gcSearchOptimalRpbn(self):
        if (True == self.msGcEn):
            sid = self.findSidByKappa()
            if (self.invalidSid != sid):
                targetRpbnSn = self.getSidMinVpcRpbn(sid)
                return targetRpbnSn
        targetRpbnSn = self.getMinVpcRpbn()
        return targetRpbnSn
        

    def moveOneRpbn(self, rpbnSn):
        rpbn = self.rpbnList[rpbnSn]
        sid = rpbn.sid

        print("gc move! rpbn=%d sid=%d vpc=%d" % (rpbnSn, sid, rpbn.vpc))
        for lpa in rpbn.lpaList:
            idx = 0
            if (self.invalidLpa != lpa):
                self.writeOneLpa(sid,lpa,False)
                rpbn.lpaList[idx] = self.invalidLpa
            idx += 1
        

    def findSidByKappa(self):
        sidIdealSpace = self.calcIdealSidSpace()
        sidActualSpace = self.calcActualSidSpace()
        maxRatio = 1
        targeSid = self.invalidSid
        for sid in range(self.sidNum):
            if (0 != sidIdealSpace[sid]):
                ratio = sidActualSpace[sid] / sidIdealSpace[sid]
                if (maxRatio < ratio):
                    maxRatio = ratio
                    targeSid = sid
            print("sid=%d ratio=%f" % (sid, ratio))       
        return targeSid



    def calcIdealSidOpRatio(self):
        idealsidOpRatio = []
    
        TRatio = [0] * self.sidNum
        VRatio = [0] * self.sidNum
        sidTVRatio = [0] * self.sidNum
        sumT = sum(self.sidTrafficStats)
        sumV = sum(self.sidVpcStats)

        for sid in range(self.sidNum):
            TRatio[sid] = round(self.sidTrafficStats[sid] / sumT, 5)
            VRatio[sid] = round(self.sidVpcStats[sid] / sumV, 5)
            TVRatio = round(TRatio[sid] * VRatio[sid], 5)
            sidTVRatio[sid] = TVRatio

        sumTV = sum(sidTVRatio)
        for tv in sidTVRatio:
            idealsidOpRatio.append(round(tv/sumTV,5))
        print("idealOp=", TRatio, VRatio, idealsidOpRatio)
        return idealsidOpRatio

    
    def calcIdealSidSpace(self):
        idealsidOpRatio = self.calcIdealSidOpRatio()
        sidIdealSpace = []
        sumV = sum(self.sidVpcStats)

        for sid in range(self.sidNum):
            sidIdealOpSapce = self.vpcPerRpbn * self.rpbnNum * self.op * idealsidOpRatio[sid]
            sidOrignalSpace = self.vpcPerRpbn * self.rpbnNum * (1 - self.op) * (self.sidVpcStats[sid] / sumV)
            sidIdealSpace.append(sidIdealOpSapce + sidOrignalSpace)

        return sidIdealSpace


    def calcActualSidSpace(self):
        actualSidSpace = [0] * self.sidNum
        
        for rpbnSn in self.closeRpbnList:
            rpbn = self.rpbnList[rpbnSn]
            actualSidSpace[rpbn.sid] += self.vpcPerRpbn

        for rpbnSn in self.openRpbnList:
            if (self.invalidRpbnSn != rpbnSn):
                rpbn = self.rpbnList[rpbnSn]
                actualSidSpace[rpbn.sid] += rpbn.wrPos

        return actualSidSpace


    def getMinVpcRpbn(self):
        minVpc = self.vpcPerRpbn
        targetRpbnSn = self.invalidRpbnSn
        for sid in range(self.sidNum):
            rpbnSn = self.getSidMinVpcRpbn(sid)
            rpbn = self.rpbnList[rpbnSn]
            if (minVpc >= rpbn.vpc):
                minVpc = rpbn.vpc
                targetRpbnSn = rpbnSn
        return targetRpbnSn


    def getSidMinVpcRpbn(self, sid):
        minVpc = self.vpcPerRpbn
        targetSn = self.invalidRpbnSn
        for sn in self.closeRpbnList:
            rpbn = self.rpbnList[sn]
            if (sid == rpbn.sid):
                if (minVpc > rpbn.vpc):
                    minVpc = rpbn.vpc
                    targetSn = sn
        return targetSn



def main():
    a = WaTest()
    b = IoMoudle()
    a.runIo(b)

if __name__ == "__main__":
    main()