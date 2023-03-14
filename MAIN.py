import os
import sys
import timeit
import MERGE
import MAIN_4G
import MAIN_3G
import MAIN_5G
import AuxPlugInUnit
import enodebfunction
import EthernetPort
import eutrancellfdd
import FeatureState
import FieldReplaceableUnit
import GNBCUCPFunction
import GNBDUFunction
import LocationArea
import McpcPCellProfile
import NRCellDU
import NRSectorCarrier
import PlugInUnit
import QciProfileEndcConfigExt
import ReportConfigB1GUtra
import RfBranch
import SectorCarrier
import SectorEquipmentFunction
import Slot
import TermPointToAmf
import UeMeasControl
import UePolicyOptimization
import UtranCell

inicio = timeit.default_timer()



#update data
MAIN_4G.processArchive('UPDATE')#USE 'TESTE' for test
MAIN_3G.processArchive('UPDATE')#USE 'TESTE' for test
MAIN_5G.processArchive('UPDATE')#USE 'TESTE' for test



#process data
enodebfunction.enodebfunction('4G')
EthernetPort.EthernetPort('5G')
eutrancellfdd.eutrancellfdd('4G')
FeatureState.FeatureState('5G')
TEC = ['4G','5G']
for i in TEC:
  FieldReplaceableUnit.FieldReplaceableUnit(i)
  SectorEquipmentFunction.SectorEquipmentFunction(i)
GNBCUCPFunction.GNBCUCPFunction('5G')
GNBDUFunction.GNBDUFunction('5G')
LocationArea.LocationArea('3G')
McpcPCellProfile.McpcPCellProfile('5G')
NRCellDU.NRCellDU('5G')
NRSectorCarrier.NRSectorCarrier('5G')
TEC = ['3G','4G']
for i in TEC:
  AuxPlugInUnit.AuxPlugInUnit(i)
  PlugInUnit.PlugInUnit(i)
QciProfileEndcConfigExt.QciProfileEndcConfigExt('5G')
ReportConfigB1GUtra.ReportConfigB1GUtra('4G')
RfBranch.RfBranch('4G')
SectorCarrier.SectorCarrier('4G')
Slot.Slot('4G')
TermPointToAmf.TermPointToAmf('5G')
UeMeasControl.UeMeasControl('4G')
UePolicyOptimization.UePolicyOptimization('4G')
UtranCell.UtranCell('3G')




















fim = timeit.default_timer()
print ('duracao [FINAL]: %.2f' % ((fim - inicio)/60) + ' min')