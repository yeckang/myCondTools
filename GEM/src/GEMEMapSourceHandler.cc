#include "myCondTools/GEM/interface/GEMEMapSourceHandler.h"
#include "CondCore/CondDB/interface/ConnectionPool.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include "RelationalAccess/ISessionProxy.h"
#include "RelationalAccess/ITransaction.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ICursor.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/Attribute.h"
#include "CoralBase/AttributeSpecification.h"
#include <TString.h>

#include <fstream>
#include <cstdlib>
#include <vector>

#include <DataFormats/MuonDetId/interface/GEMDetId.h>

popcon::GEMEMapSourceHandler::GEMEMapSourceHandler( const edm::ParameterSet& ps ):
  m_name( ps.getUntrackedParameter<std::string>( "name", "GEMEMapSourceHandler" ) ),
  m_dummy( ps.getUntrackedParameter<int>( "WriteDummy", 0 ) ),
  m_validate( ps.getUntrackedParameter<int>( "Validate", 1 ) ),
  m_connect( ps.getParameter<std::string>( "connect" ) ),
  m_connectionPset( ps.getParameter<edm::ParameterSet>( "DBParameters" ) ),
  m_conf_type( ps.getParameter<std::string>("QC8ConfType"))
{
}

popcon::GEMEMapSourceHandler::~GEMEMapSourceHandler()
{
}

void popcon::GEMEMapSourceHandler::getNewObjects()
{
  
  edm::LogInfo( "GEMEMapSourceHandler" ) << "[" << "GEMEMapSourceHandler::" << __func__ << "]:" << m_name << ": "
                                         << "BEGIN" << std::endl;
  
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  
  // first check what is already there in offline DB
  Ref payload;
  if(m_validate==1 && tagInfo().size>0) {
    payload = lastPayload();
  }
  
  // now construct new cabling map from online DB
  // FIXME: use boost::ptime
  time_t rawtime;
  time(&rawtime); //time since January 1, 1970
  tm * ptm = gmtime(&rawtime);//GMT time
  char buffer[20];
  strftime(buffer,20,"%d/%m/%Y_%H:%M:%S",ptm);
  std::string eMap_version( buffer );
  eMap =  new GEMeMap(eMap_version);
  
  std::string baseCMS = std::string(getenv("CMSSW_BASE"))+std::string("/src/myCondTools/GEM/data/");  
  std::vector<std::string> mapfiles;

  mapfiles.push_back("chamberMapCoffin.csv");
  mapfiles.push_back("vfatTypeListCoffin.csv");
  mapfiles.push_back("HV3bV3ChMapCoffin.csv");
  // VFAT Postion Map 
  GEMeMap::GEMChamberMap cMap;
  std::string field, line;
  std::string filename(baseCMS+mapfiles[0]);
  std::ifstream maptype(filename.c_str());
  //std::string buf("");
  std::cout << filename << std::endl;
  while(std::getline(maptype, line)){
    unsigned int fedId_, amcNum_, gebId_;
    //uint8_t amcNum_, gebId_;
    int  region_, station_, layer_, chamberSec_, vfatVer_; 
    std::stringstream ssline(line);
    getline( ssline, field, ',' );
    std::stringstream FEDID(field);
    getline( ssline, field, ',' );
    std::stringstream AMCNUM(field);
    getline( ssline, field, ',' );
    std::stringstream GEBID(field);
    getline( ssline, field, ',' );
    std::stringstream REGION(field);
    getline( ssline, field, ',' );
    std::stringstream STATION(field);
    getline( ssline, field, ',' );
    std::stringstream LAYER(field);
    getline( ssline, field, ',' );
    std::stringstream CHAMBERSEC(field);
    getline( ssline, field, ',' );
    std::stringstream VFATVER(field);

    FEDID >> fedId_; AMCNUM >> amcNum_; GEBID >> gebId_;
    REGION >> region_; STATION >> station_; LAYER >> layer_; CHAMBERSEC >> chamberSec_; VFATVER >> vfatVer_; 

    std::cout << "fedId: " << fedId_ << ", AMC#: " << amcNum_ << ", gebId: " << gebId_ <<
    ", region: " << region_ << ", station: " << station_ << ", layer: " << layer_ << ", chamberSec: " << chamberSec_ << 
    ", vfatVer" << vfatVer_ << std::endl;

    cMap.fedId.push_back(fedId_);
    cMap.amcNum.push_back(amcNum_);
    cMap.gebId.push_back(gebId_);
    cMap.gemNum.push_back(region_*(station_*1000 + layer_*100 + chamberSec_));
    cMap.vfatVer.push_back(vfatVer_);
  }
  eMap->theChamberMap_.push_back(cMap);

  GEMeMap::GEMVFatMap vMap;
  std::string filename1(baseCMS+mapfiles[1]);
  std::ifstream maptype1(filename1.c_str());
  std::cout << filename1 << std::endl;
  while(std::getline(maptype1, line)){
    uint16_t vfatAdd_;
    int region_, station_, layer_, chamberSec_, vfatType_,  iEta_, localPhi_;
    
    std::stringstream ssline(line);
    getline( ssline, field, ',' );
    std::stringstream VFATTYPE(field);
    getline( ssline, field, ',' );
    std::stringstream VFATVER(field);
    getline( ssline, field, ',' );
    std::stringstream REGION(field);
    getline( ssline, field, ',' );
    std::stringstream STATION(field);
    getline( ssline, field, ',' );
    std::stringstream LAYER(field);
    getline( ssline, field, ',' );
    std::stringstream CHAMBERSEC(field);
    getline( ssline, field, ',' );
    std::stringstream IETA(field);
    getline( ssline, field, ',' );
    std::stringstream LOCALPHI(field);
   

    REGION >> region_; STATION >> station_; LAYER >> layer_; CHAMBERSEC >> chamberSec_;  
    VFATTYPE >> vfatType_; IETA >> iEta_; LOCALPHI >> localPhi_;

    if (vfatType_ < 10) { 
      getline( ssline, field, ',' );
      char* chr = strdup(field.c_str());
      vfatAdd_ = strtol(chr,NULL,16);
    }
    else{
      getline( ssline, field, ',' );
      std::stringstream VFATADD(field);
      VFATADD >> vfatAdd_;
    }

    std::cout << "vfatAdd: " << vfatAdd_ << ", vfatType: " << vfatType_ << 
    ", region: " << region_ << ", station: " << station_ << ", layer: " << layer_ << ", chamberSec: " << chamberSec_ <<
    ", iEta:" << iEta_ << ", localPhi: " << localPhi_ << std::endl;

    vMap.vfatAdd.push_back(vfatAdd_);
    vMap.vfatType.push_back(vfatType_);
    vMap.iEta.push_back(iEta_);
    vMap.localPhi.push_back(localPhi_);
    vMap.gemNum.push_back(region_*(station_*1000 + layer_*100 + chamberSec_)); 
  }
  eMap->theVFatMap_.push_back(vMap);

  // VFAT Channel-Strip Map
  GEMeMap::GEMStripMap chStMap;
  std::string filename2(baseCMS+mapfiles[2]);
  std::ifstream maptype2(filename2.c_str());
  std::cout << filename2 << std::endl;
  while(std::getline(maptype2, line)){
    int vfatType_, vfatCh_, vfatStrip_;

    std::stringstream ssline(line);   
    getline( ssline, field, ',' );
    std::stringstream VFATTYPE(field);
    getline( ssline, field, ',' );
    std::stringstream VFATSTRIP(field);
    getline( ssline, field, ',' );
    std::stringstream VFATCH(field);
    VFATTYPE >> vfatType_; VFATSTRIP >> vfatStrip_; VFATCH >> vfatCh_;
    
    std::cout << "vfatType: " << vfatType_ << ", vfatStrip: " << vfatStrip_ << ", vfatChannel:" << vfatCh_ << std::endl;  

    chStMap.vfatType.push_back(vfatType_);
    chStMap.vfatStrip.push_back(vfatStrip_);
    chStMap.vfatCh.push_back(vfatCh_);
  }
  eMap->theStripMap_.push_back(chStMap); 
    
  cond::Time_t snc = mydbservice->currentTime();  
  // look for recent changes
  int difference=1;
  if (difference==1) {
    m_to_transfer.push_back(std::make_pair((GEMeMap*)eMap,snc));
  }
}

// // additional work (I added these two functions: ConnectOnlineDB and DisconnectOnlineDB)
void popcon::GEMEMapSourceHandler::ConnectOnlineDB( const std::string& connect, const edm::ParameterSet& connectionPset )
{
  cond::persistency::ConnectionPool connection;
  connection.setParameters( connectionPset );
  connection.configure();
  session = connection.createSession( connect,true );
}

void popcon::GEMEMapSourceHandler::DisconnectOnlineDB()
{
  session.close();
}
