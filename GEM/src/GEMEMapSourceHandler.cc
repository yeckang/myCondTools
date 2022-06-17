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

  mapfiles.push_back("chamberMapTestBeam.csv");
  mapfiles.push_back("stripChannelMap.csv");
  // Chamber Map 
  GEMeMap::GEMChamberMap cMap;
  std::string field, line;
  std::string filename(baseCMS+mapfiles[0]);
  std::ifstream maptype(filename.c_str());
  //std::string buf("");
  std::cout << filename << std::endl;
  while(std::getline(maptype, line)){
    unsigned int fedId_, amcNum_, gebId_;
    //uint8_t amcNum_, gebId_;
    int  region_, station_, layer_, chamberSec_, vfatVer_, chamberType_; 
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
    getline( ssline, field, ',' );
    std::stringstream CHAMBERTYPE(field);

    FEDID >> fedId_; AMCNUM >> amcNum_; GEBID >> gebId_;
    REGION >> region_; STATION >> station_; LAYER >> layer_; CHAMBERSEC >> chamberSec_; VFATVER >> vfatVer_, CHAMBERTYPE >> chamberType_; 

    std::cout << "fedId: " << fedId_ << ", AMC#: " << amcNum_ << ", gebId: " << gebId_ <<
    ", region: " << region_ << ", station: " << station_ << ", layer: " << layer_ << ", chamberSec: " << chamberSec_ << 
    ", vfatVer" << vfatVer_ << ", chamberType" << chamberType_ << std::endl;

    cMap.fedId.push_back(fedId_);
    cMap.amcNum.push_back(amcNum_);
    cMap.gebId.push_back(gebId_);
    cMap.gemNum.push_back(region_*(station_*1000 + layer_*100 + chamberSec_));
    cMap.vfatVer.push_back(vfatVer_);
    cMap.chamberType.push_back(chamberType_);
  }
  eMap->theChamberMap_.push_back(cMap);

  // VFAT Channel-Strip Map
  GEMeMap::GEMStripMap chStMap;
  std::string filename2(baseCMS+mapfiles[1]);
  std::ifstream maptype2(filename2.c_str());
  std::cout << filename2 << std::endl;
  while(std::getline(maptype2, line)){
    int chamberType_, vfat_, vfatCh_, iEta_, strip_;

    std::stringstream ssline(line);   
    getline( ssline, field, ',' );
    std::stringstream CHAMBERTYPE(field);
    getline( ssline, field, ',' );
    std::stringstream VFAT(field);
    getline( ssline, field, ',' );
    std::stringstream VFATCH(field);
    getline( ssline, field, ',' );
    std::stringstream IETA(field);
    getline( ssline, field, ',' );
    std::stringstream STRIP(field);
    CHAMBERTYPE >> chamberType_; VFAT >> vfat_; VFATCH >> vfatCh_; IETA >> iEta_; STRIP >> strip_; 
    
    //std::cout << "chamberType: " << chamberType_ << ", vfat:" << vfat_ << ", vfatChannel:" << vfatCh_ << ", iEta:" << iEta_ << ", strip: " << strip_ << std::endl;  

    chStMap.chamberType.push_back(chamberType_);
    chStMap.vfatAdd.push_back(vfat_);
    chStMap.vfatCh.push_back(vfatCh_);
    chStMap.iEta.push_back(iEta_);
    //if (chamberType_ > 30) chStMap.strip.push_back(384 - strip_);
    //else chStMap.strip.push_back(strip_);
    chStMap.strip.push_back(strip_);
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
