#include "myCondTools/GEM/interface/GEMEMapSourceHandler.h"
#include "CondCore/CondDB/interface/ConnectionPool.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include "DataFormats/MuonDetId/interface/GEMDetId.h"
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


popcon::GEMEMapSourceHandler::GEMEMapSourceHandler( const edm::ParameterSet& ps ):
  m_name( ps.getUntrackedParameter<std::string>( "name", "GEMEMapSourceHandler" ) ),
  m_dummy( ps.getUntrackedParameter<int>( "WriteDummy", 0 ) ),
  m_validate( ps.getUntrackedParameter<int>( "Validate", 1 ) ),
  m_connect( ps.getParameter<std::string>( "connect" ) ),
  m_connectionPset( ps.getParameter<edm::ParameterSet>( "DBParameters" ) ),
  m_conf_type( ps.getParameter<std::string>("QC8ConfType")),
  m_chamberMap_file( ps.getParameter<edm::FileInPath>("chamberMap").fullPath() ),
  m_chamberMap_file( ps.getParameter<edm::FileInPath>("stripChannelMap").fullPath() ),
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
  eMap =  new GEMChMap(eMap_version);
  
  std::string baseCMS = std::string(getenv("CMSSW_BASE"))+std::string("/src/myCondTools/GEM/data/");  
  std::vector<std::string> mapfiles;

  mapfiles.push_back("chamberMap2022.csv");
  mapfiles.push_back("stripChannelMap.csv");
  // Chamber Map 
  std::string field, line;
  std::string filename(baseCMS+mapfiles[0]);
  std::ifstream maptype(filename.c_str());
  //std::string buf("");
  std::cout << filename << std::endl;
  while(std::getline(maptype, line)){
    unsigned int fedId_, amcNum_, gebId_;
    //uint8_t amcNum_, gebId_;
    int  region_, station_, layer_, chamberSec_, chamberType_; 
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
    std::stringstream CHAMBERTYPE(field);

    FEDID >> fedId_; AMCNUM >> amcNum_; GEBID >> gebId_;
    REGION >> region_; STATION >> station_; LAYER >> layer_; CHAMBERSEC >> chamberSec_; CHAMBERTYPE >> chamberType_; 

    GEMChMap::chamEC ec;
    ec.fedId = fedId_;
    ec.amcNum = amcNum_;
    ec.gebId = gebId_;

    GEMChMap::chamDC dc;
    dc.detId = GEMDetId(region_,
                        1,
                        station_,
                        layer_,
                        chamberSec_,
                        0);
    dc.chamberType = chamberType_;

    eMap->add(ec,dc);

    GEMChMap::sectorEC amcEC;
    amcEC.fedId = fedId_;
    amcEC.amcNum = amcNum_;

    if (!eMap->isValidAMC(fedId_, amcNum_)) eMap->add(amcEC);
    
    std::cout << "fedId: " << fedId_ << ", AMC#: " << amcNum_ << ", gebId: " << gebId_ <<
    ", region: " << region_ << ", station: " << station_ << ", layer: " << layer_ << ", chamberSec: " << chamberSec_ << 
    ", chamberType" << chamberType_ << std::endl;
  }

  // VFAT Channel-Strip Map
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

    GEMChMap::channelNum cMap;
    cMap.chamberType = chamberType_;
    cMap.vfatAdd = vfat_;
    cMap.chNum = vfatCh_;

    GEMChMap::stripNum sMap;
    sMap.chamberType = chamberType_;
    sMap.iEta = iEta_;
    sMap.stNum = strip_;

    eMap->add(cMap, sMap);
    eMap->add(sMap, cMap);

    GEMChMap::vfatEC ec;
    ec.vfatAdd = vfat_;
    ec.chamberType = chamberType_;

    eMap->add(chamberType_, vfat_);
    eMap->add(ec, iEta_);
  }
    
  cond::Time_t snc = mydbservice->currentTime();  
  // look for recent changes
  int difference=1;
  if (difference==1) {
    m_to_transfer.push_back(std::make_pair((GEMChMap*)eMap,snc));
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
