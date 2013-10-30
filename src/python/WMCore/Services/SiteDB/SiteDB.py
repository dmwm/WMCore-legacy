#!/usr/bin/env python

"""
API for dealing with retrieving information from SiteDBv2
"""
from WMCore.Services.Service import Service

import json
import logging
import os
import re

def row2dict(columns, row):
    """Convert rows to dictionaries with column keys from description"""
    robj = {}
    for k,v in zip(columns, row):
        robj.setdefault(k,v)
    return robj

def unflattenJSON(data):
    """Tranform input to unflatten JSON format"""
    columns = data['desc']['columns']
    return [row2dict(columns, row) for row in data['result']]

class SiteDBJSON(Service):
    """API for dealing with retrieving information from SiteDBv2"""
    def __init__(self, config={}):
        config = dict(config) ### copy dict since mutables are shared between instances
        config['endpoint'] = "https://cmsweb.cern.ch/sitedb/data/prod/"
        config['accept_type'] = "application/json"
        config['content_type'] = "application/json"

        if os.getenv('CMS_SITEDB_CACHE_DIR'):
            config['cachepath'] = os.getenv('CMS_SITEDB_CACHE_DIR') + '/.cms_sitedbcache'
        elif os.getenv('HOME'):
            config['cachepath'] = os.getenv('HOME') + '/.cms_sitedbcache'
        else:
            import pwd
            config['cachepath'] = '/tmp/sitedbjson_' + pwd.getpwuid(os.getuid())[0]

        if not os.path.isdir(config['cachepath']):
            os.mkdir(config['cachepath'])

        if 'logger' not in config.keys():
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=config['cachepath'] + '/sitedbjsonparser.log',
                    filemode='w')
            config['logger'] = logging.getLogger('SiteDBParser')

        Service.__init__(self, config)

    def getJSON(self, callname, file='result.json', clearCache=False, **args):
        """
        _getJSON_

        retrieve JSON formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        if clearCache:
            self.clearCache(file, args)
        try:
            f = self.refreshCache(file, callname, args)
            result = f.read()
            f.close()
        except IOError:
            raise RuntimeError("URL not available: %s" % callname )
        try:
            # When SiteDB sends proper json, we can use simplejson
            # return json.loads(result)
            results = json.loads(result)
            results = unflattenJSON(results)
            return results
        except SyntaxError:
            self.clearCache(file, args)
            raise SyntaxError("Problem parsing data. Cachefile cleared. Retrying may work")

    def getGroupResponsibilities(self, dn):
        """Get Roles/Groups associated to a given DN"""
        file_login = 'people.json'
        file_groups = 'GroupResponsibilities.json'

        try:
            login = filter(lambda x: x['dn']==dn, self.getJSON("people", file=file_login))[0]
            responsibilities = filter(lambda x: x['username']==login['username'], self.getJSON("group-responsibilities", file=file_groups))
        except (KeyError, IndexError):
            login = filter(lambda x: x['dn']==dn, self.getJSON("people", file=file_login, clearCache=True))[0]
            responsibilities = filter(lambda x: x['username']==login['username'], self.getJSON("group-responsibilities", file=file_groups, clearCache=True))

        return responsibilities

    def _people(self, username=None, clearCache=False):
        if username:
            file = 'people_%s.json' % (username)
            people = self.getJSON("people", file=file, clearCache=clearCache, data=dict(match=username))
        else:
            file = 'people.json'
            people = self.getJSON("people", file=file, clearCache=clearCache)
        return people

    def _sitenames(self, sitename=None, clearCache=False):
        file = 'site-names.json'
        sitenames = self.getJSON('site-names', file=file, clearCache=clearCache)
        if sitename:
            sitenames = filter(lambda x: x['site_name'] == sitename, sitenames)
        return sitenames

    def _siteresources(self, clearCache=False):
        file = 'site-resources.json'
        return self.getJSON('site-resources', file=file)

    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        try:
            userinfo = filter(lambda x: x['dn']==dn, self._people())[0]
            username = userinfo['username']
        except (KeyError, IndexError):
            userinfo = filter(lambda x: x['dn']==dn, self._people())[0]
            username = userinfo['username']
        return username

    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name to list of CEs
        """
	return self.cmsNametoList(cmsName, 'CE')

    def cmsNametoSE(self, cmsName):
        """
        Convert CMS name to list of SEs
        """
        return self.cmsNametoList(cmsName, 'SE')

    def getAllCENames(self):
        """
        _getAllCENames_

        Get all CE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        ceList = filter(lambda x: x['type']=='CE', siteresources)
        ceList = map(lambda x: x['fqdn'], ceList)
        return ceList

    def getAllSENames(self):
        """
        _getAllSENames_

        Get all SE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        seList = filter(lambda x: x['type']=='SE', siteresources)
        seList = map(lambda x: x['fqdn'], seList)
        return seList

    def getAllCMSNames(self):
        """
        _getAllCMSNames_

        Get all the CMSNames from siteDB
        This will allow us to add them in resourceControl at once
        """
        sitenames = self._sitenames()
        cmsnames = filter(lambda x: x['type']=='cms', sitenames)
        cmsnames = map(lambda x: x['alias'], cmsnames)
        return cmsnames

    def cmsNametoList(self, cmsname_pattern, kind, file=None):
        """
        Convert CMS name pattern T1*, T2* to a list of CEs or SEs. The file is
        for backward compatibility with SiteDBv1
        """
        cmsname_pattern = cmsname_pattern.replace('*','.*')
        cmsname_pattern = cmsname_pattern.replace('%','.*')
        cmsname_pattern = re.compile(cmsname_pattern)

        try:
            sitenames = filter(lambda x: x['type']=='cms' and cmsname_pattern.match(x['alias']), self._sitenames())
        except IndexError:
            return []
        sitenames = set(map (lambda x: x['site_name'], sitenames))
        siteresources = filter(lambda x: x['site_name'] in sitenames, self._siteresources())
        hostList = filter(lambda x: x['type']==kind, siteresources)
        hostList = map(lambda x: x['fqdn'], hostList)

        return hostList
    
    def ceToCMSName(self, ce):
        """
        Convert SE name to the CMS Site they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of cms site alias
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==ce, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        cmsname = filter(lambda x: x['type']=='cms', siteNames)
        return [x['alias'] for x in cmsname]
	
    def seToCMSName(self, se):
        """
        Convert SE name to the CMS Site they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of cms site alias
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==se, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        cmsname = filter(lambda x: x['type']=='cms', siteNames)
        return [x['alias'] for x in cmsname]


    def cmsNametoPhEDExNode(self, cmsName):
        """
        Convert CMS name to list of Phedex Nodes
        """
        sitenames = self._sitenames()
        try:
            sitename = filter(lambda x: x['type']=='cms' and x['alias']==cmsName, sitenames)[0]['site_name']
        except IndexError:
            return None
        phedexnames = filter(lambda x: x['type']=='phedex' and x['site_name']==sitename, sitenames)
        phedexnames = map(lambda x: x['alias'], phedexnames)
        return phedexnames


    def phEDExNodetocmsName(self, node):
        """
        Convert PhEDEx node name to cms site
        """
        # api doesn't work at the moment - so reverse engineer
        # first strip special endings and check with cmsNametoPhEDExNode
        # if this fails (to my knowledge no node does fail) do a full lookup
        name = node.replace('_MSS',
                            '').replace('_Buffer',
                                        '').replace('_Export', '')

        return name
        # Disable cross-check until following bug fixed.
        # https://savannah.cern.ch/bugs/index.php?67044
#        if node in self.cmsNametoPhEDExNode(name):
#            return name
#
#        # As far as i can tell there is no way to get a full listing, would
#        # need to call CMSNametoPhEDExNode?cms_name= but can't find a way to do
#        # that. So simply raise an error
#        raise ValueError, "Unable to find CMS name for \'%s\'" % node

    def load_siteDB_Report(self, report):
        
        """
        emulate XML Report from V1 API
        only implemente the reports used in Crab2
        via ProdCommon/SiteDB/SiteDBReport
        """
        if report == 'se_node_map.ini':
            #limit sitenames to Tx site names
            AcmsName=re.compile('T[0-3]_')
            sn=filter(lambda x: x['type']=='phedex' and AcmsName.match(x['alias']),self._sitenames())
            
            # only pick SE's from the resource list
            seList = filter(lambda x:x['type']=='SE', self._siteresources())
            
            # fill V1 style report as a list of disctionaries
            seNodeMap=[]
            id=0
            for site in sn:
                # find all SE's for site with cmsName site_name
                site_name=site['site_name']
                SEs = filter (lambda x:x['site_name']==site_name, seList)
                for se in SEs:
                    seDict={'id':id, 'name':site_name, 'node':site['alias'],'se':se['fqdn']}
                    seNodeMap.append(seDict)
                    id += 1
            # turn seNomeMap into xml
            xmlReport=""
            xmlReport+='<result>\n'
            for se in seNodeMap:
                xmlReport += '<item id="%s">' % se['id']
                xmlReport += '<name>%s</name>' % se['name']
                xmlReport += '<node>%s</node>' % se['node']
                xmlReport += '<se>%s</se>' % se['se']
                xmlReport += '</item>'
            xmlReport+='</result>'
        else:
            xmlReport=""
            raise Exception ("xmlReport %s not implemented"%report)
        
        import StringIO
        return StringIO.StringIO(xmlReport)


if __name__ == '__main__':
    siteDBAPI = SiteDBJSON()
    print siteDBAPI.getGroupResponsibilities(dn='/O=GermanGrid/OU=RWTH/CN=Manuel Giffels')
    print siteDBAPI.seToCMSName(se='cmssrm.fnal.gov')
    print siteDBAPI.seToCMSName(se='grid-srm.physik.rwth-aachen.de')
    print siteDBAPI.cmsNametoSE('%T2_US')
    print siteDBAPI.cmsNametoSE('%T2_DE')
    print siteDBAPI.cmsNametoCE('%T2_DE')
