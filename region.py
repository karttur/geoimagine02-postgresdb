'''
Created on 23 feb. 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp

class ManageRegion(PGsession):
    '''
    DB support for managing regions
    '''
    def __init__(self, db, verbose = 1):
        """ The constructor connects to the database"""
        
        HOST = 'karttur'
        
        query = self._GetCredentials( HOST )

        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageRegion')
        
        self.verbose = verbose


    def _SelectMultiRecs(self, queryD, paramL, table, schema='regions', asdict=False):
        ''' Select multiple records
        '''
        return self._MultiSearch( queryD, paramL, schema, table)            
         
    def _InsertRegionCat(self,process):
        ''' Insert region categories
        '''
        
        if self._CheckWhitespace(process.parameters.regioncat):
            
            exitstr = 'EXITING - the regioncat "%s" contains whithespace' %(process.parameters.regioncat)
            
            exit(exitstr)
            
        if not process.parameters.regioncat == process.parameters.regioncat.lower():
            
            exitstr = 'EXITING - the regioncat "%s" contains upper case' %(process.parameters.regioncat)
            
            exit(exitstr)
        
        queryD = {'parentcat':process.parameters.parentcat, 'regioncat':process.parameters.regioncat}
        #Get the parentid from all cats except tracts and global
        
        if process.parameters.stratum == 0:
            
            exit ('EXITING - _InsertRegionCat failed because Stratum 0 can not be altered')

        if process.parameters.parentcat == '*' and process.parameters.stratum > 11:
            
            catrec = True
            
        else:
            
            sql = "SELECT regioncat FROM system.regioncats WHERE regioncat = '%(parentcat)s';" %queryD

            self.cursor.execute(sql)
            
            catrec = self.cursor.fetchone()
            
        if catrec != None:
            
            #check for the regioncat itself
            #query = {'cat': region.regioncat}
            
            sql = "SELECT * FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %queryD
            
            self.cursor.execute(sql)
            
            record = self.cursor.fetchone()
            
            if record != None: 
                
                if process.overwrite or process.delete:
                    
                    sql = "DELETE FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %queryD
                
                    self.cursor.execute(sql)
                    
                    self.conn.commit()
                    
                    if process.delete:
                        
                        return
                    
                else:
                    
                    return
                
            # INSERT region category     
            sql = "INSERT INTO system.regioncats (regioncat, parentcat, stratum, title, label) VALUES ('%s', '%s', %s, '%s', '%s')"\
                    %(process.parameters.regioncat, process.parameters.parentcat, process.parameters.stratum,process.parameters.title, process.parameters.label)
            
            self.cursor.execute(sql)
            
            self.conn.commit()
                
        else:
            
            exitstr = 'The parentcat region %s for region %s does not exists, it must be added proir to the region' %(process.parameters.prantid, process.parameters.regioncat)
            
            exit(exitstr)


    def _Insert1DegDefRegion(self, query):
        '''
        '''
        
        if self._CheckWhitespace(query['regionid']):
            
            exitstr = 'EXITING - the regioncat "%s" contains whithespace' %(query['regionid'])
            
            exit(exitstr)
            
        if not query['regionid'] == query['regionid'].lower():
            
            exitstr = 'EXITING - the regioncat "%s" contains upper case' %(query['regionid'])
            
            exit(exitstr)
            
        self._CheckInsertSingleRecord(query,'system','defregions')


    def _InsertDefRegion(self, layer, query, bounds, llD, overwrite, delete):
        '''
        '''
        
        if self._CheckWhitespace(query['regionid']):
            
            exitstr = 'EXITING - the regioncat "%s" contains whithespace' %(query['regionid'])
            
            exit(exitstr)
            
        if not query['regionid'] == query['regionid'].lower():
            
            exitstr = 'EXITING - the regioncat "%s" contains upper case' %(query['regionid'])
            
            exit(exitstr)
            
        if overwrite or delete:
            
            self.cursor.execute("DELETE FROM system.defregions WHERE regionid = '%(regionid)s' AND regioncat ='%(parentcat)s' AND parentid ='%(parentid)s'  ;" %query)
            
            self.conn.commit()
            
            if delete:
            
                self._InsertRegion(query, bounds, llD, overwrite, delete)
                
                return
            
        #Check that the regioncat is correctly set
        self.cursor.execute("SELECT * FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %query)
        
        record = self.cursor.fetchone()
        
        if record == None:
        
            exitstr = 'the regioncat %(regioncat)s does not exist in the regioncats table' %query
            
            exit(exitstr)

        #Check that the parent regions is set
        self.cursor.execute("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %query)
        
        record = self.cursor.fetchone()
        
        if record == None:
            
            if query['parentid'] in ['south-america','antarctica'] and query['parentcat'] == 'subcontinent':
                
                xquery = {'parentid':query['parentid'], 'parentcat':'continent'}
                
                self.cursor.execute("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %xquery)
                
                record = self.cursor.fetchone()
                
                if record == None:
                    
                    exitstr = 'the parentid region "%s" of regioncat "%s" does not exist in the defregions table' %(query['parentid'], query['parentcat'])
                    
                    exit(exitstr)
            else:

                exitstr = 'the parentid region "%s" of regioncat "%s" does not exist in the defregions table' %(query['parentid'], query['parentcat'])
               
                print ("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %query)

                exit(exitstr)

        #Check if the region itself already exists

        self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(regionid)s';" %query)
        
        record = self.cursor.fetchone()
        
        if record == None:
            
            self.cursor.execute('INSERT INTO system.defregions (regioncat, regionid, regionname, parentid, title, label) VALUES (%s, %s, %s, %s, %s, %s)',
                                (query['regioncat'], query['regionid'], query['regionname'], query['parentid'], query['title'], query['label']))
            
            self.conn.commit()

        else:
            
            if query['regioncat'] != record[0]:
                
                pass
            
                '''
                if layer.locus.locus in ['antarctica','south-america']:
                
                    query2 = {'id': layer.locus.locus,'cat':query['regioncat']}
                    
                    self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(id)s' and regioncat = '%(cat)s';" %query2)
                    
                    record = self.cursor.fetchone()
                    
                    if record == None:
                    
                        self.cursor.execute('INSERT INTO system.defregions (regioncat, regionid, regionname, parentid, title, label) VALUES (%s, %s, %s, %s, %s, %s)',
                                            (query['regioncat'], query['regionid'], query['regionname'], query['parentid'], query['title'], query['label']))
                        
                        self.conn.commit()
                else:
                    
                    pass
                '''

        query['system'] = 'system'
        
        query['regiontype'] = 'D'
        
        self._InsertRegion(query, bounds, llD, overwrite, delete)

        InsertCompDef(self,layer.comp)
        
        InsertCompProd(self,layer.comp)
                
        InsertLayer(self, layer, overwrite, delete)

    def _InsertRegion(self, query, bounds, llD, overwrite, delete):
        '''
        '''
        
        if overwrite or delete:
            self.cursor.execute("DELETE FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
            self.conn.commit()
            if delete:
                return

        self.cursor.execute("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            
            sql = "INSERT INTO %(system)s.regions (regionid, regioncat, regiontype) VALUES \
                    ('%(regionid)s', '%(regioncat)s', '%(regiontype)s');" %query
            
            self.cursor.execute( sql )

            self.conn.commit()
            query['epsg'] = query['epsg']
            query['minx'] = bounds[0]
            query['miny'] = bounds[1]
            query['maxx'] = bounds[2]
            query['maxy'] = bounds[3]
            
            sql = "UPDATE %(system)s.regions SET (epsg, minx, miny, maxx, maxy) = \
                    (%(epsg)s, %(minx)s, %(miny)s, %(maxx)s, %(maxy)s) WHERE regionid = '%(regionid)s';" %query
                    
            self.cursor.execute( sql )

            self.conn.commit()
            
            for key in llD:
                
                query[key] = llD[key]


            sql = "UPDATE %(system)s.regions SET (ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat) = \
                    (%(ullon)s,%(ullat)s,%(urlon)s,%(urlat)s,%(lrlon)s,%(lrlat)s,%(lllon)s,%(lllat)s) WHERE regionid = '%(regionid)s';" %query
                    
            self.cursor.execute( sql )
            
            self.conn.commit()
            
        elif record[0] != query['regioncat']:
            
            if query['regionid'] in ['antarctica','south-america']:
                
                pass
            
            else:
                                
                exitstr = 'Duplicate categories (%s %s) for regionid %s' %(record[0],query['regioncat'], query['regionid'])
                
                exit(exitstr)
        #TGTODO duplicate name for tract but different user???, delete and overwrite

    def _SelectComp(self,comp):
        #comp['system'] = system
        return SelectComp(self, comp)


    def _LoadBulkDefregions(self,tmpFPN):
        #self._DeleteSRTMBulkTiles(params)
        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'system.defregions', sep=',')
            self.conn.commit()
            #cur.copy_from(f, 'test', columns=('col1', 'col2'), sep=",")

    def _LoadBulkRegions(self,tmpFPN, skip1st=True):

        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            if skip1st:
                next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'system.regions', sep=',')
            self.conn.commit()
